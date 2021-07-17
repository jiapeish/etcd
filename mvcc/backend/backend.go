// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/pkg/capnslog"
	humanize "github.com/dustin/go-humanize"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var (
	defaultBatchLimit    = 10000
	defaultBatchInterval = 100 * time.Millisecond

	defragLimit = 10000

	// initialMmapSize is the initial size of the mmapped region. Setting this larger than
	// the potential max db size can prevent writer from blocking reader.
	// This only works for linux.
	initialMmapSize = uint64(10 * 1024 * 1024 * 1024)

	plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "mvcc/backend")

	// minSnapshotWarningTimeout is the minimum threshold to trigger a long running snapshot warning.
	minSnapshotWarningTimeout = 30 * time.Second
)

// 该接口的主要功能是将底层存储与上层进行解耦，定义底层存储需要对上层提供的接口
type Backend interface {
	// 创建一个只读事务，是v3存储对只读事务对抽象
	// ReadTx returns a read transaction. It is replaced by ConcurrentReadTx in the main data path, see #10523.
	ReadTx() ReadTx
	// 创建一个批量事务，是对批量读写事务的抽象
	BatchTx() BatchTx
	// ConcurrentReadTx returns a non-blocking read transaction.
	ConcurrentReadTx() ReadTx

	Snapshot() Snapshot
	Hash(ignores map[IgnoreKey]struct{}) (uint32, error)
	// 获取当前已存储的总字节数
	// Size returns the current size of the backend physically allocated.
	// The backend can hold DB space that is not utilized at the moment,
	// since it can conduct pre-allocation or spare unused space for recycling.
	// Use SizeInUse() instead for the actual DB size.
	Size() int64
	// SizeInUse returns the current size of the backend logically in use.
	// Since the backend can manage free space in a non-byte unit such as
	// number of pages, the returned value can be not exactly accurate in bytes.
	SizeInUse() int64
	// OpenReadTxN returns the number of currently open read transactions in the backend.
	OpenReadTxN() int64
	// 碎片整理
	Defrag() error
	// 提交批量读写事务
	ForceCommit()
	Close() error
}

type Snapshot interface {
	// Size gets the size of the snapshot.
	Size() int64
	// WriteTo writes the snapshot into the given writer.
	WriteTo(w io.Writer) (n int64, err error)
	// Close closes the snapshot.
	Close() error
}

// 该结构体是v3版本存储提供的Back-end接口的默认实现，其底层存储使用的是bolt-db
type backend struct {
	// size and commits are used with atomic operations so they must be
	// 64-bit aligned, otherwise 32-bit tests will crash

	// 当前backend实例已存储的总字节数
	// size is the number of bytes allocated in the backend
	size int64
	// sizeInUse is the number of bytes actually used in the backend
	sizeInUse int64
	// 从启动到目前为止，已提交的事务数
	// commits counts number of commits since start
	commits int64
	// openReadTxN is the number of currently open read transactions in the backend
	openReadTxN int64

	mu sync.RWMutex
	db *bolt.DB // 底层的bolt-db存储

	// 两次批量读写事务提交的最大时间差
	batchInterval time.Duration
	// 指定一次批量事务中最大的操作数，当超过该阈值，当前的批量事务会自动提交
	batchLimit    int
	// 批量读写事务，batch-tx-buffered是在batch-tx的基础上添加了缓存功能，两者都实现了Batch-tx接口
	batchTx       *batchTxBuffered

	// 只读事务，实现了Read-tx接口
	readTx *readTx

	stopc chan struct{}
	donec chan struct{}

	lg *zap.Logger
}

type BackendConfig struct {
	// bolt-db数据库文件的路径
	// Path is the file path to the backend file.
	Path string
	// 提交两次批量事务的最大时间差，用来初始化backend实例中的batch-interval字段，默认100ms
	// BatchInterval is the maximum time before flushing the BatchTx.
	BatchInterval time.Duration
	// 指定每个批量读写事务能包含的最大操作个数，当超过这个阈值后，当前批量读写事务会自动提交；
	// 用来初始化backend中batch-limit字段，默认10000
	// BatchLimit is the maximum puts before flushing the BatchTx.
	BatchLimit int
	// BackendFreelistType is the backend boltdb's freelist type.
	BackendFreelistType bolt.FreelistType
	// bolt-db使用了mmap技术对数据库文件进行映射，该字段用来设置mmap中使用的内存大小，在创建bolt-db实例时使用
	// MmapSize is the number of bytes to mmap for the backend.
	MmapSize uint64
	// Logger logs backend-side operations.
	Logger *zap.Logger
	// UnsafeNoFsync disables all uses of fsync.
	UnsafeNoFsync bool `json:"unsafe-no-fsync"`
}

func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		BatchInterval: defaultBatchInterval,
		BatchLimit:    defaultBatchLimit,
		MmapSize:      initialMmapSize,
	}
}

func New(bcfg BackendConfig) Backend {
	return newBackend(bcfg)
}

func NewDefaultBackend(path string) Backend {
	bcfg := DefaultBackendConfig()
	bcfg.Path = path
	return newBackend(bcfg)
}

func newBackend(bcfg BackendConfig) *backend {
	bopts := &bolt.Options{} // 初始化bolt-db时的参数
	if boltOpenOptions != nil {
		*bopts = *boltOpenOptions
	}
	bopts.InitialMmapSize = bcfg.mmapSize() // mmap使用的内存大小
	bopts.FreelistType = bcfg.BackendFreelistType
	bopts.NoSync = bcfg.UnsafeNoFsync
	bopts.NoGrowSync = bcfg.UnsafeNoFsync

	db, err := bolt.Open(bcfg.Path, 0600, bopts) // 创建bolt-db实例
	if err != nil {
		if bcfg.Logger != nil {
			bcfg.Logger.Panic("failed to open database", zap.String("path", bcfg.Path), zap.Error(err))
		} else {
			plog.Panicf("cannot open database at %s (%v)", bcfg.Path, err)
		}
	}

	// 创建backend实例，并初始化其中各个字段
	// In future, may want to make buffering optional for low-concurrency systems
	// or dynamically swap between buffered/non-buffered depending on workload.
	b := &backend{
		db: db,

		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,

		// 创建read-tx实例并初始化
		readTx: &readTx{
			buf: txReadBuffer{
				txBuffer: txBuffer{make(map[string]*bucketBuffer)},
			},
			buckets: make(map[string]*bolt.Bucket),
			txWg:    new(sync.WaitGroup),
		},

		stopc: make(chan struct{}),
		donec: make(chan struct{}),

		lg: bcfg.Logger,
	}
	b.batchTx = newBatchTxBuffered(b) // 创建batch-tx-buffered实例并初始化
	go b.run() // 启动一个单独的goroutine，其中会定时提交当前的批量读写事务，并开启新的批量读写事务
	return b
}

// BatchTx returns the current batch tx in coalescer. The tx can be used for read and
// write operations. The write result can be retrieved within the same tx immediately.
// The write result is isolated with other txs until the current one get committed.
func (b *backend) BatchTx() BatchTx {
	return b.batchTx
}

func (b *backend) ReadTx() ReadTx { return b.readTx }

// ConcurrentReadTx creates and returns a new ReadTx, which:
// A) creates and keeps a copy of backend.readTx.txReadBuffer,
// B) references the boltdb read Tx (and its bucket cache) of current batch interval.
func (b *backend) ConcurrentReadTx() ReadTx {
	b.readTx.RLock()
	defer b.readTx.RUnlock()
	// prevent boltdb read Tx from been rolled back until store read Tx is done. Needs to be called when holding readTx.RLock().
	b.readTx.txWg.Add(1)
	// TODO: might want to copy the read buffer lazily - create copy when A) end of a write transaction B) end of a batch interval.
	return &concurrentReadTx{
		buf:     b.readTx.buf.unsafeCopy(),
		tx:      b.readTx.tx,
		txMu:    &b.readTx.txMu,
		buckets: b.readTx.buckets,
		txWg:    b.readTx.txWg,
	}
}

// ForceCommit forces the current batching tx to commit.
func (b *backend) ForceCommit() {
	b.batchTx.Commit()
}

// 用当前bolt-db中的数据创建相应的快照，并用tx write-to方法备份整个bolt-db数据库的数据
func (b *backend) Snapshot() Snapshot {
	b.batchTx.Commit() // 提交当前的读写事务，主要是为了提交缓冲区中的操作

	b.mu.RLock()
	defer b.mu.RUnlock()
	tx, err := b.db.Begin(false) // 开启一个只读事务
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to begin tx", zap.Error(err))
		} else {
			plog.Fatalf("cannot begin tx (%s)", err)
		}
	}

	stopc, donec := make(chan struct{}), make(chan struct{})
	dbBytes := tx.Size() // 获取整个bolt-db中保存的数据大小
	go func() { // 启动一个单独的协程，检测快照数据是否已经发送完成
		defer close(donec)
		// 假设发送快照的最小速度是100MB/s
		// sendRateBytes is based on transferring snapshot data over a 1 gigabit/s connection
		// assuming a min tcp throughput of 100MB/s.
		var sendRateBytes int64 = 100 * 1024 * 1014
		// 创建定时器
		warningTimeout := time.Duration(int64((float64(dbBytes) / float64(sendRateBytes)) * float64(time.Second)))
		if warningTimeout < minSnapshotWarningTimeout {
			warningTimeout = minSnapshotWarningTimeout
		}
		start := time.Now()
		ticker := time.NewTicker(warningTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C: // 超时未发送完快照数据，则会输出警告日志
				if b.lg != nil {
					b.lg.Warn(
						"snapshotting taking too long to transfer",
						zap.Duration("taking", time.Since(start)),
						zap.Int64("bytes", dbBytes),
						zap.String("size", humanize.Bytes(uint64(dbBytes))),
					)
				} else {
					plog.Warningf("snapshotting is taking more than %v seconds to finish transferring %v MB [started at %v]", time.Since(start).Seconds(), float64(dbBytes)/float64(1024*1014), start)
				}

			case <-stopc: // 发送快照数据结束
				snapshotTransferSec.Observe(time.Since(start).Seconds())
				return
			}
		}
	}()

	return &snapshot{tx, stopc, donec} // 创建快照实例
}

type IgnoreKey struct {
	Bucket string
	Key    string
}

func (b *backend) Hash(ignores map[IgnoreKey]struct{}) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	b.mu.RLock()
	defer b.mu.RUnlock()
	err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("cannot get hash of bucket %s", string(next))
			}
			h.Write(next)
			b.ForEach(func(k, v []byte) error {
				bk := IgnoreKey{Bucket: string(next), Key: string(k)}
				if _, ok := ignores[bk]; !ok {
					h.Write(k)
					h.Write(v)
				}
				return nil
			})
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

func (b *backend) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *backend) SizeInUse() int64 {
	return atomic.LoadInt64(&b.sizeInUse)
}

// 该方法会按照batch-interval指定的时间间隔，定时提交批量读写事务，在提交之后会立即开启一个新的批量读写事务
func (b *backend) run() {
	defer close(b.donec)
	t := time.NewTimer(b.batchInterval)
	defer t.Stop()
	for {
		select { // 阻塞等待上述定时器到期
		case <-t.C:
		case <-b.stopc:
			b.batchTx.CommitAndStop()
			return
		}
		if b.batchTx.safePending() != 0 {
			b.batchTx.Commit() // 提交当前的批量读写事务，并开启一个新的批量读写事务
		}
		t.Reset(b.batchInterval) // 重置定时器
	}
}

func (b *backend) Close() error {
	close(b.stopc)
	<-b.donec
	return b.db.Close()
}

// Commits returns total number of commits since start
func (b *backend) Commits() int64 {
	return atomic.LoadInt64(&b.commits)
}

// 整理当前bolt-db实例中的碎片，提高其中bucket的填充率；
// 整理碎片实际上是创建新的bolt-db数据库文件，并将旧数据库文件的数据写入到新数据库文件中；
// 因为在写入新数据库文件时是顺序写入的，所以会提高填充比例（fill percent），从而达到碎片整理的目的；
// 在整理碎片的过程中，需要持有read-tx, batch-tx, backend的锁；
func (b *backend) Defrag() error {
	return b.defrag()
}

// 主要完成了新老数据库的切换
func (b *backend) defrag() error {
	now := time.Now()

	// 加锁，获取read-tx, batch-tx, backend中的三把锁
	// TODO: make this non-blocking?
	// lock batchTx to ensure nobody is using previous tx, and then
	// close previous ongoing tx.
	b.batchTx.Lock()
	defer b.batchTx.Unlock()

	// lock database after lock tx to avoid deadlock.
	b.mu.Lock()
	defer b.mu.Unlock()

	// block concurrent read requests while resetting tx
	b.readTx.Lock()
	defer b.readTx.Unlock()

	// 提交当前的批量读写事务，注意参数，此次提交之后不会立即打开新的批量读写事务
	b.batchTx.unsafeCommit(true)

	b.batchTx.tx = nil

	// Create a temporary file to ensure we start with a clean slate.
	// Snapshotter.cleanupSnapdir cleans up any of these that are found during startup.
	dir := filepath.Dir(b.db.Path())
	temp, err := ioutil.TempFile(dir, "db.tmp.*")
	if err != nil {
		return err
	}
	options := bolt.Options{}
	if boltOpenOptions != nil {
		options = *boltOpenOptions
	}
	options.OpenFile = func(path string, i int, mode os.FileMode) (file *os.File, err error) {
		return temp, nil
	}
	tdbp := temp.Name() // 获取新数据库文件的路径
	// 创建新的bolt-db实例，对应的数据库文件是个临时文件
	tmpdb, err := bolt.Open(tdbp, 0600, &options)
	if err != nil {
		return err
	}

	dbp := b.db.Path() // 获取旧数据库文件的路径
	size1, sizeInUse1 := b.Size(), b.SizeInUse()
	if b.lg != nil {
		b.lg.Info(
			"defragmenting",
			zap.String("path", dbp),
			zap.Int64("current-db-size-bytes", size1),
			zap.String("current-db-size", humanize.Bytes(uint64(size1))),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse1),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse1))),
		)
	}
	// 进行碎片整理，其底层是创建一个新的bolt-db数据库文件，并将当前数据库中的全部数据写入其中；
	// 在写入过程中，会将新的bucket填充比例设置为90%，从而达到碎片整理的效果
	// gofail: var defragBeforeCopy struct{}
	err = defragdb(b.db, tmpdb, defragLimit)
	if err != nil {
		tmpdb.Close()
		if rmErr := os.RemoveAll(tmpdb.Path()); rmErr != nil {
			if b.lg != nil {
				b.lg.Error("failed to remove db.tmp after defragmentation completed", zap.Error(rmErr))
			} else {
				plog.Fatalf("failed to remove db.tmp after defragmentation completed: %v", rmErr)
			}
		}
		return err
	}

	err = b.db.Close() // 关闭旧的bolt-db实例
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to close database", zap.Error(err))
		} else {
			plog.Fatalf("cannot close database (%s)", err)
		}
	}
	err = tmpdb.Close() // 关闭新的bolt-db实例
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to close tmp database", zap.Error(err))
		} else {
			plog.Fatalf("cannot close database (%s)", err)
		}
	}
	// gofail: var defragBeforeRename struct{}
	err = os.Rename(tdbp, dbp) // 重命名新数据库文件，覆盖旧数据库文件
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to rename tmp database", zap.Error(err))
		} else {
			plog.Fatalf("cannot rename database (%s)", err)
		}
	}

	// 重新创建bolt-db实例，此时使用的数据库文件是碎片整理之后的新数据库文件
	b.db, err = bolt.Open(dbp, 0600, boltOpenOptions)
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to open database", zap.String("path", dbp), zap.Error(err))
		} else {
			plog.Panicf("cannot open database at %s (%v)", dbp, err)
		}
	}
	// 开启新的批量读写事务
	b.batchTx.tx = b.unsafeBegin(true)

	b.readTx.reset()
	// 开启新的只读事务
	b.readTx.tx = b.unsafeBegin(false)

	size := b.readTx.tx.Size()
	db := b.readTx.tx.DB()
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(db.Stats().FreePageN)*int64(db.Info().PageSize)))

	took := time.Since(now)
	defragSec.Observe(took.Seconds())

	size2, sizeInUse2 := b.Size(), b.SizeInUse()
	if b.lg != nil {
		b.lg.Info(
			"defragmented",
			zap.String("path", dbp),
			zap.Int64("current-db-size-bytes-diff", size2-size1),
			zap.Int64("current-db-size-bytes", size2),
			zap.String("current-db-size", humanize.Bytes(uint64(size2))),
			zap.Int64("current-db-size-in-use-bytes-diff", sizeInUse2-sizeInUse1),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse2),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse2))),
			zap.Duration("took", took),
		)
	}
	return nil
}

// 该方法完成了从旧数据库文件向新数据库文件复制键值对的功能
func defragdb(odb, tmpdb *bolt.DB, limit int) error {
	// open a tx on tmpdb for writes
	tmptx, err := tmpdb.Begin(true) // 在新数据库实例上开启一个读写事务
	if err != nil {
		return err
	}

	// open a tx on old db for read
	tx, err := odb.Begin(false) // 在旧数据库实例上开启一个只读事务
	if err != nil {
		return err
	}
	defer tx.Rollback() // 方法结束时关闭该只读事务

	c := tx.Cursor() // 获取旧实例上的cursor，用于遍历其中所有的bucket

	count := 0
	for next, _ := c.First(); next != nil; next, _ = c.Next() {
		// 读取旧数据库实例中的所有bucket，并在新数据库实例上创建对应的bucket
		b := tx.Bucket(next) // 获取指定的bucket实例
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %s", string(next))
		}

		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		if berr != nil {
			return berr
		}
		// 为提高利用率，将填充比例设置为90%；
		// 下面会从旧bucket中读取全部的键值对，并填充到新bucket中，这个过程是顺序写入的
		tmpb.FillPercent = 0.9 // for seq write in for each

		b.ForEach(func(k, v []byte) error { // 遍历旧bucket中的全部键值对
			count++
			if count > limit { // 当读取的键值对数量超过阈值，则提交当前读写事务（新数据库）
				err = tmptx.Commit()
				if err != nil {
					return err
				}
				// 重新开启一个读写事务，继续后边的写入操作
				tmptx, err = tmpdb.Begin(true)
				if err != nil {
					return err
				}
				tmpb = tmptx.Bucket(next)
				// 设置填充比例
				tmpb.FillPercent = 0.9 // for seq write in for each

				count = 0
			}
			return tmpb.Put(k, v) // 将读取到的键值对写入到新数据库文件中
		})
	}

	return tmptx.Commit() // 提交读写事务（新数据库）
}

// 该方法根据入参开启新的只读事务或读写事务，并更新backend中的相关字段
func (b *backend) begin(write bool) *bolt.Tx {
	b.mu.RLock()
	tx := b.unsafeBegin(write) // 开启事务
	b.mu.RUnlock()

	size := tx.Size()
	db := tx.DB()
	stats := db.Stats()
	atomic.StoreInt64(&b.size, size) // 更新backend-size字段，记录当前数据库大小
	atomic.StoreInt64(&b.sizeInUse, size-(int64(stats.FreePageN)*int64(db.Info().PageSize)))
	atomic.StoreInt64(&b.openReadTxN, int64(stats.OpenTxN))

	return tx
}

func (b *backend) unsafeBegin(write bool) *bolt.Tx {
	tx, err := b.db.Begin(write) // 调用bolt-db的接口开启事务
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to begin tx", zap.Error(err))
		} else {
			plog.Fatalf("cannot begin tx (%s)", err)
		}
	}
	return tx
}

func (b *backend) OpenReadTxN() int64 {
	return atomic.LoadInt64(&b.openReadTxN)
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(batchInterval time.Duration, batchLimit int) (*backend, string) {
	dir, err := ioutil.TempDir(os.TempDir(), "etcd_backend_test")
	if err != nil {
		panic(err)
	}
	tmpPath := filepath.Join(dir, "database")
	bcfg := DefaultBackendConfig()
	bcfg.Path, bcfg.BatchInterval, bcfg.BatchLimit = tmpPath, batchInterval, batchLimit
	return newBackend(bcfg), tmpPath
}

func NewDefaultTmpBackend() (*backend, string) {
	return NewTmpBackend(defaultBatchInterval, defaultBatchLimit)
}

// 该结构体实现了backend Snapshot接口，注意write-to方法比较重要
type snapshot struct {
	*bolt.Tx // backend snapshot write-to方法就是通过tx write-to方法实现的
	stopc chan struct{}
	donec chan struct{}
}

func (s *snapshot) Close() error {
	close(s.stopc)
	<-s.donec
	return s.Tx.Rollback()
}
