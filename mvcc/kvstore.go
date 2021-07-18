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

package mvcc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/schedule"
	"go.etcd.io/etcd/pkg/traceutil"

	"github.com/coreos/pkg/capnslog"
	"go.uber.org/zap"
)

var (
	keyBucketName  = []byte("key")
	metaBucketName = []byte("meta")

	consistentIndexKeyName  = []byte("consistent_index")
	scheduledCompactKeyName = []byte("scheduledCompactRev")
	finishedCompactKeyName  = []byte("finishedCompactRev")

	ErrCompacted = errors.New("mvcc: required revision has been compacted")
	ErrFutureRev = errors.New("mvcc: required revision is a future revision")
	ErrCanceled  = errors.New("mvcc: watcher is canceled")
	ErrClosed    = errors.New("mvcc: closed")

	plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "mvcc")
)

const (
	// markedRevBytesLen is the byte length of marked revision.
	// The first `revBytesLen` bytes represents a normal revision. The last
	// one byte is the mark.
	markedRevBytesLen      = revBytesLen + 1
	markBytePosition       = markedRevBytesLen - 1
	markTombstone     byte = 't'
)

var restoreChunkKeys = 10000 // non-const for testing
var defaultCompactBatchLimit = 1000

// ConsistentIndexGetter is an interface that wraps the Get method.
// Consistent index is the offset of an entry in a consistent replicated log.
type ConsistentIndexGetter interface {
	// ConsistentIndex returns the consistent index of current executing entry.
	ConsistentIndex() uint64
}

type StoreConfig struct {
	CompactionBatchLimit int
}

type store struct {
	ReadView
	WriteView

	// consistentIndex caches the "consistent_index" key's value. Accessed
	// through atomics so must be 64-bit aligned.
	consistentIndex uint64

	cfg StoreConfig

	// mu read locks for txns and write locks for non-txn store changes.
	mu sync.RWMutex

	ig ConsistentIndexGetter

	b       backend.Backend // 当前store实例关联的后端存储
	kvindex index // 当前store实例关联的内存索引

	le lease.Lessor // 租约相关

	// revMuLock protects currentRev and compactMainRev.
	// Locked at end of write txn and released after write txn unlock lock.
	// Locked before locking read txn and released after locking.
	revMu sync.RWMutex
	// currentRev is the revision of the last completed transaction.
	currentRev int64
	// compactMainRev is the main revision of the last compaction.
	compactMainRev int64 // 记录最近一次压缩后最小的revision信息（main revision部分的值）

	// bytesBuf8 is a byte slice of length 8
	// to avoid a repetitive allocation in saveIndex.
	bytesBuf8 []byte // 索引缓冲区，主要用于记录consistent-index

	fifoSched schedule.Scheduler // fifo调度器

	stopc chan struct{}

	lg *zap.Logger
}

// NewStore returns a new store. It is useful to create a store inside
// mvcc pkg. It should only be used for testing externally.
func NewStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, ig ConsistentIndexGetter, cfg StoreConfig) *store {
	if cfg.CompactionBatchLimit == 0 {
		cfg.CompactionBatchLimit = defaultCompactBatchLimit
	}
	s := &store{
		cfg:     cfg,
		b:       b, // 初始化backend字段
		ig:      ig,
		kvindex: newTreeIndex(lg),

		le: le,

		currentRev:     1,
		compactMainRev: -1,

		bytesBuf8: make([]byte, 8),
		fifoSched: schedule.NewFIFOScheduler(),

		stopc: make(chan struct{}),

		lg: lg,
	}
	s.ReadView = &readView{s}
	s.WriteView = &writeView{s}
	if s.le != nil {
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write(traceutil.TODO()) })
	}

	// 获取backend的读写事务，创建名为"key"和"meta"的两个bucket，然后提交事务
	tx := s.b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(keyBucketName)
	tx.UnsafeCreateBucket(metaBucketName)
	tx.Unlock()
	s.b.ForceCommit()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.restore(); err != nil { // 从backend中恢复当前store的所有状态，其中包括内存中的btree索引等
		// TODO: return the error instead of panic here?
		panic("failed to recover store from backend")
	}

	return s
}

func (s *store) compactBarrier(ctx context.Context, ch chan struct{}) {
	if ctx == nil || ctx.Err() != nil {
		select {
		case <-s.stopc:
		default:
			// fix deadlock in mvcc,for more information, please refer to pr 11817.
			// s.stopc is only updated in restore operation, which is called by apply
			// snapshot call, compaction and apply snapshot requests are serialized by
			// raft, and do not happen at the same time.
			s.mu.Lock()
			f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
			s.fifoSched.Schedule(f)
			s.mu.Unlock()
		}
		return
	}
	close(ch)
}

func (s *store) Hash() (hash uint32, revision int64, err error) {
	start := time.Now()

	s.b.ForceCommit()
	h, err := s.b.Hash(DefaultIgnores)

	hashSec.Observe(time.Since(start).Seconds())
	return h, s.currentRev, err
}

func (s *store) HashByRev(rev int64) (hash uint32, currentRev int64, compactRev int64, err error) {
	start := time.Now()

	s.mu.RLock()
	s.revMu.RLock()
	compactRev, currentRev = s.compactMainRev, s.currentRev
	s.revMu.RUnlock()

	if rev > 0 && rev <= compactRev {
		s.mu.RUnlock()
		return 0, 0, compactRev, ErrCompacted
	} else if rev > 0 && rev > currentRev {
		s.mu.RUnlock()
		return 0, currentRev, 0, ErrFutureRev
	}

	if rev == 0 {
		rev = currentRev
	}
	keep := s.kvindex.Keep(rev)

	tx := s.b.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	s.mu.RUnlock()

	upper := revision{main: rev + 1}
	lower := revision{main: compactRev + 1}
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	h.Write(keyBucketName)
	err = tx.UnsafeForEach(keyBucketName, func(k, v []byte) error {
		kr := bytesToRev(k)
		if !upper.GreaterThan(kr) {
			return nil
		}
		// skip revisions that are scheduled for deletion
		// due to compacting; don't skip if there isn't one.
		if lower.GreaterThan(kr) && len(keep) > 0 {
			if _, ok := keep[kr]; !ok {
				return nil
			}
		}
		h.Write(k)
		h.Write(v)
		return nil
	})
	hash = h.Sum32()

	hashRevSec.Observe(time.Since(start).Seconds())
	return hash, currentRev, compactRev, err
}

func (s *store) updateCompactRev(rev int64) (<-chan struct{}, error) {
	s.revMu.Lock() // 获取rev-mu的写锁
	if rev <= s.compactMainRev { // 检测传入的rev参数是否合法
		ch := make(chan struct{})
		f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
		s.fifoSched.Schedule(f)
		s.revMu.Unlock()
		return ch, ErrCompacted
	}
	if rev > s.currentRev {
		s.revMu.Unlock()
		return nil, ErrFutureRev
	}

	s.compactMainRev = rev // 更新compact-main-rev

	rbytes := newRevBytes()
	revToBytes(revision{main: rev}, rbytes) // 将传入的rev值封装成revision实例，并写入[]byte切片中

	tx := s.b.BatchTx() // 获取bolt-db中的读写事务，并将上边创建的revision实例写入meta bucket
	tx.Lock()
	tx.UnsafePut(metaBucketName, scheduledCompactKeyName, rbytes)
	tx.Unlock()
	// ensure that desired compaction is persisted
	s.b.ForceCommit() // 提交当前读写事务

	s.revMu.Unlock()

	return nil, nil
}

func (s *store) compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	ch := make(chan struct{})
	var j = func(ctx context.Context) {
		if ctx.Err() != nil {
			s.compactBarrier(ctx, ch)
			return
		}
		start := time.Now()
		keep := s.kvindex.Compact(rev) // 对内存索引进行压缩，其返回值是此次压缩过程中涉及的revision实例
		indexCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))
		if !s.scheduleCompaction(rev, keep) { // 真正对bolt-db进行压缩的地方
			s.compactBarrier(nil, ch)
			return
		}
		close(ch) // 当正常压缩之后，会关闭该通道，通知监听者
	}

	// fifo-sched是一个fifo scheduler，在scheduler接口定义了一个schedule(j Job)方法，该方法会将Job放入调度器进行调度，
	// 其中的job实际上就是一个func(context.Context)函数，
	// 这里将对bolt-db的压缩操作(即上边定义的j函数）放入fifo调度器中，异步执行
	s.fifoSched.Schedule(j)
	trace.Step("schedule compaction")
	return ch, nil
}

func (s *store) compactLockfree(rev int64) (<-chan struct{}, error) {
	ch, err := s.updateCompactRev(rev)
	if nil != err {
		return ch, err
	}

	return s.compact(traceutil.TODO(), rev)
}

// 用于实现键值对的压缩；
// 1、将传入的参数转换成revision实例；
// 2、在meta bucket中记录此次压缩的键值对信息(key为scheduled-compact-rev，value为revision)；
// 3、调用tree-index-compact方法完成对内存索引的压缩；
// 4、通过fifo-scheduler异步完成对bolt-db的压缩；
func (s *store) Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	s.mu.Lock() // 获取mu的写锁

	ch, err := s.updateCompactRev(rev)
	trace.Step("check and update compact revision")
	if err != nil {
		s.mu.Unlock()
		return ch, err
	}
	s.mu.Unlock()

	return s.compact(trace, rev)
}

// DefaultIgnores is a map of keys to ignore in hash checking.
var DefaultIgnores map[backend.IgnoreKey]struct{}

func init() {
	DefaultIgnores = map[backend.IgnoreKey]struct{}{
		// consistent index might be changed due to v2 internal sync, which
		// is not controllable by the user.
		{Bucket: string(metaBucketName), Key: string(consistentIndexKeyName)}: {},
	}
}

func (s *store) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.b.BatchTx()
	tx.Lock()
	s.saveIndex(tx)
	tx.Unlock()
	s.b.ForceCommit()
}

// 当follower节点收到快照数据时，会使用快照数据恢复当前节点的状态；
// 调用该方法完成内存索引和store中其他状态的恢复
func (s *store) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.stopc)
	s.fifoSched.Stop()

	atomic.StoreUint64(&s.consistentIndex, 0)
	s.b = b // 指向新的backend实例
	s.kvindex = newTreeIndex(s.lg) // 指向新的内存索引
	s.currentRev = 1
	s.compactMainRev = -1
	s.fifoSched = schedule.NewFIFOScheduler() // 指向新的fifo scheduler
	s.stopc = make(chan struct{})

	return s.restore() // 开始恢复内存索引
}

// 1、批量读取bolt-db中所有键值对数据；
// 2、将每个键值对封装成rev-key-value实例，并写入一个channel；
// 3、由另一个单独的goroutine读取该通道，并完成内存索引的恢复；
func (s *store) restore() error {
	s.setupMetricsReporter()

	min, max := newRevBytes(), newRevBytes() // 创建min, max; 后续在bolt-db中进行范围查询时的起始key和结束key就是min, max
	revToBytes(revision{main: 1}, min) // min为1_0
	revToBytes(revision{main: math.MaxInt64, sub: math.MaxInt64}, max)

	keyToLease := make(map[string]lease.LeaseID)

	// restore index
	tx := s.b.BatchTx() // 获取读写事务，并加锁
	tx.Lock()

	// 在meta bucket中查询上次的压缩完成时的相关记录
	_, finishedCompactBytes := tx.UnsafeRange(metaBucketName, finishedCompactKeyName, nil, 0)
	if len(finishedCompactBytes) != 0 { // 根据查询结果，恢复compact-main-rev字段
		s.compactMainRev = bytesToRev(finishedCompactBytes[0]).main

		if s.lg != nil {
			s.lg.Info(
				"restored last compact revision",
				zap.String("meta-bucket-name", string(metaBucketName)),
				zap.String("meta-bucket-name-key", string(finishedCompactKeyName)),
				zap.Int64("restored-compact-revision", s.compactMainRev),
			)
		} else {
			plog.Printf("restore compact to %d", s.compactMainRev)
		}
	}
	// 在meta bucket中查询上次的压缩启动时的相关记录
	_, scheduledCompactBytes := tx.UnsafeRange(metaBucketName, scheduledCompactKeyName, nil, 0)
	scheduledCompact := int64(0)
	if len(scheduledCompactBytes) != 0 { // 根据查询结果恢复scheduled-compact
		scheduledCompact = bytesToRev(scheduledCompactBytes[0]).main
	}

	// index keys concurrently as they're loaded in from tx
	keysGauge.Set(0)
	// 这里会启动一个单独的goroutine，用于接收从backend中读取的键值对数据，并恢复到新建的内存索引中(kv-index)
	rkvc, revc := restoreIntoIndex(s.lg, s.kvindex)
	for {
		// 查询bolt-db中的key bucket，返回键值对数量的上限由第4个参数指定
		keys, vals := tx.UnsafeRange(keyBucketName, min, max, int64(restoreChunkKeys))
		if len(keys) == 0 { // 查询结果为空时直接跳出循环
			break
		}
		// rkvc blocks if the total pending keys exceeds the restore
		// chunk size to keep keys from consuming too much memory.
		restoreChunk(s.lg, rkvc, keys, vals, keyToLease) // 将查询到的键值对写入recv-kv-chan中
		if len(keys) < restoreChunkKeys {
			// partial set implies final set
			// 范围查询得到的结果数小于restore-chunk-keys，表示最后一次查询
			break
		}
		// next set begins after where this one ended
		// 更新min作为下一次范围查询的起始key
		newMin := bytesToRev(keys[len(keys)-1][:revBytesLen])
		newMin.sub++
		revToBytes(newMin, min)
	}
	close(rkvc)
	s.currentRev = <-revc // 从该通道获取恢复之后的current-rev

	// keys in the range [compacted revision -N, compaction] might all be deleted due to compaction.
	// the correct revision should be set to compaction revision in the case, not the largest revision
	// we have seen.
	if s.currentRev < s.compactMainRev {
		s.currentRev = s.compactMainRev
	}
	if scheduledCompact <= s.compactMainRev {
		scheduledCompact = 0
	}

	for key, lid := range keyToLease {
		if s.le == nil {
			panic("no lessor to attach lease")
		}
		err := s.le.Attach(lid, []lease.LeaseItem{{Key: key}})
		if err != nil {
			if s.lg != nil {
				s.lg.Warn(
					"failed to attach a lease",
					zap.String("lease-id", fmt.Sprintf("%016x", lid)),
					zap.Error(err),
				)
			} else {
				plog.Errorf("unexpected Attach error: %v", err)
			}
		}
	}

	tx.Unlock()

	if scheduledCompact != 0 { // 如果在开始恢复之前，存在未执行完的压缩操作，则重启该压缩操作；
		s.compactLockfree(scheduledCompact)

		if s.lg != nil {
			s.lg.Info(
				"resume scheduled compaction",
				zap.String("meta-bucket-name", string(metaBucketName)),
				zap.String("meta-bucket-name-key", string(scheduledCompactKeyName)),
				zap.Int64("scheduled-compact-revision", scheduledCompact),
			)
		} else {
			plog.Printf("resume scheduled compaction at %d", scheduledCompact)
		}
	}

	return nil
}

type revKeyValue struct {
	key  []byte // bolt-db中的key值，可以转换得到revision实例
	kv   mvccpb.KeyValue // bolt-db中的value值
	kstr string // 原始的key值
}

// 该函数启动一个后台goroutine，读取recv-kv-chan中的rev-key-value实例，并将其中的键值对数据恢复到内存索引中
func restoreIntoIndex(lg *zap.Logger, idx index) (chan<- revKeyValue, <-chan int64) {
	rkvc, revc := make(chan revKeyValue, restoreChunkKeys), make(chan int64, 1)
	go func() {
		currentRev := int64(1) // 记录当前遇到的最大的main revision值
		// 在该goroutine结束时（即recv-kv-chan关闭时），将当前已知的最大main revision值写入rev-chan中，待其他goroutine读取
		defer func() { revc <- currentRev }()
		// restore the tree index from streaming the unordered index.
		// 虽然b-tree查找效率很高，但随着深度增加，效率随之下降，
		// 这里使用map做了一层缓存，更加高效的查找对应的key-index实例;
		// 前边从bolt-db中读取到的键值对数据并不是按照原始key值进行排序的，如果直接向b-tree写入，
		// 则可能会引起节点的分裂等变换操作，效率比较低，所以加了这一层map做的缓存；
		kiCache := make(map[string]*keyIndex, restoreChunkKeys)
		for rkv := range rkvc { // 从channel中读取rev-key-value实例
			ki, ok := kiCache[rkv.kstr] // 先查询缓存
			// purge kiCache if many keys but still missing in the cache
			// 如果ki-cache中缓存了大量的key，但是依然没有命中，则清理缓存；
			if !ok && len(kiCache) >= restoreChunkKeys {
				i := 10 // 只清理10个key
				for k := range kiCache {
					delete(kiCache, k)
					if i--; i == 0 {
						break
					}
				}
			}
			// cache miss, fetch from tree index if there
			if !ok { // 如果缓存未命中，则从内存索引中查找对应的key-index实例，并添加到缓存中
				ki = &keyIndex{key: rkv.kv.Key}
				if idxKey := idx.KeyIndex(ki); idxKey != nil {
					kiCache[rkv.kstr], ki = idxKey, idxKey
					ok = true
				}
			}
			rev := bytesToRev(rkv.key) // 将rev-key-value中的key转换成revision实例
			currentRev = rev.main // 更新current-rev值
			if ok {
				if isTombstone(rkv.key) { // 当前rev-key-value实例对应一个tomb-stone键值对
					ki.tombstone(lg, rev.main, rev.sub) // 在对应的key-index实例中插入tomb-stone
					continue
				}
				ki.put(lg, rev.main, rev.sub) // 在对应的key-index实例中添加正常的revision信息
			} else if !isTombstone(rkv.key) {
				// 如果从内存索引中仍然未查询到对应的key-index实例，则需要填充key-index实例中的其他字段，并添加到内存索引中
				ki.restore(lg, revision{rkv.kv.CreateRevision, 0}, rev, rkv.kv.Version)
				idx.Insert(ki)
				kiCache[rkv.kstr] = ki // 同时会将该key-index实例添加到缓存中
			}
		}
	}()
	return rkvc, revc
}

// restore方法中通过unsafe-range方法从bolt-db中查询键值对，然后通过该方法转换成recv-kv实例，并写入recv-kv-chan中
func restoreChunk(lg *zap.Logger, kvc chan<- revKeyValue, keys, vals [][]byte, keyToLease map[string]lease.LeaseID) {
	for i, key := range keys { // 遍历读取到的revision信息
		rkv := revKeyValue{key: key}
		if err := rkv.kv.Unmarshal(vals[i]); err != nil { // 反序列化得到key-value
			if lg != nil {
				lg.Fatal("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
			} else {
				plog.Fatalf("cannot unmarshal event: %v", err)
			}
		}
		rkv.kstr = string(rkv.kv.Key) // 记录原始key值
		if isTombstone(key) {
			delete(keyToLease, rkv.kstr) // 删除tomb-stone标识的键值对
		} else if lid := lease.LeaseID(rkv.kv.Lease); lid != lease.NoLease {
			keyToLease[rkv.kstr] = lid
		} else {
			delete(keyToLease, rkv.kstr)
		}
		kvc <- rkv // 写入chan等待处理
	}
}

func (s *store) Close() error {
	close(s.stopc)
	s.fifoSched.Stop()
	return nil
}

func (s *store) saveIndex(tx backend.BatchTx) {
	if s.ig == nil {
		return
	}
	bs := s.bytesBuf8
	ci := s.ig.ConsistentIndex()
	binary.BigEndian.PutUint64(bs, ci) // 将consistent-index值写入缓冲区
	// put the index into the underlying backend
	// tx has been locked in TxnBegin, so there is no need to lock it again
	tx.UnsafePut(metaBucketName, consistentIndexKeyName, bs) // 将consistent-index值记录到meta bucket中
	atomic.StoreUint64(&s.consistentIndex, ci)
}

func (s *store) ConsistentIndex() uint64 {
	if ci := atomic.LoadUint64(&s.consistentIndex); ci > 0 {
		return ci
	}
	tx := s.b.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	_, vs := tx.UnsafeRange(metaBucketName, consistentIndexKeyName, nil, 0)
	if len(vs) == 0 {
		return 0
	}
	v := binary.BigEndian.Uint64(vs[0])
	atomic.StoreUint64(&s.consistentIndex, v)
	return v
}

func (s *store) setupMetricsReporter() {
	b := s.b
	reportDbTotalSizeInBytesMu.Lock()
	reportDbTotalSizeInBytes = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesMu.Unlock()
	reportDbTotalSizeInBytesDebugMu.Lock()
	reportDbTotalSizeInBytesDebug = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesDebugMu.Unlock()
	reportDbTotalSizeInUseInBytesMu.Lock()
	reportDbTotalSizeInUseInBytes = func() float64 { return float64(b.SizeInUse()) }
	reportDbTotalSizeInUseInBytesMu.Unlock()
	reportDbOpenReadTxNMu.Lock()
	reportDbOpenReadTxN = func() float64 { return float64(b.OpenReadTxN()) }
	reportDbOpenReadTxNMu.Unlock()
	reportCurrentRevMu.Lock()
	reportCurrentRev = func() float64 {
		s.revMu.RLock()
		defer s.revMu.RUnlock()
		return float64(s.currentRev)
	}
	reportCurrentRevMu.Unlock()
	reportCompactRevMu.Lock()
	reportCompactRev = func() float64 {
		s.revMu.RLock()
		defer s.revMu.RUnlock()
		return float64(s.compactMainRev)
	}
	reportCompactRevMu.Unlock()
}

// appendMarkTombstone appends tombstone mark to normal revision bytes.
func appendMarkTombstone(lg *zap.Logger, b []byte) []byte {
	if len(b) != revBytesLen {
		if lg != nil {
			lg.Panic(
				"cannot append tombstone mark to non-normal revision bytes",
				zap.Int("expected-revision-bytes-size", revBytesLen),
				zap.Int("given-revision-bytes-size", len(b)),
			)
		} else {
			plog.Panicf("cannot append mark to non normal revision bytes")
		}
	}
	return append(b, markTombstone)
}

// isTombstone checks whether the revision bytes is a tombstone.
func isTombstone(b []byte) bool {
	return len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone
}
