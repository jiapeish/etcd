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
	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// 对批量读写事务的抽象
type BatchTx interface {
	ReadTx // 内嵌对只读事务的抽象接口
	UnsafeCreateBucket(name []byte) // 创建bucket
	UnsafePut(bucketName []byte, key []byte, value []byte)
	// 向指定bucket中添加键值对，与unsafe-put方法的区别是，
	// 该方法会将对应bucket实例的填充比例设置为90%，这样可以在顺序写入时，提高bucket利用率
	UnsafeSeqPut(bucketName []byte, key []byte, value []byte)
	// 在指定bucket中删除指定的键值对
	UnsafeDelete(bucketName []byte, key []byte)
	// 提交当前的读写事务，之后立即打开一个新的读写事务
	// Commit commits a previous tx and begins a new writable one.
	Commit()
	// 提交当前的读写事务，之后不会打开新的读写事务
	// CommitAndStop commits the previous tx and does not create a new one.
	CommitAndStop()
}

type batchTx struct {
	sync.Mutex
	tx      *bolt.Tx // bolt-db层面的读写事务
	backend *backend // 关联的backend实例

	pending int // 当前事务中执行的修改操作个数，在当前读写事务提交时，该字段会被重置为0
}

func (t *batchTx) Lock() {
	t.Mutex.Lock()
}

func (t *batchTx) Unlock() {
	if t.pending >= t.backend.batchLimit { // 检测当前事务的修改操作是否达到上限
		t.commit(false) // 提交当前读写事务，并开启新的事务
	}
	t.Mutex.Unlock()
}

// BatchTx interface embeds ReadTx interface. But RLock() and RUnlock() do not
// have appropriate semantics in BatchTx interface. Therefore should not be called.
// TODO: might want to decouple ReadTx and BatchTx

func (t *batchTx) RLock() {
	panic("unexpected RLock")
}

func (t *batchTx) RUnlock() {
	panic("unexpected RUnlock")
}

func (t *batchTx) UnsafeCreateBucket(name []byte) {
	_, err := t.tx.CreateBucket(name) // 调用bolt-db接口创建相应的bucket实例
	if err != nil && err != bolt.ErrBucketExists {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to create a bucket",
				zap.String("bucket-name", string(name)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot create bucket %s (%v)", name, err)
		}
	}
	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
func (t *batchTx) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, false)
}

// UnsafeSeqPut must be called holding the lock on the tx.
func (t *batchTx) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, true)
}

func (t *batchTx) unsafePut(bucketName []byte, key []byte, value []byte, seq bool) {
	bucket := t.tx.Bucket(bucketName) // 通过bolt-db接口获取指定的bucket实例
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}
	if seq { // 顺序写入时，将填充率设置为0.9
		// it is useful to increase fill percent when the workloads are mostly append-only.
		// this can delay the page split and reduce space usage.
		bucket.FillPercent = 0.9
	}
	if err := bucket.Put(key, value); err != nil { // 调用bolt-db接口写入键值对
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to write to a bucket",
				zap.String("bucket-name", string(bucketName)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot put key into bucket (%v)", err)
		}
	}
	t.pending++ // 递增
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}
	return unsafeRange(bucket.Cursor(), key, endKey, limit)
}

func unsafeRange(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else { // 如果没有指定end-key，则直接查找指定key对应的键值对并返回
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() { // 从key位置开始遍历
		vs = append(vs, cv) // 记录符合条件的value值
		keys = append(keys, ck) // 记录符合条件的key值
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTx) UnsafeDelete(bucketName []byte, key []byte) {
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}
	err := bucket.Delete(key)
	if err != nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to delete a key",
				zap.String("bucket-name", string(bucketName)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot delete key from bucket (%v)", err)
		}
	}
	t.pending++
}

// UnsafeForEach must be called holding the lock on the tx.
func (t *batchTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	return unsafeForEach(t.tx, bucketName, visitor)
}

func unsafeForEach(tx *bolt.Tx, bucket []byte, visitor func(k, v []byte) error) error {
	if b := tx.Bucket(bucket); b != nil { // 查找指定的bucket实例
		return b.ForEach(visitor) // 对bolt-db中键值对进行遍历
	}
	return nil
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTx) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTx) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTx) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

// 处理读写事务
func (t *batchTx) commit(stop bool) {
	// commit the last tx
	if t.tx != nil {
		if t.pending == 0 && !stop { // 当前读写事务中未进行任何修改操作，则无须开启新事务
			return
		}

		start := time.Now()

		// gofail: var beforeCommit struct{}
		err := t.tx.Commit() // 通过bolt-db提供的接口提交当前读写事务
		// gofail: var afterCommit struct{}

		rebalanceSec.Observe(t.tx.Stats().RebalanceTime.Seconds())
		spillSec.Observe(t.tx.Stats().SpillTime.Seconds())
		writeSec.Observe(t.tx.Stats().WriteTime.Seconds())
		commitSec.Observe(time.Since(start).Seconds())
		atomic.AddInt64(&t.backend.commits, 1) // 递增commits字段

		t.pending = 0 // 重置pending字段
		if err != nil {
			if t.backend.lg != nil {
				t.backend.lg.Fatal("failed to commit tx", zap.Error(err))
			} else {
				plog.Fatalf("cannot commit tx (%s)", err)
			}
		}
	}
	if !stop {
		t.tx = t.backend.begin(true) // 开启新的读写事务
	}
}

type batchTxBuffered struct {
	batchTx
	buf txWriteBuffer
}

func newBatchTxBuffered(backend *backend) *batchTxBuffered {
	tx := &batchTxBuffered{
		batchTx: batchTx{backend: backend},
		buf: txWriteBuffer{
			txBuffer: txBuffer{make(map[string]*bucketBuffer)},
			seq:      true,
		},
	}
	tx.Commit() // 开启一个读写事务
	return tx
}

func (t *batchTxBuffered) Unlock() {
	if t.pending != 0 { // 检测当前读写事务中是否发生了修改操作
		t.backend.readTx.Lock() // blocks txReadBuffer for writing.
		// 将当前batch-tx-buffer中缓存的键值对更新到read-tx缓存中，从而实现只读事务的缓存更新
		t.buf.writeback(&t.backend.readTx.buf)
		t.backend.readTx.Unlock()
		if t.pending >= t.backend.batchLimit {
			t.commit(false) // 如果当前事务的修改操作数达到上限，则提交当前事务，并开启新事务
		}
	}
	t.batchTx.Unlock()
}

func (t *batchTxBuffered) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

func (t *batchTxBuffered) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTxBuffered) commit(stop bool) {
	// all read txs must be closed to acquire boltdb commit rwlock
	t.backend.readTx.Lock()
	t.unsafeCommit(stop)
	t.backend.readTx.Unlock()
}

// 先回滚当前的只读事务，提交当前的读写事务，然后开启新的只读事务和读写事务
func (t *batchTxBuffered) unsafeCommit(stop bool) {
	if t.backend.readTx.tx != nil { // 如果当前已经开启了只读事务，则将该事务回滚（bolt-db中的只读事务只能回滚，无法提交）
		// wait all store read transactions using the current boltdb tx to finish,
		// then close the boltdb tx
		go func(tx *bolt.Tx, wg *sync.WaitGroup) {
			wg.Wait()
			if err := tx.Rollback(); err != nil {
				if t.backend.lg != nil {
					t.backend.lg.Fatal("failed to rollback tx", zap.Error(err))
				} else {
					plog.Fatalf("cannot rollback tx (%s)", err)
				}
			}
		}(t.backend.readTx.tx, t.backend.readTx.txWg)
		t.backend.readTx.reset() // 清空read-tx中的缓存
	}

	t.batchTx.commit(stop) // 如果当前已经开启了读写事务，则将该事务提交，并创建新的读写事务

	if !stop { // 根据该参数决定事务是否开启新的只读事务
		t.backend.readTx.tx = t.backend.begin(false)
	}
}

func (t *batchTxBuffered) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.batchTx.UnsafePut(bucketName, key, value) // 将键值对写入bolt-db
	t.buf.put(bucketName, key, value) // 将键值对写入batch-tx-buffer的缓存
}

func (t *batchTxBuffered) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.batchTx.UnsafeSeqPut(bucketName, key, value)
	t.buf.putSeq(bucketName, key, value) // 将键值对写入batch-tx-buffer的缓存
}
