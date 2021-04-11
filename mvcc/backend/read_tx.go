// Copyright 2017 The etcd Authors
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

	bolt "go.etcd.io/bbolt"
)

// safeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
// overwrites on a bucket should only fetch with limit=1, but safeRangeBucket
// is known to never overwrite any key so range is safe.
var safeRangeBucket = []byte("key")

// 对只读事务的抽象
type ReadTx interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()

	// 在指定的bucket中进行范围查找
	UnsafeRange(bucketName []byte, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)
	// 遍历指定bucket中的全部键值对
	UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error
}

/*
 * ┌───────────────┐          ┌─────────────┐
 * │   readTx      │          │ txReadBuffer│
 * │               │          │             │    embed
 * ├───────────────┤          ├─────────────┼───────────────┐
 * │    buf        ├─────────►│             │               │
 * │               │          │             │               │
 * └───────────────┘          └─────────────┘               │
 *                                                   ┌──────▼────────┐           ┌────────────────┐
 *                                                   │  txBuffer     │           │   bucketBuffer │
 *                                                   ├───────────────┤           ├────────────────┤
 *                           ┌──────────────┐        │               ├──────────►│                │
 * ┌───────────────┐ embed   │   batchTx    │        │   buckets     │           │     buf        │
 * │batchTxBuffered├────────►├──────────────┤        └───────▲───────┘           └────────────────┘
 * ├───────────────┤         │              │                │
 * │               │         └──────────────┘                │
 * │    buf        ├──────┐                                  │
 * └───────────────┘      │                                  │
 *                        │  ┌──────────────┐    embed       │
 *                        └─►│txWriteBuffer ├────────────────┘
 *                           ├──────────────┤
 *                           │              │
 *                           └──────────────┘
 */
type readTx struct {
	// mu protects accesses to the txReadBuffer
	mu  sync.RWMutex
	buf txReadBuffer // 用来缓存bucket与其中键值对集合的映射关系

	// TODO: group and encapsulate {txMu, tx, buckets, txWg}, as they share the same lifecycle.
	// txMu protects accesses to buckets and tx on Range requests.
	txMu    sync.RWMutex
	tx      *bolt.Tx // 该read-tx实例底层封装的bolt-tx实例，即bolt-db层面的只读事务
	buckets map[string]*bolt.Bucket
	// txWg protects tx from being rolled back at the end of a batch interval until all reads using this tx are done.
	txWg *sync.WaitGroup
}

func (rt *readTx) Lock()    { rt.mu.Lock() }
func (rt *readTx) Unlock()  { rt.mu.Unlock() }
func (rt *readTx) RLock()   { rt.mu.RLock() }
func (rt *readTx) RUnlock() { rt.mu.RUnlock() }

func (rt *readTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	// 只有查询名称为key的bucket时，才是真正的范围查询，其他情况下只能返回一个键值对
	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {
		panic("do not use unsafeRange on non-keys bucket")
	}
	// 首先从缓存中查询键值对
	keys, vals := rt.buf.Range(bucketName, key, endKey, limit)
	// 检测缓存返回对键值对数量是否达到limit限制，如果达到，则直接返回缓存的查询结果
	if int64(len(keys)) == limit {
		return keys, vals
	}

	// find/cache bucket
	bn := string(bucketName)
	rt.txMu.RLock()
	bucket, ok := rt.buckets[bn]
	rt.txMu.RUnlock()
	if !ok {
		rt.txMu.Lock()
		bucket = rt.tx.Bucket(bucketName)
		rt.buckets[bn] = bucket
		rt.txMu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
		return keys, vals
	}
	rt.txMu.Lock()
	c := bucket.Cursor()
	rt.txMu.Unlock()

	// 通过unsafe-range从bolt-db中查询
	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))
	// 将查询缓存的结果与查询bolt-db的结果合并，然后返回
	return append(k2, keys...), append(v2, vals...)
}

func (rt *readTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	if err := rt.buf.ForEach(bucketName, getDups); err != nil {
		return err
	}
	rt.txMu.Lock()
	err := unsafeForEach(rt.tx, bucketName, visitNoDup)
	rt.txMu.Unlock()
	if err != nil {
		return err
	}
	return rt.buf.ForEach(bucketName, visitor)
}

func (rt *readTx) reset() {
	rt.buf.reset()
	rt.buckets = make(map[string]*bolt.Bucket)
	rt.tx = nil
	rt.txWg = new(sync.WaitGroup)
}

// TODO: create a base type for readTx and concurrentReadTx to avoid duplicated function implementation?
type concurrentReadTx struct {
	buf     txReadBuffer
	txMu    *sync.RWMutex
	tx      *bolt.Tx
	buckets map[string]*bolt.Bucket
	txWg    *sync.WaitGroup
}

func (rt *concurrentReadTx) Lock()   {}
func (rt *concurrentReadTx) Unlock() {}

// RLock is no-op. concurrentReadTx does not need to be locked after it is created.
func (rt *concurrentReadTx) RLock() {}

// RUnlock signals the end of concurrentReadTx.
func (rt *concurrentReadTx) RUnlock() { rt.txWg.Done() }

func (rt *concurrentReadTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	if err := rt.buf.ForEach(bucketName, getDups); err != nil {
		return err
	}
	rt.txMu.Lock()
	err := unsafeForEach(rt.tx, bucketName, visitNoDup)
	rt.txMu.Unlock()
	if err != nil {
		return err
	}
	return rt.buf.ForEach(bucketName, visitor)
}

func (rt *concurrentReadTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {
		panic("do not use unsafeRange on non-keys bucket")
	}
	keys, vals := rt.buf.Range(bucketName, key, endKey, limit)
	if int64(len(keys)) == limit {
		return keys, vals
	}

	// find/cache bucket
	bn := string(bucketName)
	rt.txMu.RLock()
	bucket, ok := rt.buckets[bn]
	rt.txMu.RUnlock()
	if !ok {
		rt.txMu.Lock()
		bucket = rt.tx.Bucket(bucketName)
		rt.buckets[bn] = bucket
		rt.txMu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
		return keys, vals
	}
	rt.txMu.Lock()
	c := bucket.Cursor()
	rt.txMu.Unlock()

	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))
	return append(k2, keys...), append(v2, vals...)
}
