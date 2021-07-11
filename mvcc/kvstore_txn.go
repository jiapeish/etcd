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

package mvcc

import (
	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/traceutil"
	"go.uber.org/zap"
)

type storeTxnRead struct {
	s  *store
	tx backend.ReadTx

	firstRev int64
	rev      int64

	trace *traceutil.Trace
}

func (s *store) Read(trace *traceutil.Trace) TxnRead {
	s.mu.RLock()
	s.revMu.RLock()
	// backend holds b.readTx.RLock() only when creating the concurrentReadTx. After
	// ConcurrentReadTx is created, it will not block write transaction.
	tx := s.b.ConcurrentReadTx()
	tx.RLock() // RLock is no-op. concurrentReadTx does not need to be locked after it is created.
	firstRev, rev := s.compactMainRev, s.currentRev
	s.revMu.RUnlock()
	return newMetricsTxnRead(&storeTxnRead{s, tx, firstRev, rev, trace})
}

func (tr *storeTxnRead) FirstRev() int64 { return tr.firstRev }
func (tr *storeTxnRead) Rev() int64      { return tr.rev }

func (tr *storeTxnRead) Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	return tr.rangeKeys(key, end, tr.Rev(), ro)
}

// 释放在store-read()方法中获取的底层只读事务上的锁、和store-mu上的读锁
func (tr *storeTxnRead) End() {
	tr.tx.RUnlock() // RUnlock signals the end of concurrentReadTx.
	tr.s.mu.RUnlock()
}

type storeTxnWrite struct {
	storeTxnRead
	tx backend.BatchTx // 当前实例关联的读写事务
	// beginRev is the revision where the txn begins; it will write to the next revision.
	// 记录创建当前store-txn-write实例时store-current-rev字段的值
	beginRev int64
	changes  []mvccpb.KeyValue // 在当前读写事务中发生改动的键值对信息
}

func (s *store) Write(trace *traceutil.Trace) TxnWrite {
	s.mu.RLock() // 加读锁
	tx := s.b.BatchTx() // 获取读写事务
	tx.Lock() // 获取读写事务的锁
	tw := &storeTxnWrite{ // 创建store-txn-write实例，实现了txn-write接口
		storeTxnRead: storeTxnRead{s, tx, 0, 0, trace}, // 其中first rev初始化为0
		tx:           tx,
		beginRev:     s.currentRev,
		changes:      make([]mvccpb.KeyValue, 0, 4),
	}
	return newMetricsTxnWrite(tw)
}

func (tw *storeTxnWrite) Rev() int64 { return tw.beginRev }

// 与store-txn-read的range方法的主要区别在于传入的rev参数
func (tw *storeTxnWrite) Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	rev := tw.beginRev
	if len(tw.changes) > 0 {
		// 如果当前读写事务中有更新键值对的操作，则该方法也需要能查询到这些更新的键值对，所以传入的rev参数是begin-rev + 1
		rev++
	}
	return tw.rangeKeys(key, end, rev, ro)
}

func (tw *storeTxnWrite) DeleteRange(key, end []byte) (int64, int64) {
	if n := tw.deleteRange(key, end); n != 0 || len(tw.changes) > 0 {
		return n, tw.beginRev + 1 // 根据当前事务是否有更新操作，决定返回的main revision的值
	}
	return 0, tw.beginRev
}

// 该方法实现向存储中追加一个键值对数据的功能
func (tw *storeTxnWrite) Put(key, value []byte, lease lease.LeaseID) int64 {
	tw.put(key, value, lease)
	return tw.beginRev + 1
}

// 1. 将consistent-index记录到名为"meta"的bucket中；
// 2. 递增store-current-rev;
// 3. 调用unlock方法提交当前事务，并开启新的读写事务；
func (tw *storeTxnWrite) End() {
	// only update index if the txn modifies the mvcc state.
	if len(tw.changes) != 0 { // 检测当前读写事务中是否有修改操作；
		// 将consistent-index(实际就是当前处理的最后一条entry记录的索引值)添加记录到bolt-db中名为"meta"的bucket中
		tw.s.saveIndex(tw.tx)
		// hold revMu lock to prevent new read txns from opening until writeback.
		tw.s.revMu.Lock() // 要修改store中的current-rev字段，需加读锁同步
		tw.s.currentRev++ // 递增current-rev
	}
	// 在batch-tx-buffered及batch-tx中的unlock方法中，会将当前读写事务提交，同时开启新的读写事务；
	tw.tx.Unlock()
	if len(tw.changes) != 0 {
		tw.s.revMu.Unlock() // 修改current-rev完成，释放读锁
	}
	tw.s.mu.RUnlock()
}

// 1. 扫描内存索引（即tree-index)，得到对应的revision;
// 2. 通过revision查询bolt-db得到真正的键值对数据；
// 3. 将键值对数据反序列化成key-value，并封装为range-result实例返回；
func (tr *storeTxnRead) rangeKeys(key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
	rev := ro.Rev
	if rev > curRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: curRev}, ErrFutureRev
	}
	if rev <= 0 {
		rev = curRev
	}
	if rev < tr.s.compactMainRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: 0}, ErrCompacted
	}

	// 检测range-options中指定的revision是否合法，如果不合法，则根据cur-rev（对store-txn-read来说，就是rev字段）
	// 当前的compact-main-rev值对rev进行矫正；
	// 调用kv-index的方法查询指定范围的键值对信息；
	revpairs := tr.s.kvindex.Revisions(key, end, rev)
	tr.trace.Step("range keys from in-memory index tree")
	if len(revpairs) == 0 {
		return &RangeResult{KVs: nil, Count: 0, Rev: curRev}, nil
	}
	if ro.Count {
		return &RangeResult{KVs: nil, Count: len(revpairs), Rev: curRev}, nil // 只获取键值对的个数时，只查询索引即可
	}

	limit := int(ro.Limit)
	if limit <= 0 || limit > len(revpairs) {
		limit = len(revpairs)
	}

	kvs := make([]mvccpb.KeyValue, limit)
	revBytes := newRevBytes() // 新建一个[]byte切片
	for i, revpair := range revpairs[:len(kvs)] {
		revToBytes(revpair, revBytes)
		_, vs := tr.tx.UnsafeRange(keyBucketName, revBytes, nil, 0) // 从bolt-db查询
		if len(vs) != 1 { // 虽然用unsafe-range进行查询，但查询结果只能有一个
			if tr.s.lg != nil {
				tr.s.lg.Fatal(
					"range failed to find revision pair",
					zap.Int64("revision-main", revpair.main),
					zap.Int64("revision-sub", revpair.sub),
				)
			} else {
				plog.Fatalf("range cannot find rev (%d,%d)", revpair.main, revpair.sub)
			}
		}
		// 将查询到的键值对数据进行反序列化，得到key-value实例，并追加到kvs中
		if err := kvs[i].Unmarshal(vs[0]); err != nil {
			if tr.s.lg != nil {
				tr.s.lg.Fatal(
					"failed to unmarshal mvccpb.KeyValue",
					zap.Error(err),
				)
			} else {
				plog.Fatalf("cannot unmarshal event: %v", err)
			}
		}
	}
	tr.trace.Step("range keys from bolt db")
	return &RangeResult{KVs: kvs, Count: len(revpairs), Rev: curRev}, nil // 将kvs封装为range-result实例并返回
}

func (tw *storeTxnWrite) put(key, value []byte, leaseID lease.LeaseID) {
	rev := tw.beginRev + 1 // 当前事务产生的修改对应的main revision部分都是该值
	c := rev
	oldLease := lease.NoLease

	// if the key exists before, use its previous created and
	// get its previous leaseID
	// 在内存索引中查找对应的键值对信息，在之后创建key-value实例时会用到这些值
	_, created, ver, err := tw.s.kvindex.Get(key, rev)
	if err == nil {
		c = created.main
		oldLease = tw.s.le.GetLease(lease.LeaseItem{Key: string(key)})
	}
	tw.trace.Step("get key's previous created_revision and leaseID")
	ibytes := newRevBytes()
	// 创建此次put操作对应的revision实例，其中main revision部分是begin-rev + 1
	idxRev := revision{main: rev, sub: int64(len(tw.changes))}
	// 将main revision和sub revision两部分写入i-bytes中，之后会将其作为写入bolt-db的key
	revToBytes(idxRev, ibytes)

	ver = ver + 1
	kv := mvccpb.KeyValue{ // 创建key-value实例
		Key:            key, // 原始key值
		Value:          value, // 原始value值
		CreateRevision: c, // 如果内存索引中已经存在该键值对，则创建版本不变；否则为新建key，将其设置为begin-rev + 1
		ModRevision:    rev, // begin-rev + 1
		Version:        ver, // 递增version
		Lease:          int64(leaseID),
	}

	d, err := kv.Marshal() // 将上面创建的kv实例序列化
	if err != nil {
		if tw.storeTxnRead.s.lg != nil {
			tw.storeTxnRead.s.lg.Fatal(
				"failed to marshal mvccpb.KeyValue",
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot marshal event: %v", err)
		}
	}

	tw.trace.Step("marshal mvccpb.KeyValue")
	// 将key（由main revision和sub revision组成）和value（上述kv实例序列化的结果）写入bolt-db中名为"key"的bucket
	tw.tx.UnsafeSeqPut(keyBucketName, ibytes, d)
	// 将原始key与revision实例的对应关系写入内存索引中
	tw.s.kvindex.Put(key, idxRev)
	// 将上述kv写入到changes中
	tw.changes = append(tw.changes, kv)
	tw.trace.Step("store kv pair into bolt db")

	if oldLease != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to detach lease")
		}
		err = tw.s.le.Detach(oldLease, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			if tw.storeTxnRead.s.lg != nil {
				tw.storeTxnRead.s.lg.Fatal(
					"failed to detach old lease from a key",
					zap.Error(err),
				)
			} else {
				plog.Errorf("unexpected error from lease detach: %v", err)
			}
		}
	}
	if leaseID != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to attach lease")
		}
		err = tw.s.le.Attach(leaseID, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			panic("unexpected error from lease Attach")
		}
	}
	tw.trace.Step("attach lease to kv pair")
}

func (tw *storeTxnWrite) deleteRange(key, end []byte) int64 {
	rrev := tw.beginRev
	if len(tw.changes) > 0 { // 根据当前事务是否有更新操作，决定后续使用的main revision值
		rrev++
	}

	// 在内存索引中查询待删除的key
	keys, _ := tw.s.kvindex.Range(key, end, rrev)
	if len(keys) == 0 {
		return 0
	}
	for _, key := range keys { // 遍历待删除的key，逐个删除
		tw.delete(key)
	}
	return int64(len(keys)) // 返回删除的个数
}

// 该方法会向bolt-db和内存索引中添加tomb-stone键值对
func (tw *storeTxnWrite) delete(key []byte) {
	ibytes := newRevBytes()
	idxRev := revision{main: tw.beginRev + 1, sub: int64(len(tw.changes))}
	revToBytes(idxRev, ibytes) // 将revision转换成bolt-db中的key

	if tw.storeTxnRead.s != nil && tw.storeTxnRead.s.lg != nil {
		ibytes = appendMarkTombstone(tw.storeTxnRead.s.lg, ibytes) // 添加一个't'标识tomb-stone
	} else {
		// TODO: remove this in v3.5
		ibytes = appendMarkTombstone(nil, ibytes)
	}

	kv := mvccpb.KeyValue{Key: key} // 创建一个key-value实例，其中只包含key

	d, err := kv.Marshal() // 进行序列化
	if err != nil {
		if tw.storeTxnRead.s.lg != nil {
			tw.storeTxnRead.s.lg.Fatal(
				"failed to marshal mvccpb.KeyValue",
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot marshal event: %v", err)
		}
	}

	// 将上边生成的key(ibytes)和value(key-value序列化后的数据)写入名为"key"的bucket中
	tw.tx.UnsafeSeqPut(keyBucketName, ibytes, d)
	// 在内存索引中添加tomb-stone
	err = tw.s.kvindex.Tombstone(key, idxRev)
	if err != nil {
		if tw.storeTxnRead.s.lg != nil {
			tw.storeTxnRead.s.lg.Fatal(
				"failed to tombstone an existing key",
				zap.String("key", string(key)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot tombstone an existing key (%s): %v", string(key), err)
		}
	}
	tw.changes = append(tw.changes, kv) // 向changes中追加上述key-value实例

	item := lease.LeaseItem{Key: string(key)}
	leaseID := tw.s.le.GetLease(item)

	if leaseID != lease.NoLease {
		err = tw.s.le.Detach(leaseID, []lease.LeaseItem{item})
		if err != nil {
			if tw.storeTxnRead.s.lg != nil {
				tw.storeTxnRead.s.lg.Fatal(
					"failed to detach old lease from a key",
					zap.Error(err),
				)
			} else {
				plog.Errorf("cannot detach %v", err)
			}
		}
	}
}

func (tw *storeTxnWrite) Changes() []mvccpb.KeyValue { return tw.changes }
