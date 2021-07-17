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
	"sort"
)

// txBuffer handles functionality shared between txWriteBuffer and txReadBuffer.
type txBuffer struct {
	buckets map[string]*bucketBuffer // 记录了bucket名称与对应的bucket-buffer之间的映射关系
}

// 负责清空buckets字段中的全部内容
func (txb *txBuffer) reset() {
	for k, v := range txb.buckets { // 遍历buckets
		if v.used == 0 {
			// demote
			delete(txb.buckets, k) // 删除未使用的bucket-buffer
		}
		v.used = 0 // 清空使用过的bucket-buffer
	}
}

// txWriteBuffer buffers writes of pending updates that have not yet committed.
type txWriteBuffer struct {
	txBuffer
	seq bool // 用于标记写入当前tx-write-buffer的键值对是否为顺序的
}

func (txw *txWriteBuffer) put(bucket, k, v []byte) {
	txw.seq = false // 表示非顺序写入
	txw.putSeq(bucket, k, v)
}

// 该方法用于向指定bucket-buffer添加键值对
func (txw *txWriteBuffer) putSeq(bucket, k, v []byte) {
	b, ok := txw.buckets[string(bucket)] // 获取指定的bucket-buffer
	if !ok { // 如果未查找到，则创建对应的bucket-buffer实例，并保存到buckets中
		b = newBucketBuffer()
		txw.buckets[string(bucket)] = b
	}
	b.add(k, v) // 添加键值对
}

// 将当前tx-write-buffer中键值对合并到指定对tr-read-buffer中，达到更新只读事务缓存的效果
func (txw *txWriteBuffer) writeback(txr *txReadBuffer) {
	for k, wb := range txw.buckets { // 遍历所有的bucket-buffer
		rb, ok := txr.buckets[k] // 从传入的bucket-buffer中查找指定的bucket-buffer
		if !ok { // 如果tr-read-buffer中不存在对应的bucket-buffer，则直接使用tx-write-buffer中缓存的bucket-buffer实例
			delete(txw.buckets, k)
			txr.buckets[k] = wb
			continue
		}
		if !txw.seq && wb.used > 1 {
			// assume no duplicate keys
			sort.Sort(wb) // 如果当前tx-write-buffer中的键值对是非顺序写入的，则先进行排序
		}
		rb.merge(wb) // 合并两个bucket-buffer实例并去重
	}
	txw.reset() // 清空tx-write-buffer
}

// txReadBuffer accesses buffered updates.
type txReadBuffer struct{ txBuffer }

func (txr *txReadBuffer) Range(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if b := txr.buckets[string(bucketName)]; b != nil {
		return b.Range(key, endKey, limit)
	}
	return nil, nil
}

func (txr *txReadBuffer) ForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	if b := txr.buckets[string(bucketName)]; b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// unsafeCopy returns a copy of txReadBuffer, caller should acquire backend.readTx.RLock()
func (txr *txReadBuffer) unsafeCopy() txReadBuffer {
	txrCopy := txReadBuffer{
		txBuffer: txBuffer{
			buckets: make(map[string]*bucketBuffer, len(txr.txBuffer.buckets)),
		},
	}
	for bucketName, bucket := range txr.txBuffer.buckets {
		txrCopy.txBuffer.buckets[bucketName] = bucket.Copy()
	}
	return txrCopy
}

type kv struct {
	key []byte
	val []byte
}

// bucketBuffer buffers key-value pairs that are pending commit.
type bucketBuffer struct {
	buf []kv // 每个元素表示一个键值对
	// used tracks number of elements in use so buf can be reused without reallocation.
	used int // 记录buf中目前使用的下标位置
}

func newBucketBuffer() *bucketBuffer {
	return &bucketBuffer{buf: make([]kv, 512), used: 0}
}

func (bb *bucketBuffer) Range(key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	f := func(i int) bool { return bytes.Compare(bb.buf[i].key, key) >= 0 } // 定义key的比较方式
	idx := sort.Search(bb.used, f) // 查询0 ~ used之间是否有指定的key
	if idx < 0 {
		return nil, nil
	}
	if len(endKey) == 0 { // 没有指定的key，则只返回key对应的键值对
		if bytes.Equal(key, bb.buf[idx].key) {
			keys = append(keys, bb.buf[idx].key)
			vals = append(vals, bb.buf[idx].val)
		}
		return keys, vals
	}
	if bytes.Compare(endKey, bb.buf[idx].key) <= 0 { // 如果指定了end-key，则检测其合法性
		return nil, nil
	}
	// 从前面查找到的idx位置开始遍历，直到遍历到end-key或是遍历的键值对个数达到上限
	for i := idx; i < bb.used && int64(len(keys)) < limit; i++ {
		if bytes.Compare(endKey, bb.buf[i].key) <= 0 {
			break
		}
		keys = append(keys, bb.buf[i].key)
		vals = append(vals, bb.buf[i].val)
	}
	return keys, vals // 返回全部符合条件的键值对
}

// 该方法提供了遍历当前bucket-buffer实例缓存的所有键值对的功能，visitor函数用于处理每个键值对
func (bb *bucketBuffer) ForEach(visitor func(k, v []byte) error) error {
	for i := 0; i < bb.used; i++ { // 遍历userd之前的所有元素
		if err := visitor(bb.buf[i].key, bb.buf[i].val); err != nil {
			return err
		}
	}
	return nil
}

// 添加键值对缓存，当buf的空间被用尽时，会进行扩容
func (bb *bucketBuffer) add(k, v []byte) {
	bb.buf[bb.used].key, bb.buf[bb.used].val = k, v
	bb.used++
	if bb.used == len(bb.buf) {
		buf := make([]kv, (3*len(bb.buf))/2)
		copy(buf, bb.buf)
		bb.buf = buf
	}
}

// merge merges data from bb into bbsrc.
// 将传入的bb-src与当前的bucket-buffer进行合并，之后会对合并结果进行排序和去重
func (bb *bucketBuffer) merge(bbsrc *bucketBuffer) {
	for i := 0; i < bbsrc.used; i++ { // 添加到当前的bucket-buffer中
		bb.add(bbsrc.buf[i].key, bbsrc.buf[i].val)
	}
	if bb.used == bbsrc.used { // 如果复制之前bucket-buffer是空的，则键值对复制完后直接返回
		return
	}
	// 如果复制之前bucket-buffer不是空的，则需要判断复制之后是否需要进行排序
	if bytes.Compare(bb.buf[(bb.used-bbsrc.used)-1].key, bbsrc.buf[0].key) < 0 {
		return
	}

	sort.Stable(bb) // 进行排序，稳定的，即相等键值对的相对位置在排序之后不会改变

	// remove duplicates, using only newest update
	widx := 0
	for ridx := 1; ridx < bb.used; ridx++ { // 清除重复的key，使用key的最新值
		if !bytes.Equal(bb.buf[ridx].key, bb.buf[widx].key) {
			widx++
		}
		bb.buf[widx] = bb.buf[ridx] // 新添加的键值对覆盖原有的键值对
	}
	bb.used = widx + 1
}

func (bb *bucketBuffer) Len() int { return bb.used }
func (bb *bucketBuffer) Less(i, j int) bool {
	return bytes.Compare(bb.buf[i].key, bb.buf[j].key) < 0
}
func (bb *bucketBuffer) Swap(i, j int) { bb.buf[i], bb.buf[j] = bb.buf[j], bb.buf[i] }

func (bb *bucketBuffer) Copy() *bucketBuffer {
	bbCopy := bucketBuffer{
		buf:  make([]kv, len(bb.buf)),
		used: bb.used,
	}
	copy(bbCopy.buf, bb.buf)
	return &bbCopy
}
