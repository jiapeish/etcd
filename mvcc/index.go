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
	"sort"
	"sync"

	"github.com/google/btree"
	"go.uber.org/zap"
)

// 对google开源的b-tree实现进行了一层封装，对外提供该index接口
type index interface {
	Get(key []byte, atRev int64) (rev, created revision, ver int64, err error)
	Range(key, end []byte, atRev int64) ([][]byte, []revision)
	Revisions(key, end []byte, atRev int64) []revision
	Put(key []byte, rev revision)
	Tombstone(key []byte, rev revision) error // 添加tomb-stone
	RangeSince(key, end []byte, rev int64) []revision
	Compact(rev int64) map[revision]struct{} // 压缩b-tree中的全部key-index
	Keep(rev int64) map[revision]struct{}
	Equal(b index) bool

	Insert(ki *keyIndex)
	KeyIndex(ki *keyIndex) *keyIndex
}

type treeIndex struct {
	sync.RWMutex
	tree *btree.BTree // 通过该字段操作开源b-tree
	lg   *zap.Logger
}

func newTreeIndex(lg *zap.Logger) index {
	return &treeIndex{
		tree: btree.New(32), // b-tree的度为32，除了根节点的每个节点最少有32个元素，最多有64个元素
		lg:   lg,
	}
}

// 1、向b-tree中添加key-index实例
// 2、向key-index中追加revision信息
func (ti *treeIndex) Put(key []byte, rev revision) {
	keyi := &keyIndex{key: key} // 创建key-index实例

	ti.Lock()
	defer ti.Unlock()
	item := ti.tree.Get(keyi) // 在b-tree上查找指定的元素
	if item == nil { // 在b-tree中不存在指定的Key
		keyi.put(ti.lg, rev.main, rev.sub) // 向key-index中追加一个revision
		ti.tree.ReplaceOrInsert(keyi) // 向b-tree中添加该key-index实例
		return
	}
	okeyi := item.(*keyIndex) // 如果在b-tree中查找到该key对应的元素，则向其追加一个revision实例
	okeyi.put(ti.lg, rev.main, rev.sub)
}

// 从b-tree中查找revision信息
func (ti *treeIndex) Get(key []byte, atRev int64) (modified, created revision, ver int64, err error) {
	keyi := &keyIndex{key: key} // 创建key-index实例
	ti.RLock()
	defer ti.RUnlock()
	// 查找指定key对应的key-index实例
	if keyi = ti.keyIndex(keyi); keyi == nil {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}
	return keyi.get(ti.lg, atRev)
}

func (ti *treeIndex) KeyIndex(keyi *keyIndex) *keyIndex {
	ti.RLock()
	defer ti.RUnlock()
	return ti.keyIndex(keyi)
}

func (ti *treeIndex) keyIndex(keyi *keyIndex) *keyIndex {
	if item := ti.tree.Get(keyi); item != nil {
		return item.(*keyIndex)
	}
	return nil
}

func (ti *treeIndex) visit(key, end []byte, f func(ki *keyIndex)) {
	keyi, endi := &keyIndex{key: key}, &keyIndex{key: end} // 创建key和end对应的key-index实例

	ti.RLock()
	defer ti.RUnlock()

	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		if len(endi.key) > 0 && !item.Less(endi) { // 忽略end之后的元素
			return false
		}
		f(item.(*keyIndex))
		return true
	})
}

func (ti *treeIndex) Revisions(key, end []byte, atRev int64) (revs []revision) {
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil
		}
		return []revision{rev}
	}
	ti.visit(key, end, func(ki *keyIndex) {
		if rev, _, _, err := ki.get(ti.lg, atRev); err == nil {
			revs = append(revs, rev)
		}
	})
	return revs
}

// 范围查询，查找b-tree中key ~ end之间元素中的指定revision信息
func (ti *treeIndex) Range(key, end []byte, atRev int64) (keys [][]byte, revs []revision) {
	if end == nil {
		// 未指定end时，只查询key对应的revision信息
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil, nil
		}
		return [][]byte{key}, []revision{rev}
	}
	ti.visit(key, end, func(ki *keyIndex) {
		// 从当前元素中查询指定的revision元素，并追加到revs中
		if rev, _, _, err := ki.get(ti.lg, atRev); err == nil {
			revs = append(revs, rev)
			keys = append(keys, ki.key) // 将当前key追加到keys中
		}
	})
	return keys, revs
}

// 先在b-tree中查找指定的key-index实例，然后调用key-index的tomb-stone方法结束其当前generation实例并创建新的generation实例；
func (ti *treeIndex) Tombstone(key []byte, rev revision) error {
	keyi := &keyIndex{key: key}

	ti.Lock()
	defer ti.Unlock()
	item := ti.tree.Get(keyi)
	if item == nil {
		return ErrRevisionNotFound
	}

	ki := item.(*keyIndex)
	return ki.tombstone(ti.lg, rev.main, rev.sub)
}

// RangeSince returns all revisions from key(including) to end(excluding)
// at or after the given rev. The returned slice is sorted in the order
// of revision.
// 查询key~end的元素，并从中查询main revision部分大于指定值的revision信息；
// 最终返回的revision实例不是按照key进行排序的，而是按照greater-than进行排序；
func (ti *treeIndex) RangeSince(key, end []byte, rev int64) []revision {
	keyi := &keyIndex{key: key}

	ti.RLock()
	defer ti.RUnlock()

	if end == nil { // 未指定end时，只查询key对应的revision信息
		item := ti.tree.Get(keyi)
		if item == nil {
			return nil
		}
		keyi = item.(*keyIndex)
		return keyi.since(ti.lg, rev) // 获取rev之后的revision实例
	}

	endi := &keyIndex{key: end}
	var revs []revision
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		if len(endi.key) > 0 && !item.Less(endi) { // 忽略大于end的元素
			return false
		}
		curKeyi := item.(*keyIndex)
		revs = append(revs, curKeyi.since(ti.lg, rev)...) // 用since方法获取当前key-index实例中rev之后的revision实例
		return true
	})
	sort.Sort(revisions(revs)) // 对revs中记录的revision实例进行排序，并返回

	return revs
}

// 遍历b-tree中所有的key-index实例，并对其中的generations进行压缩
func (ti *treeIndex) Compact(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	if ti.lg != nil {
		ti.lg.Info("compact tree index", zap.Int64("revision", rev))
	} else {
		plog.Printf("store.index: compact %d", rev)
	}
	ti.Lock()
	clone := ti.tree.Clone()
	ti.Unlock()

	clone.Ascend(func(item btree.Item) bool {
		keyi := item.(*keyIndex)
		//Lock is needed here to prevent modification to the keyIndex while
		//compaction is going on or revision added to empty before deletion
		ti.Lock()
		keyi.compact(ti.lg, rev, available)
		if keyi.isEmpty() { // 检测key-index实例中的generations是否已经为空
			item := ti.tree.Delete(keyi)
			if item == nil {
				if ti.lg != nil {
					ti.lg.Panic("failed to delete during compaction")
				} else {
					plog.Panic("store.index: unexpected delete failure during compaction")
				}
			}
		}
		ti.Unlock()
		return true
	})
	return available
}

// Keep finds all revisions to be kept for a Compaction at the given rev.
func (ti *treeIndex) Keep(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	ti.RLock()
	defer ti.RUnlock()
	ti.tree.Ascend(func(i btree.Item) bool {
		keyi := i.(*keyIndex)
		keyi.keep(rev, available)
		return true
	})
	return available
}

func (ti *treeIndex) Equal(bi index) bool {
	b := bi.(*treeIndex)

	if ti.tree.Len() != b.tree.Len() {
		return false
	}

	equal := true

	ti.tree.Ascend(func(item btree.Item) bool {
		aki := item.(*keyIndex)
		bki := b.tree.Get(item).(*keyIndex)
		if !aki.equal(bki) {
			equal = false
			return false
		}
		return true
	})

	return equal
}

func (ti *treeIndex) Insert(ki *keyIndex) {
	ti.Lock()
	defer ti.Unlock()
	ti.tree.ReplaceOrInsert(ki)
}
