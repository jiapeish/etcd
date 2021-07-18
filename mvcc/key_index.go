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
	"bytes"
	"errors"
	"fmt"

	"github.com/google/btree"
	"go.uber.org/zap"
)

var (
	ErrRevisionNotFound = errors.New("mvcc: revision not found")
)

//                                BTree Index
//                        ┌───────────────────────────────────┐
//                        │                                   │                                     ┌─────────────┐
//                        │          ┌──────────┐             │                                     │┼───────────┼│
//                        │          ├─────┼────┴──────┐      │                                     ││           ││
//                        │          │     │           │      │                                     ││           ││
//                        │          │     │           │      │          ┌───────────────┐          ││           ││        ┌──────────────┐
// ┌───────────────┐      │   ┌──────▼┐  ┌─▼────┐    ┌─▼────┐ │          │               │          ││           ││        │              │
// │               │      │   └───────┘  ├──────┤    └──────┘ │          │               │          ││           ││        │              │
// │ Origin Key    ├─────►│              │      │             ├─────────►│ keyIndex      ├──────────►│ BoltDB    │┼───────►│ Final Value  │
// │               │      │         ┌────▼┐   ┌─▼───┐         │          │               │          ││           ││        │              │
// └───────────────┘      │         └─────┘   └─────┘         │          └───────────────┘          ││           ││        └──────────────┘
//                        │                                   │                                     ││           ││
//                        │                                   │                                     ││           ││
//                        └───────────────────────────────────┘                                     │┼───────────┼│
//                                                                                                  └─────────────┘


// ┌──────────────────────keyIndex───────────────────────────────┐
// │                                                             │
// │                                                             │
// │      ┌───────────────────────────────────────────┐          │
// │      │                                           │          │
// │      │           key  :  fooKey                  │          │
// │      │                                           │          │
// │      └───────────────────────────────────────────┘          │
// │                                                             │
// │                                                             │
// │      ┌───────────────────────────────────────────┐          │
// │      │                                           │          │
// │      │          modified : 4.0                   │          │
// │      │                                           │          │
// │      └───────────────────────────────────────────┘          │
// │                                                             │
// │                                                             │
// │   ┌──────────────────generations───────────────────────┐    │
// │   │                                                    │    │
// │   │                                                    │    │
// │   │  ┌─────┐        ┌───────┬───────┬──────┬─────┐     │    │
// │   │  │     │        │       │       │      │     │     │    │
// │   │  │ [0] ├───────►│ 1.0   │ 2.0   │ 3.0  │ 4.0 │     │    │
// │   │  │     │        │       │       │      │     │     │    │
// │   │  └─────┘        └───────┴───────┴──────┴─────┘     │    │
// │   │                                                    │    │
// │   └────────────────────────────────────────────────────┘    │
// │                                                             │
// └─────────────────────────────────────────────────────────────┘


// ┌──────────────────────keyIndex───────────────────────────────┐
// │                                                             │
// │        ┌───────────────────────────────────────────┐        │
// │        │                                           │        │
// │        │           key  :  fooKey                  │        │
// │        │                                           │        │
// │        └───────────────────────────────────────────┘        │
// │                                                             │
// │                                                             │
// │        ┌───────────────────────────────────────────┐        │
// │        │                                           │        │
// │        │          modified : 6.0                   │        │
// │        │                                           │        │
// │        └───────────────────────────────────────────┘        │
// │                                                             │
// │   ┌──────────────────generations─────────────────────────┐  │
// │   │                                                      │  │
// │   │                                                      │  │
// │   │  ┌─────┐        ┌───────┬───────┬──────┬───────────┐ │  │
// │   │  │     │        │       │       │      │           │ │  │
// │   │  │ [0] ├───────►│ 1.0   │ 2.0   │ 3.0  │ Tombstone │ │  │
// │   │  │     │        │       │       │      │           │ │  │
// │   │  └─────┘        └───────┴───────┴──────┴───────────┘ │  │
// │   │                                                      │  │
// │   │  ┌─────┐         ┌─────┬─────┬───────────┐           │  │
// │   │  │     │         │     │     │           │           │  │
// │   │  │ [1] │──-─────►│ 4.0 │ 5.0 │ Tombstone │           │  │
// │   │  └─────┘         └─────┴─────┴───────────┘           │  │
// │   │                                                      │  │
// │   │  ┌─────┐         ┌─────┬───────────┐                 │  │
// │   │  │     │         │     │           │                 │  │
// │   │  │ [2] │─────-──►│ 6.0 │ Tombstone │                 │  │
// │   │  └─────┘         └─────┴───────────┘                 │  │
// │   │                                                      │  │
// │   └──────────────────────────────────────────────────────┘  │
// │                                                             │
// └─────────────────────────────────────────────────────────────┘


// ┌──────────────────────generations────────────────────────────┐
// │                                                             │
// │     ┌─────┐        ┌───────┬───────┬──────┬───────────┐     │
// │     │     │        │       │       │      │           │     │
// │     │ [0] ├───────►│ 1.0   │ 2.0   │ 3.0  │ Tombstone │     │
// │     │     │        │       │       │      │           │     │
// │     └─────┘        └───────┴───────┴──────┴───────────┘     │
// │                                                             │
// │     ┌─────┐         ┌─────┬─────┬───────────┐               │
// │     │     │         │     │     │           │               │
// │     │ [1] ├─────────► 8.0 │ 9.0 │ Tombstone │               │
// │     └─────┘         └─────┴─────┴───────────┘               │
// │                                                             │
// └─────────────────────────────────────────────────────────────┘




// keyIndex stores the revisions of a key in the backend.
// Each keyIndex has at least one key generation.
// Each generation might have several key versions.
// Tombstone on a key appends an tombstone version at the end
// of the current generation and creates a new empty generation.
// Each version of a key has an index pointing to the backend.
//
// For example: put(1.0);put(2.0);tombstone(3.0);put(4.0);tombstone(5.0) on key "foo"
// generate a keyIndex:
// key:     "foo"
// rev: 5
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {1.0, 2.0, 3.0(t)}
//
// Compact a keyIndex removes the versions with smaller or equal to
// rev except the largest one. If the generation becomes empty
// during compaction, it will be removed. if all the generations get
// removed, the keyIndex should be removed.
//
// For example:
// compact(2) on the previous example
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {2.0, 3.0(t)}
//
// compact(4)
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//
// compact(5):
// generations:
//    {empty} -> key SHOULD be removed.
//
// compact(6):
// generations:
//    {empty} -> key SHOULD be removed.
type keyIndex struct {
	key         []byte // 客户端提供的原始key值
	modified    revision // the main rev of the last modification
	generations []generation
}

// put puts a revision to the keyIndex.
func (ki *keyIndex) put(lg *zap.Logger, main int64, sub int64) {
	rev := revision{main: main, sub: sub}

	if !rev.GreaterThan(ki.modified) {
		if lg != nil {
			lg.Panic(
				"'put' with an unexpected smaller revision",
				zap.Int64("given-revision-main", rev.main),
				zap.Int64("given-revision-sub", rev.sub),
				zap.Int64("modified-revision-main", ki.modified.main),
				zap.Int64("modified-revision-sub", ki.modified.sub),
			)
		} else {
			plog.Panicf("store.keyindex: put with unexpected smaller revision [%v / %v]", rev, ki.modified)
		}
	}
	if len(ki.generations) == 0 { // 创建generations[0]实例
		ki.generations = append(ki.generations, generation{})
	}
	g := &ki.generations[len(ki.generations)-1]
	if len(g.revs) == 0 { // create a new key
		keysGauge.Inc()
		g.created = rev // 新建key,则初始化对应generation实例的created字段
	}
	g.revs = append(g.revs, rev) // 追加revision信息
	g.ver++ // 递增
	ki.modified = rev // 更新该字段
}


// 恢复当前key-index中的信息
func (ki *keyIndex) restore(lg *zap.Logger, created, modified revision, ver int64) {
	if len(ki.generations) != 0 {
		if lg != nil {
			lg.Panic(
				"'restore' got an unexpected non-empty generations",
				zap.Int("generations-size", len(ki.generations)),
			)
		} else {
			plog.Panicf("store.keyindex: cannot restore non-empty keyIndex")
		}
	}

	ki.modified = modified
	g := generation{created: created, ver: ver, revs: []revision{modified}} // 创建generation实例
	ki.generations = append(ki.generations, g) // 添加到generations中
	keysGauge.Inc()
}

// tombstone puts a revision, pointing to a tombstone, to the keyIndex.
// It also creates a new empty generation in the keyIndex.
// It returns ErrRevisionNotFound when tombstone on an empty generation.
// 在当前generation中追加一个revision实例，然后新建一个generation实例
func (ki *keyIndex) tombstone(lg *zap.Logger, main int64, sub int64) error {
	if ki.isEmpty() {
		if lg != nil {
			lg.Panic(
				"'tombstone' got an unexpected empty keyIndex",
				zap.String("key", string(ki.key)),
			)
		} else {
			plog.Panicf("store.keyindex: unexpected tombstone on empty keyIndex %s", string(ki.key))
		}
	}
	if ki.generations[len(ki.generations)-1].isEmpty() {
		return ErrRevisionNotFound
	}
	ki.put(lg, main, sub) // 在当前generation中追加一个revision
	ki.generations = append(ki.generations, generation{}) // 在generations中创建新的generation实例
	keysGauge.Dec()
	return nil
}

// get gets the modified, created revision and version of the key that satisfies the given atRev.
// Rev must be higher than or equal to the given atRev.
// 在当前key-index实例中查找小于指定的main revision的最大revision
func (ki *keyIndex) get(lg *zap.Logger, atRev int64) (modified, created revision, ver int64, err error) {
	if ki.isEmpty() {
		if lg != nil {
			lg.Panic(
				"'get' got an unexpected empty keyIndex",
				zap.String("key", string(ki.key)),
			)
		} else {
			plog.Panicf("store.keyindex: unexpected get on empty keyIndex %s", string(ki.key))
		}
	}
	g := ki.findGeneration(atRev) // 根据给定的main revison，查找对应的generation实例
	if g.isEmpty() {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}

	n := g.walk(func(rev revision) bool { return rev.main > atRev }) // 在generation中查找对应的revision实例
	if n != -1 {
		// 返回目标revision实例、该key此次创建的revision实例、到目标revision之前的修改次数
		return g.revs[n], g.created, g.ver - int64(len(g.revs)-n-1), nil
	}

	return revision{}, revision{}, 0, ErrRevisionNotFound
}

// since returns revisions since the given rev. Only the revision with the
// largest sub revision will be returned if multiple revisions have the same
// main revision.
// 批量查找revision，返回当前key-index实例中main部分大于指定值的revision实例，
// 如果查询结果中包含了多个main部分相同的revision实例，则只返回其中sub部分最大的实例
func (ki *keyIndex) since(lg *zap.Logger, rev int64) []revision {
	if ki.isEmpty() {
		if lg != nil {
			lg.Panic(
				"'since' got an unexpected empty keyIndex",
				zap.String("key", string(ki.key)),
			)
		} else {
			plog.Panicf("store.keyindex: unexpected get on empty keyIndex %s", string(ki.key))
		}
	}
	since := revision{rev, 0}
	var gi int
	// find the generations to start checking
	// 逆序遍历所有generation实例，查询从哪个generation开始查找（即gi对应的generation实例）
	for gi = len(ki.generations) - 1; gi > 0; gi-- {
		g := ki.generations[gi]
		if g.isEmpty() {
			continue
		}
		// 比较创建当前generation实例的revision与since
		// 在greater-than方法中会先比较main revision部分，如果相同，则比较sub revision部分；
		if since.GreaterThan(g.created) {
			break
		}
	}

	var revs []revision // 记录返回结果
	var last int64 // 用于记录当前所遇到的最大的main revision值
	for ; gi < len(ki.generations); gi++ { // 从gi处开始遍历generation实例
		for _, r := range ki.generations[gi].revs { // 遍历每个generation中记录的revision实例
			if since.GreaterThan(r) { // 忽略main revision部分较小的revision实例
				continue
			}
			// 如果查找到main revision部分相同，且sub revision更大的revision实例，
			// 则用其替换之前记录的返回结果，从而实现main部分相同时只返回sub部分较大的revision实例
			if r.main == last {
				// replace the revision with a new one that has higher sub value,
				// because the original one should not be seen by external
				revs[len(revs)-1] = r
				continue
			}
			revs = append(revs, r) // 将符合条件的revision实例记录到revs中
			last = r.main // 更新last字段
		}
	}
	return revs
}

// 随着客户端不断修改键值对，key-index中记录的revision实例和generation实例会不断增加，可以通过compact方法对key-index进行压缩；
// 在压缩时会将main部分小于指定值的revision实例全部删除；
// 在压缩过程中，如果出现了空的generation实例，则会将其删除；
// 如果key-index中全部的generation实例都被清除了，则该key-index实例也会被删除；
// compact-n表示压缩掉revision-main <= n的所有版本；
//
//┌──────generations───────────────┐          ┌──────generations─────────────┐          ┌───────generations──────────────┐
//│                                │          │                              │          │                                │
//│ ┌───┐  ┌────┬───┬─────────┐    │          │ ┌───┐  ┌────┬────┬─────────┐ │          │  ┌────┐                        │
//│ │   │  │    │   │         │    │          │ │   │  │    │    │         │ │          │  │    │                        │
//│ │[0]├──►2.0 │3.0│Tombstone│    │          │ │[0]├──►4.0 │5.0 │Tombstone│ │          │  │[0] ├───►empty               │
//│ │   │  │    │   │         │    │          │ │   │  │    │    │         │ │          │  │    │                        │
//│ └───┘  └────┴───┴─────────┘    │          │ └───┘  └────┴────┴─────────┘ │          │  │    │                        │
//│                                │          │                              │          │  └────┘                        │
//│ ┌───┐   ┌────┬────┬─────────┐  │          │ ┌────┐                       ├──────────►                                │
//│ │   │   │    │    │         │  ├─────────►│ │    │                       │ compact-5│                                │
//│ │[1]├──►│4.0 │5.0 │Tombstone│  │compact-4 │ │[1] ├──►empty               │          │                                │
//│ │   │   │    │    │         │  │          │ │    │                       │          │                                │
//│ └───┘   └────┴────┴─────────┘  │          │ └────┘                       │          │                                │
//│                                │          │                              │          │                                │
//│  ┌───┐                         │          │                              │          │                                │
//│  │   ├──►empty                 │          │                              │          │                                │
//│  │[2]│                         │          └──────────────────────────────┘          └────────────────────────────────┘
//│  └───┘                         │
//│                                │
//└────────────────────────────────┘
//

// compact compacts a keyIndex by removing the versions with smaller or equal
// revision than the given atRev except the largest one (If the largest one is
// a tombstone, it will not be kept).
// If a generation becomes empty during compaction, it will be removed.
func (ki *keyIndex) compact(lg *zap.Logger, atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		if lg != nil {
			lg.Panic(
				"'compact' got an unexpected empty keyIndex",
				zap.String("key", string(ki.key)),
			)
		} else {
			plog.Panicf("store.keyindex: unexpected compact on empty keyIndex %s", string(ki.key))
		}
	}

	genIdx, revIndex := ki.doCompact(atRev, available)

	g := &ki.generations[genIdx]
	// 遍历目标generation实例中的全部revision实例，清空目标generation实例中main部分小于指定值的revision
	if !g.isEmpty() {
		// remove the previous contents.
		if revIndex != -1 {
			g.revs = g.revs[revIndex:] // 清理目标generation
		}
		// remove any tombstone
		// 如果目标generation实例中只有tomb-stone，则将其删除
		if len(g.revs) == 1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[0])
			genIdx++ // 递增，后边会清理该idx之前的全部generation实例
		}
	}

	// remove the previous generations.
	// 清理目标generation实例之前的全部generation实例
	ki.generations = ki.generations[genIdx:]
}

// keep finds the revision to be kept if compact is called at given atRev.
func (ki *keyIndex) keep(atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		return
	}

	genIdx, revIndex := ki.doCompact(atRev, available)
	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove any tombstone
		if revIndex == len(g.revs)-1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[revIndex])
		}
	}
}

func (ki *keyIndex) doCompact(atRev int64, available map[revision]struct{}) (genIdx int, revIndex int) {
	// walk until reaching the first revision smaller or equal to "atRev",
	// and add the revision to the available map
	f := func(rev revision) bool { // 遍历generation时使用的回调函数
		if rev.main <= atRev {
			available[rev] = struct{}{}
			return false
		}
		return true
	}

	genIdx, g := 0, &ki.generations[0]
	// find first generation includes atRev or created after atRev
	// 遍历所有的generation实例，目标generation实例中的tomb-stone大于指定的revision
	for genIdx < len(ki.generations)-1 {
		if tomb := g.revs[len(g.revs)-1].main; tomb > atRev {
			break
		}
		genIdx++
		g = &ki.generations[genIdx]
	}

	revIndex = g.walk(f)

	return genIdx, revIndex
}

func (ki *keyIndex) isEmpty() bool {
	return len(ki.generations) == 1 && ki.generations[0].isEmpty()
}

//┌────────────generations───────────────────────────┐
//│                                                  │
//│    ┌───┐    ┌─────┬─────┬────┬─────────────┐     │
//│    │   │    │     │     │    │             │     │
//│    │[0]├───►│ 1.0 │ 2.0 │3.0 │ Tombstone   │     │
//│    └───┘    └─────┴─────┴────┴─────────────┘     │
//│                                                  │
//│                                                  │
//│    ┌───┐     ┌─────┬────┬───────────┐            │
//│    │   │     │     │    │           │            │
//│    │[1]├────►│ 8.0 │9.0 │ Tombstone │            │
//│    └───┘     └─────┴────┴───────────┘            │
//│                                                  │
//└──────────────────────────────────────────────────┘
// 有时find-generation方法无法查到指定main revision所在的generation实例，
// 假设当前的main revision是5，当前key-index-generations的状态如上图所示，
// 因为main revision在4～7这段时间内，该key被删除了，所以无法查找到其所在的generation实例，这时会返回nil

// findGeneration finds out the generation of the keyIndex that the
// given rev belongs to. If the given rev is at the gap of two generations,
// which means that the key does not exist at the given rev, it returns nil.
// 查找指定main revision所在的generation实例
func (ki *keyIndex) findGeneration(rev int64) *generation {
	lastg := len(ki.generations) - 1
	cg := lastg // 指向当前key-index实例中最后一个generation实例，并逐个向前查找

	for cg >= 0 {
		if len(ki.generations[cg].revs) == 0 { // 过滤掉空的generation实例
			cg--
			continue
		}
		g := ki.generations[cg]
		if cg != lastg { // 如果不是最后一个generation实例，则先与tomb-stone revision进行比较
			if tomb := g.revs[len(g.revs)-1].main; tomb <= rev {
				return nil
			}
		}
		if g.revs[0].main <= rev { // 与generation中第一个revision比较
			return &ki.generations[cg]
		}
		cg--
	}
	return nil
}

func (ki *keyIndex) Less(b btree.Item) bool {
	return bytes.Compare(ki.key, b.(*keyIndex).key) == -1
}

func (ki *keyIndex) equal(b *keyIndex) bool {
	if !bytes.Equal(ki.key, b.key) {
		return false
	}
	if ki.modified != b.modified {
		return false
	}
	if len(ki.generations) != len(b.generations) {
		return false
	}
	for i := range ki.generations {
		ag, bg := ki.generations[i], b.generations[i]
		if !ag.equal(bg) {
			return false
		}
	}
	return true
}

func (ki *keyIndex) String() string {
	var s string
	for _, g := range ki.generations {
		s += g.String()
	}
	return s
}

// generation contains multiple revisions of a key.
// 当第一次创建客户端指定的key时，对应的generations[0]会被创建，表示第0代版本信息；
// 所以每个key值至少对应一个generation实例，如果没有，则表示当前key值应该被删除；
// 每代中包含多个revision信息，当客户端后续不断修改该key时，generations[0]中会不断追加revision信息；
// 当向generation实例中追加一个tomb-stone时，表示删除当前key，此时就会结束当前的generation实例，
// 后续不再向该generation实例中追加revision信息，同时会创建新的generation实例；
// 可以认为generation对应了当前key的一次从创建到删除的生命周期；
type generation struct {
	ver     int64 // 记录当前generation所包含的修改次数，即revs数组的长度
	created revision // when the generation is created (put in first revision).
	revs    []revision // 当客户端不断更新该键值对时，该数组会不断追加每次更新对应的revision信息
}

func (g *generation) isEmpty() bool { return g == nil || len(g.revs) == 0 }

// walk walks through the revisions in the generation in descending order.
// It passes the revision to the given function.
// walk returns until: 1. it finishes walking all pairs 2. the function returns false.
// walk returns the position at where it stopped. If it stopped after
// finishing walking, -1 will be returned.
// 从generation实例中查找符合条件的revision实例
func (g *generation) walk(f func(rev revision) bool) int {
	l := len(g.revs)
	for i := range g.revs {
		ok := f(g.revs[l-i-1]) // 逆序查找generation-revs数组
		if !ok {
			return l - i - 1 // 返回目标revision的下标
		}
	}
	return -1
}

func (g *generation) String() string {
	return fmt.Sprintf("g: created[%d] ver[%d], revs %#v\n", g.created, g.ver, g.revs)
}

func (g generation) equal(b generation) bool {
	if g.ver != b.ver {
		return false
	}
	if len(g.revs) != len(b.revs) {
		return false
	}

	for i := range g.revs {
		ar, br := g.revs[i], b.revs[i]
		if ar != br {
			return false
		}
	}
	return true
}
