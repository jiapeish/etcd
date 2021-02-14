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

package raft

import pb "go.etcd.io/etcd/raft/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot // 该快照数据也是未写入Storage中的
	// all entries that have not yet been written to storage.
	entries []pb.Entry
	offset  uint64 // entries中的第一条Entry记录的索引值

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
// 尝试获取unstable的第一条Entry记录的索引值
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	// 如果unstable记录了快照，则通过快照元数据返回相应的索引值
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
// 尝试获取unstable的最后一条Entry记录的索引值
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true // 返回entries中最后一条Entry记录的索引值
	}
	if u.snapshot != nil { // 如果存在快照数据，则通过其元数据返回索引值
		return u.snapshot.Metadata.Index, true
	}
	return 0, false // entries和snapshot都是空的，则整个unstable也就是空的
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
// 尝试获取指定Entry记录的Term值，根据条件查找指定的Entry记录的位置
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	// 指定索引值对应的Entry记录已经不在unstable中，可能已经被持久化或者被写入快照
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i { // 检测是不是快照所包含的最后一条Entry记录
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex() // 获取unstable中最后一条Entry记录的索引
	if !ok {
		return 0, false
	}
	if i > last { // 指定的索引值超出了unstable已知范围，查找失败
		return 0, false
	}

	return u.entries[i-u.offset].Term, true // 从entries字段中查找指定的Entry并返回其Term值
}

// 当unstable.entries中的Entry记录已经被写入Storage之后，会调用unstable.stableTo方法清除
// entries中对应的Entry记录
func (u *unstable) stableTo(i, t uint64) {
	// 查找指定Entry记录的Term值，若查找失败则代表对应的Entry不在unstable中，直接返回
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	if gt == t && i >= u.offset { // 指定的Entry记录在unstable.entries中保存
		// 指定索引值之前的Entry记录都已完成持久化，则将其之前的Entry记录全部清除
		u.entries = u.entries[i+1-u.offset:]
		u.offset = i + 1 // 更新offset字段
		// 随着多次追加日志和截断日志的操作，unstable.entries底层的数组会越来越大，shrinkEntriesArray方法
		// 会在底层数组长度超过实际占用的2倍时，对底层数组进行缩减
		u.shrinkEntriesArray()
	}
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries)) // 重新创建切片
		copy(newEntries, u.entries) // 复制原有切片中数据
		u.entries = newEntries // 重置entries字段
	}
}

// 当unstable.snapshot字段指向的快照被写入Storage之后，会调用该方法将snapshot字段清空
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}

// 向unstable.entries中追加Entry记录
func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index // 获取第一条待追加的Entry记录的索引值
	switch {
	// 若待追加的记录与entries中的记录正好连续，则可以直接向entries中追加
	case after == u.offset+uint64(len(u.entries)):
		// after is the next index in the u.entries
		// directly append
		u.entries = append(u.entries, ents...)
	// 直接用待追加的Entry记录替换当前的entries字段，并更新offset
	case after <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		u.offset = after
		u.entries = ents
	// after在offset~last之间，则after~last之间的Entry记录冲突；
	// 这里会将offset~after之间的记录保留，抛弃after之后的记录，然后完成追加操作；
	// 其中unstable.slice方法会检测after是否合法，并返回offset~after的切片；
	default:
		// truncate to after and copy to u.entries
		// then append
		u.logger.Infof("truncate the unstable entries before index %d", after)
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}

func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
