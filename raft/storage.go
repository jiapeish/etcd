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

import (
	"errors"
	"sync"

	pb "go.etcd.io/etcd/raft/raftpb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
type Storage interface {
	// TODO(tbg): split this into two interfaces, LogStorage and StateStorage.

	// InitialState returns the saved HardState and ConfState information.
	InitialState() (pb.HardState, pb.ConfState, error)
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	// 该方法返回Storage接口中记录的第一条Entry的索引值(Index)，
	// 在该Entry之前的所有Entry都已经被包含进了最近的一次Snapshot中
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	Snapshot() (pb.Snapshot, error)
}

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
// MemoryStorage的大部分操作是需要加锁同步的；
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents []pb.Entry
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents: make([]pb.Entry, 1),
	}
}

// InitialState implements the Storage interface.
// 该方法直接返回hardState字段中记录的HardState实例，并使用快照的元数据中记录的信息创建ConfState实例返回；
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if lo <= offset { // 如果待查询的最小Index值（lo）小于FirstIndex，则返回异常
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 { // 如果待查询的最大Index值（hi）大于LastIndex，则返回异常
		raftLogger.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 { // 如果MemoryStorage.ents只包含一条Entry，则其为空Entry，则返回异常
		return nil, ErrUnavailable
	}

	ents := ms.ents[lo-offset : hi-offset] // 获取lo~hi之间的Entry
	return limitSize(ents, maxSize), nil // 限制返回的Entry切片的总字节大小
}

// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
// 返回ents数组中最后一个元素的Index字段值
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
// 返回ents数组中第一个元素的Index字段值
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
// 当MemoryStorage需要更新快照数据时，会调用ApplySnapshot方法将Snapshot实例保存到MemoryStorage中；
// 例如，在节点重启时，就会通过读取快照文件创建对应的Snapshot实例，然后保存到MemoryStorage中；
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	//handle check for old snapshot being applied
	// 通过快照的元数据比较当前MemoryStorage中记录的Snapshot与待处理的Snapshot数据的新旧程度
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex { // 比较两个pb.Snapshot所包含的最后一条记录的Index值
		return ErrSnapOutOfDate // 如果待处理的Snapshot数据比较旧，则直接返回异常；
	}

	ms.snapshot = snap // 更新MemoryStorage.Snapshot字段
	// 重置MemoryStorage.Snapshot字段，此时在ents中只有一个空的Entry实例
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
// 接收当前集群状态，以及Snapshot相关数据来更新snapshot字段
// 第一个参数i，是新建snapshot包含的最大的索引值
// 第二个参数cs，是当前集群的状态
// 第三个参数data，是新建snapshot的具体数据
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	// 边界检查，i 必须大于当前snapshot包含的最大Index值
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	// i 必须小于MemoryStorage的LastIndex值，否则异常
	if i > ms.lastIndex() {
		raftLogger.Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	// 更新MemoryStorage.snapshot的元数据
	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data // 更新具体的快照数据
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
// 新建snapshot之后，一般会调用该方法将MemoryStorage.ents中指定索引之前的Entry记录全部抛弃，
// 从而实现压缩MemoryStorage.ents的目的；
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	// 边界检测，firstIndex < compactIndex < lastIndex
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		raftLogger.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset // compactIndex对应的Entry下标
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i) // 创建新的切片，用来存储compactIndex之后的Entry
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	// 将compactIndex之后的Entry拷贝到ents中，并更新MemoryStorage.ents字段
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
// 设置完快照数据后，就可以向MemoryStorage中追加Entry记录了
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex() // 获取当前MemoryStorage的FirstIndex值
	last := entries[0].Index + uint64(len(entries)) - 1 // 获取待添加的最后一条Entry的Index值

	// shortcut if there is no new entry.
	if last < first { // entries切片中所有的Entry都已过时，无需添加
		return nil
	}
	// truncate compacted entries
	// first之前的Entry已经计入了Snapshot中，不应再记录到ents中，所以这部分Entry裁掉；
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	// 计算entries切片中第一条可用的Entry与first直接的差距
	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		// 保留MemoryStorage.ents中first~offset之间的部分，抛弃offset之后的部分
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		// 然后将待追加的Entry追加到MemoryStorage.ents中
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		// 直接将待追加的日志记录entries追加到MemoryStorage.ents中
		ms.ents = append(ms.ents, entries...)
	default:
		raftLogger.Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
