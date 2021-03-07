// Copyright 2016 The etcd Authors
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

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

type readIndexStatus struct {
	req   pb.Message // 记录了对应的MsgReadIndex请求
	index uint64 // 表示该MsgReadIndex请求到达时，对应的已提交位置（即raftLog-committed)；
	// NB: this never records 'false', but it's more convenient to use this
	// instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
	// this becomes performance sensitive enough (doubtful), quorum.VoteResult
	// can change to an API that is closer to that of CommittedIndex.
	acks map[uint64]bool // 记录了该MsgReadIndex相关的MsgHeartbeatResp响应消息
}

type readOnly struct {
	option           ReadOnlyOption // 当前只读请求的处理模式：ReadOnlySafe和ReadOnlyLeaseBased
	// 在etcd服务端收到MsgReadIndex消息时，会为其创建一个消息id（唯一的）， 并作为MsgReadIndex消息的第一条entry记录，
	// 在pendingReadIndex中记录了消息id与对应请求readIndexStatus实例的映射
	pendingReadIndex map[string]*readIndexStatus
	readIndexQueue   []string // 记录了MsgReadIndex请求对应的消息id
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}

// addRequest adds a read only reuqest into readonly struct.
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `m` is the original read only request message from the local or remote node.
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	s := string(m.Entries[0].Data) // 在readIndex消息的第一个记录中，记录了消息id
	if _, ok := ro.pendingReadIndex[s]; ok { // 检测是否存在相同id的MsgReadIndex，存在则不再记录该请求
		return
	}
	ro.pendingReadIndex[s] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]bool)} // 创建实例并记录
	ro.readIndexQueue = append(ro.readIndexQueue, s) // 记录消息id
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
// 该方法统计指定的消息id收到了多少MsgHeartbeatResp响应消息，从而确定在收到对应的MsgReadIndex消息的时候，
// 当前leader节点是否仍为集群的leader
func (ro *readOnly) recvAck(id uint64, context []byte) map[uint64]bool {
	// 获取消息id对应的readIndexStatus实例
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return nil
	}

	rs.acks[id] = true // 表示MsgHeartbeatResp消息的发送节点与当前节点联通
	return rs.acks
}

// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	ctx := string(m.Context) // MsgHeartbeat消息对应的MsgReadIndex消息id
	rss := []*readIndexStatus{}

	for _, okctx := range ro.readIndexQueue { // 遍历readOnly中记录的消息id
		i++
		rs, ok := ro.pendingReadIndex[okctx] // 查找消息id对应的readIndexStatus实例
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		rss = append(rss, rs) // 将readIndexStatus实例保存到rss数组中
		if okctx == ctx { // 查找到指定的MsgReadIndex消息id
			found = true
			break
		}
	}

	if found { // 查找到指定的MsgReadIndex消息id，则清空readOnly中所有在他之前的消息id及相关内容
		ro.readIndexQueue = ro.readIndexQueue[i:] // 清理
		for _, rs := range rss {
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data)) // 清理
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
