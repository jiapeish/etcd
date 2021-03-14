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

package rafthttp

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"sync"
	"time"

	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"

	"go.uber.org/zap"
)

const (
	connPerPipeline = 4
	// pipelineBufSize is the size of pipeline buffer, which helps hold the
	// temporary network latency.
	// The size ensures that pipeline does not drop messages when the network
	// is out of work for less than 1 second in good path.
	pipelineBufSize = 64
)

var errStopped = errors.New("stopped")

type pipeline struct {
	peerID types.ID // 该pipeline对应节点的id

	tr     *Transport // 关联的rafthttp-transport实例
	picker *urlPicker
	status *peerStatus
	raft   Raft // 底层raft实例
	errorc chan error
	// deprecate when we depercate v2 API
	followerStats *stats.FollowerStats

	msgc chan raftpb.Message // pipeline实例从该通道中获取待发送的消息
	// wait for the handling routines
	// 负责同步多个goroutine结束；
	// 每个pipeline实例会启动多个后台goroutine（默认4个）来处理msgc通道中的消息；
	// 在pipeline-stop方法中必须等待这些goroutine都结束（wg-wait)，才能关闭该pipeline实例
	wg    sync.WaitGroup
	stopc chan struct{}
}

func (p *pipeline) start() {
	p.stopc = make(chan struct{})
	p.msgc = make(chan raftpb.Message, pipelineBufSize) // 缓存默认64，防止网络闪断造成消息丢失
	p.wg.Add(connPerPipeline)
	for i := 0; i < connPerPipeline; i++ {
		go p.handle() // 启动用于发送消息的goroutine
	}

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"started HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	} else {
		plog.Infof("started HTTP pipelining with peer %s", p.peerID)
	}
}

func (p *pipeline) stop() {
	close(p.stopc)
	p.wg.Wait()

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"stopped HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	} else {
		plog.Infof("stopped HTTP pipelining with peer %s", p.peerID)
	}
}

func (p *pipeline) handle() {
	defer p.wg.Done()

	for {
		select {
		case m := <-p.msgc: // 获取待发送的MsgSnap类型的消息
			start := time.Now()
			err := p.post(pbutil.MustMarshal(&m)) // 将消息序列化，然后创建http请求并发送出去
			end := time.Now()

			if err != nil {
				p.status.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())

				if m.Type == raftpb.MsgApp && p.followerStats != nil {
					p.followerStats.Fail()
				}
				p.raft.ReportUnreachable(m.To)
				if isMsgSnap(m) {
					p.raft.ReportSnapshot(m.To, raft.SnapshotFailure) // 向底层raft状态机报告失败信息
				}
				sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
				continue
			}

			p.status.activate()
			if m.Type == raftpb.MsgApp && p.followerStats != nil {
				p.followerStats.Succ(end.Sub(start))
			}
			if isMsgSnap(m) {
				p.raft.ReportSnapshot(m.To, raft.SnapshotFinish) // 向底层raft状态机报告发送成功信息
			}
			sentBytes.WithLabelValues(types.ID(m.To).String()).Add(float64(m.Size()))
		case <-p.stopc:
			return
		}
	}
}

// post POSTs a data payload to a url. Returns nil if the POST succeeds,
// error on any failure.
// 真正完成消息发送的地方，会启动一个goroutine监听对发送过程的控制，并获取发送结果
func (p *pipeline) post(data []byte) (err error) {
	u := p.picker.pick() // 获取对端暴露的url
	req := createPostRequest(u, RaftPrefix, bytes.NewBuffer(data), "application/protobuf", p.tr.URLs, p.tr.ID, p.tr.ClusterID)

	done := make(chan struct{}, 1) // 主要用于通知下边的goroutine请求是否已经发送完成
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	go func() { // 该goroutine用于监听请求是否需要取消
		select {
		case <-done:
		case <-p.stopc: // 如果在请求的发送过程中，pipeline被关闭，则取消该请求
			waitSchedule()
			cancel() // 取消请求
		}
	}()

	resp, err := p.tr.pipelineRt.RoundTrip(req) // 发送上述http post请求，并获取响应
	done <- struct{}{} // 通知上述goroutine请求已经发送完毕
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body) // 读取http-response-body内容
	if err != nil {
		p.picker.unreachable(u) // 出现异常时，将该url标记为不可用，再尝试其他url
		return err
	}

	err = checkPostResponse(resp, b, req, p.peerID) // 检测响应的内容
	if err != nil {
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
func waitSchedule() { time.Sleep(time.Millisecond) }
