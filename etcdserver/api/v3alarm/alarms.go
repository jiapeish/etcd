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

// Package v3alarm manages health status alarms in etcd.
package v3alarm

import (
	"sync"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/pkg/types"

	"github.com/coreos/pkg/capnslog"
)

var (
	alarmBucketName = []byte("alarm")
	plog            = capnslog.NewPackageLogger("go.etcd.io/etcd", "alarm")
)

type BackendGetter interface {
	Backend() backend.Backend
}

type alarmSet map[types.ID]*pb.AlarmMember

// AlarmStore persists alarms to the backend.
type AlarmStore struct {
	mu    sync.Mutex
	types map[pb.AlarmType]alarmSet

	bg BackendGetter
}

func NewAlarmStore(bg BackendGetter) (*AlarmStore, error) {
	ret := &AlarmStore{types: make(map[pb.AlarmType]alarmSet), bg: bg}
	err := ret.restore()
	return ret, err
}

// 负责新建alarm-member实例，并将其记录到alarm-store-types字段和底层存储中
func (a *AlarmStore) Activate(id types.ID, at pb.AlarmType) *pb.AlarmMember {
	a.mu.Lock()
	defer a.mu.Unlock()

	newAlarm := &pb.AlarmMember{MemberID: uint64(id), Alarm: at} // 新建实例
	if m := a.addToMap(newAlarm); m != newAlarm { // 添加到types字段
		return m
	}

	v, err := newAlarm.Marshal() // 将新实例序列化
	if err != nil {
		plog.Panicf("failed to marshal alarm member")
	}

	b := a.bg.Backend() // 获取底层的存储
	b.BatchTx().Lock()
	b.BatchTx().UnsafePut(alarmBucketName, v, nil) // 持久化到底层存储中
	b.BatchTx().Unlock()

	return newAlarm
}

// 负责从types字段和底层存储中删除指定的alarm-member实现
func (a *AlarmStore) Deactivate(id types.ID, at pb.AlarmType) *pb.AlarmMember {
	a.mu.Lock()
	defer a.mu.Unlock()

	t := a.types[at] // 根据alarm type类型查询alarm set
	if t == nil {
		t = make(alarmSet)
		a.types[at] = t
	}
	m := t[id] // 根据id查询alarm member实例
	if m == nil {
		return nil
	}

	delete(t, id) // 从types字段中删除指定的alarm member实例

	v, err := m.Marshal()
	if err != nil {
		plog.Panicf("failed to marshal alarm member")
	}

	b := a.bg.Backend()
	b.BatchTx().Lock()
	b.BatchTx().UnsafeDelete(alarmBucketName, v) // 从底层存储中删除指定的alarm member信息
	b.BatchTx().Unlock()

	return m
}

func (a *AlarmStore) Get(at pb.AlarmType) (ret []*pb.AlarmMember) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if at == pb.AlarmType_NONE { // 如果传入的参数为none，则返回该alarm-store实例中的全部alarm-member实例
		for _, t := range a.types { // 遍历types字段中全部的alarm-member返回
			for _, m := range t {
				ret = append(ret, m)
			}
		}
		return ret
	}
	for _, m := range a.types[at] { // 只返回指定类型的alarm-member实例
		ret = append(ret, m)
	}
	return ret
}

func (a *AlarmStore) restore() error {
	b := a.bg.Backend()
	tx := b.BatchTx()

	tx.Lock()
	tx.UnsafeCreateBucket(alarmBucketName)
	err := tx.UnsafeForEach(alarmBucketName, func(k, v []byte) error {
		var m pb.AlarmMember
		if err := m.Unmarshal(k); err != nil {
			return err
		}
		a.addToMap(&m)
		return nil
	})
	tx.Unlock()

	b.ForceCommit()
	return err
}

func (a *AlarmStore) addToMap(newAlarm *pb.AlarmMember) *pb.AlarmMember {
	t := a.types[newAlarm.Alarm]
	if t == nil {
		t = make(alarmSet)
		a.types[newAlarm.Alarm] = t
	}
	m := t[types.ID(newAlarm.MemberID)]
	if m != nil {
		return m
	}
	t[types.ID(newAlarm.MemberID)] = newAlarm
	return newAlarm
}
