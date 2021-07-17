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
	"encoding/binary"
	"time"

	"go.uber.org/zap"
)

// 完成bolt-db中存储的键值对的压缩
func (s *store) scheduleCompaction(compactMainRev int64, keep map[revision]struct{}) bool {
	totalStart := time.Now()
	defer func() { dbCompactionTotalMs.Observe(float64(time.Since(totalStart) / time.Millisecond)) }()
	keyCompactions := 0
	defer func() { dbCompactionKeysCounter.Add(float64(keyCompactions)) }()

	end := make([]byte, 8)
	// 将compact-main-rev + 1写入，其中compact方法已经更新过compact-main-rev,
	// 作为范围查询的结束key(revision)
	binary.BigEndian.PutUint64(end, uint64(compactMainRev+1))

	last := make([]byte, 8+1+8) // 范围查询的起始key(revision)
	for {
		var rev revision

		start := time.Now()

		tx := s.b.BatchTx() // 获取读写事务
		tx.Lock()
		keys, _ := tx.UnsafeRange(keyBucketName, last, end, int64(s.cfg.CompactionBatchLimit)) // 进行范围查询
		for _, key := range keys { // 遍历上述查询到的key(revision)，并逐个进行删除
			rev = bytesToRev(key)
			if _, ok := keep[rev]; !ok {
				tx.UnsafeDelete(keyBucketName, key)
				keyCompactions++
			}
		}

		if len(keys) < s.cfg.CompactionBatchLimit { // 最后一次批量处理
			rbytes := make([]byte, 8+1+8)
			revToBytes(revision{main: compactMainRev}, rbytes) // 将compact-main-rev封装成revision，并写入meta bucket
			tx.UnsafePut(metaBucketName, finishedCompactKeyName, rbytes)
			tx.Unlock() // 读写事务使用完毕，解锁
			if s.lg != nil {
				s.lg.Info(
					"finished scheduled compaction",
					zap.Int64("compact-revision", compactMainRev),
					zap.Duration("took", time.Since(totalStart)),
				)
			} else {
				plog.Infof("finished scheduled compaction at %d (took %v)", compactMainRev, time.Since(totalStart))
			}
			return true
		}

		// update last
		// 更新last, 下次范围查询的起始key依然是last
		revToBytes(revision{main: rev.main, sub: rev.sub + 1}, last)
		tx.Unlock()
		// Immediately commit the compaction deletes instead of letting them accumulate in the write buffer
		s.b.ForceCommit()
		dbCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))

		select { // 每次范围处理结束之后
		case <-time.After(10 * time.Millisecond):
		case <-s.stopc:
			return false
		}
	}
}
