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

package fileutil

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
)

func PurgeFile(lg *zap.Logger, dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}) <-chan error {
	return purgeFile(lg, dirname, suffix, max, interval, stop, nil, nil)
}

func PurgeFileWithDoneNotify(lg *zap.Logger, dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}) (<-chan struct{}, <-chan error) {
	doneC := make(chan struct{})
	errC := purgeFile(lg, dirname, suffix, max, interval, stop, nil, doneC)
	return doneC, errC
}

// purgeFile is the internal implementation for PurgeFile which can post purged files to purgec if non-nil.
// if donec is non-nil, the function closes it to notify its exit.
// 定期查询指定的目录，并统计指定后缀的文件个数，如果文件个数超过指定的上限，则进行清理操作；
func purgeFile(lg *zap.Logger, dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}, purgec chan<- string, donec chan<- struct{}) <-chan error {
	errC := make(chan error, 1)
	go func() {
		if donec != nil {
			defer close(donec)
		}
		for {
			fnames, err := ReadDir(dirname) // 查找目标目录下的子文件名称，其中还会根据文件名称进行排序
			if err != nil {
				errC <- err
				return
			}
			newfnames := make([]string, 0)
			for _, fname := range fnames { // 根据指定的后缀对文件名进行过滤
				if strings.HasSuffix(fname, suffix) {
					newfnames = append(newfnames, fname)
				}
			}
			sort.Strings(newfnames)
			fnames = newfnames
			for len(newfnames) > int(max) { // 指定后缀的文件个数是否超过上限
				f := filepath.Join(dirname, newfnames[0])
				l, err := TryLockFile(f, os.O_WRONLY, PrivateFileMode) // 获取文件锁
				if err != nil {
					break
				}
				if err = os.Remove(f); err != nil { // 删除并进行错误处理
					errC <- err
					return
				}
				if err = l.Close(); err != nil { // 关闭对应的文件句柄，并进行错误处理
					if lg != nil {
						lg.Warn("failed to unlock/close", zap.String("path", l.Name()), zap.Error(err))
					} else {
						plog.Errorf("error unlocking %s when purging file (%v)", l.Name(), err)
					}
					errC <- err
					return
				}
				if lg != nil {
					lg.Info("purged", zap.String("path", f))
				} else {
					plog.Infof("purged file %s successfully", f)
				}
				newfnames = newfnames[1:] // 清除指定的文件
			}
			if purgec != nil {
				for i := 0; i < len(fnames)-len(newfnames); i++ {
					purgec <- fnames[i]
				}
			}
			select { // 阻塞指定的时间间隔
			case <-time.After(interval):
			case <-stop: // 当前节点正在关闭，则退出该goroutine
				return
			}
		}
	}()
	return errC
}
