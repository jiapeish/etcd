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

package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/pkg/fileutil"

	"go.uber.org/zap"
)

// filePipeline pipelines allocating disk space
// 负责预创建日志文件并为日志文件预分配空间；在file-pipeline中会启动一个独立的goroutine来创建".tmp"临时文件，
// 当进行日志文件切换时，直接将临时文件重命名即可；
type filePipeline struct {
	lg *zap.Logger

	// dir to put files
	dir string // 存放临时文件的目录
	// size of files to make, in bytes
	size int64 // 创建临时文件时预分配空间的大小，默认64MB
	// count number of files generated
	count int // 当前pipeline实例创建的临时文件数

	filec chan *fileutil.LockedFile // 新建的临时文件句柄会通过该channel返回给wal实例使用
	errc  chan error // 当创建临时文件出现异常时，会将异常传递到该channel中
	donec chan struct{} // 当file-pipeline-close被调用时，会关闭该channel，从而通知pipeline实例删除最后一次创建的临时文件
}

func newFilePipeline(lg *zap.Logger, dir string, fileSize int64) *filePipeline {
	fp := &filePipeline{ // 创建pipeline实例
		lg:    lg,
		dir:   dir,
		size:  fileSize,
		filec: make(chan *fileutil.LockedFile),
		errc:  make(chan error, 1),
		donec: make(chan struct{}),
	}
	go fp.run()
	return fp
}

// Open returns a fresh file for writing. Rename the file before calling
// Open again or there will be file collisions.
// 在wal切换日志文件时会调用该方法，从file-channel中获取之前创建好的临时文件
func (fp *filePipeline) Open() (f *fileutil.LockedFile, err error) {
	select {
	case f = <-fp.filec: // 从该通道获取已经创建好的临时文件
	case err = <-fp.errc: // 创建临时文件出现异常，则从该通道获取
	}
	return f, err
}

func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

func (fp *filePipeline) alloc() (f *fileutil.LockedFile, err error) {
	// count % 2 so this file isn't the same as the one last published
	// 防止与上一个临时文件重名，编号为0或1
	fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))
	if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return nil, err
	}
	if err = fileutil.Preallocate(f.File, fp.size, true); err != nil { // 尝试预分配空间
		if fp.lg != nil {
			fp.lg.Warn("failed to preallocate space when creating a new WAL", zap.Int64("size", fp.size), zap.Error(err))
		} else {
			plog.Errorf("failed to allocate space when creating new wal file (%v)", err)
		}
		f.Close() // 出现异常时关闭
		return nil, err
	}
	fp.count++ // 创建的临时文件数量递增
	return f, nil // 返回创建的临时文件
}

func (fp *filePipeline) run() {
	defer close(fp.errc)
	for {
		f, err := fp.alloc() // 创建临时文件
		if err != nil {
			fp.errc <- err // 创建临时文件失败，则将异常传递到err channel
			return
		}
		select {
		case fp.filec <- f: // 放入上边创建的临时文件句柄
		case <-fp.donec: // 关闭时触发，删除最后一次创建的临时文件
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
