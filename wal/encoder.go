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

package wal

import (
	"encoding/binary"
	"hash"
	"io"
	"os"
	"sync"

	"go.etcd.io/etcd/pkg/crc"
	"go.etcd.io/etcd/pkg/ioutil"
	"go.etcd.io/etcd/wal/walpb"
)

// walPageBytes is the alignment for flushing records to the backing Writer.
// It should be a multiple of the minimum sector size so that WAL can safely
// distinguish between torn writes and ordinary data corruption.
const walPageBytes = 8 * minSectorSize

type encoder struct {
	mu sync.Mutex // 进行读写文件时，需要加锁同步
	// 是带有缓冲区的writer，在写入时，每写满一个page大小的缓冲区，
	// 就会自动触发一次flush操作，将数据同步刷新到磁盘上；每个page大小由wal-pagebytes指定
	bw *ioutil.PageWriter

	crc       hash.Hash32
	buf       []byte // 日志序列化后，会暂存在该缓冲区，该缓冲区会被复用，防止每次序列化创建缓冲区带来的开销
	uint64buf []byte // 在写入一条日志记录时，该缓冲区用来暂存一个frame长度的数据（frame由日志数据和填充数据构成）
}

func newEncoder(w io.Writer, prevCrc uint32, pageOffset int) *encoder {
	return &encoder{
		bw:  ioutil.NewPageWriter(w, walPageBytes, pageOffset),
		crc: crc.New(prevCrc, crcTable),
		// 1MB buffer
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

// newFileEncoder creates a new encoder with current file offset for the page writer.
func newFileEncoder(f *os.File, prevCrc uint32) (*encoder, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return newEncoder(f, prevCrc, int(offset)), nil
}

// 该方法是真正完成日志写入功能的地方；
// 会对日志记录进行序列化，并完成8字节对齐；
func (e *encoder) encode(rec *walpb.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.crc.Write(rec.Data)
	rec.Crc = e.crc.Sum32()
	var (
		data []byte
		err  error
		n    int
	)

	// 将待写入的日志记录进行序列化，如果日志记录太大，无法复用encoder-buf缓冲区，则直接序列化
	if rec.Size() > len(e.buf) {
		data, err = rec.Marshal()
		if err != nil {
			return err
		}
	} else { // 复用encoder-buf缓冲区
		n, err = rec.MarshalTo(e.buf)
		if err != nil {
			return err
		}
		data = e.buf[:n]
	}

	// 计算序列化之后的数据长度，在encode-frame-size方法会完成8字节对齐，
	// 将日志数据和填充数据看作一个frame，返回值是整个frame的长度及其中填充数据的长度
	lenField, padBytes := encodeFrameSize(len(data))
	// 将frame的长度序列化到encoder-uint64-buf数组中，然后写入文件
	if err = writeUint64(e.bw, lenField, e.uint64buf); err != nil {
		return err
	}

	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...) // 向data中写入填充字节
	}
	n, err = e.bw.Write(data) // 将data中的序列化数据写入文件
	walWriteBytes.Add(float64(n))
	return err
}

func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}

func (e *encoder) flush() error {
	e.mu.Lock()
	n, err := e.bw.FlushN()
	e.mu.Unlock()
	walWriteBytes.Add(float64(n))
	return err
}

func writeUint64(w io.Writer, n uint64, buf []byte) error {
	// http://golang.org/src/encoding/binary/binary.go
	binary.LittleEndian.PutUint64(buf, n)
	nv, err := w.Write(buf)
	walWriteBytes.Add(float64(nv))
	return err
}
