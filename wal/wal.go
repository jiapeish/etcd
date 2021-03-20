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
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal/walpb"

	"github.com/coreos/pkg/capnslog"
	"go.uber.org/zap"
)

const (
	// 该类型日志记录的data字段保存了元数据，在每个wal文件的开头，都会记录一条metadata-type的日志记录
	metadataType int64 = iota + 1
	// 该类型日志记录的data字段保存的是entry记录，即客户端发给服务端处理的数据，如，raftexample中客户端发送的键值对数据
	entryType
	// 该类型日志记录的data字段保存的是当前集群的状态信息（即hardstate），在每次批量写入entry-type类型日志记录之前，都会先写入一条state-type日志记录
	stateType
	// 用于数据校验
	crcType
	// 保持了快照数据的相关信息，即walpb-snapshot，但其中不包含完整的快照数据
	snapshotType

	// warnSyncDuration is the amount of time allotted to an fsync before
	// logging a warning
	warnSyncDuration = time.Second
)

var (
	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

	plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "wal")

	ErrMetadataConflict             = errors.New("wal: conflicting metadata found")
	ErrFileNotFound                 = errors.New("wal: file not found")
	ErrCRCMismatch                  = errors.New("wal: crc mismatch")
	ErrSnapshotMismatch             = errors.New("wal: snapshot mismatch")
	ErrSnapshotNotFound             = errors.New("wal: snapshot not found")
	ErrSliceOutOfRange              = errors.New("wal: slice bounds out of range")
	ErrMaxWALEntrySizeLimitExceeded = errors.New("wal: max entry size limit exceeded")
	ErrDecoderNotFound              = errors.New("wal: decoder not found")
	crcTable                        = crc32.MakeTable(crc32.Castagnoli)
)

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
type WAL struct {
	lg *zap.Logger

	// 存放wal日志文件的路径
	dir string // the living directory of the underlay files

	// dirFile is a fd for the wal directory for syncing on Rename
	dirFile *os.File

	metadata []byte           // metadata recorded at the head of each WAL
	state    raftpb.HardState // hardstate recorded at the head of WAL

	// 其中index字段记录了对应快照数据所涵盖的最后一条entry记录的索引值，term字段记录了对应entry记录的term值
	start     walpb.Snapshot // snapshot to start reading
	// 在读取wal日志文件时，将二进制数据反序列化成record实例
	decoder   *decoder       // decoder to decode records
	readClose func() error   // closer for decode reader

	unsafeNoSync bool // if set, do not fsync

	// 读写wal日志文件时需要加锁同步
	mu      sync.Mutex
	// 最后一条entry记录的索引值
	enti    uint64   // index of the last entry saved to the wal
	// 负责将写入wal日志文件的record实例序列化成二进制数据
	encoder *encoder // encoder to encode records

	// 当前wal实例管理的所有wal日志文件对应的句柄
	locks []*fileutil.LockedFile // the locked files the WAL holds (the name is increasing)
	// 负责创建新的临时文件
	fp    *filePipeline
}

// Create creates a WAL ready for appending records. The given metadata is
// recorded at the head of each WAL file, and can be retrieved with ReadAll
// after the file is Open.
// 1、创建临时目录，并在临时目录中创建编号为"0-0"的wal日志文件，文件名由两部分组成：单调递增的seq，和该日志文件中第一条日志记录的索引值；
// 2、尝试为该wal日志文件预分配磁盘空间
// 3、向该wal日志文件中写入一条crc-type日志记录、一条metadata-type日志记录、一条snapshot-type日志记录；
// 4、创建wal实例关联的file-pipeline实例；
// 5、将临时目录重命名为wal-dir字段指定的名称；
// Note: 之所以先使用临时目录完成初始化操作，再将其重命名，是为了让整个初始化过程像一个原子操作；
func Create(lg *zap.Logger, dirpath string, metadata []byte) (*WAL, error) {
	if Exist(dirpath) { // 检测文件夹是否存在
		return nil, os.ErrExist
	}

	// keep temporary wal directory so WAL initialization appears atomic
	tmpdirpath := filepath.Clean(dirpath) + ".tmp" // 拼接临时目录d路径
	if fileutil.Exist(tmpdirpath) { // 清空临时目录中的文件
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	if err := fileutil.CreateDirAll(tmpdirpath); err != nil { // 创建临时文件夹
		if lg != nil {
			lg.Warn(
				"failed to create a temporary WAL directory",
				zap.String("tmp-dir-path", tmpdirpath),
				zap.String("dir-path", dirpath),
				zap.Error(err),
			)
		}
		return nil, err
	}

	p := filepath.Join(tmpdirpath, walName(0, 0)) // 第一个wal日志文件的路径，文件名为0-0
	f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode) // 打开并锁定临时文件
	if err != nil {
		if lg != nil {
			lg.Warn(
				"failed to flock an initial WAL file",
				zap.String("path", p),
				zap.Error(err),
			)
		}
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil { // 把临时文件的指针指向文件结尾
		if lg != nil {
			lg.Warn(
				"failed to seek an initial WAL file",
				zap.String("path", p),
				zap.Error(err),
			)
		}
		return nil, err
	}
	if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil { // 对新建的临时文件进行空间预分配
		if lg != nil {
			lg.Warn(
				"failed to preallocate an initial WAL file",
				zap.String("path", p),
				zap.Int64("segment-bytes", SegmentSizeBytes),
				zap.Error(err),
			)
		}
		return nil, err
	}

	w := &WAL{ // 创建wal实例
		lg:       lg,
		dir:      dirpath, // 存放wal日志文件的目录路径
		metadata: metadata, // 元数据
	}
	w.encoder, err = newFileEncoder(f.File, 0) // 创建写wal日志文件的encoder
	if err != nil {
		return nil, err
	}
	w.locks = append(w.locks, f) // 将wal日志文件对应的locked-file实例记录到locks字段中，表示当前wal实例正在管理该日志文件
	if err = w.saveCrc(0); err != nil { // 创建一条crc-type日志写入到wal日志文件
		return nil, err
	}
	// 将元数据封装成一条metadata-type日志记录写入wal日志文件
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
		return nil, err
	}
	if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil { // 创建一条空的snapshot-type日志记录写入临时文件
		return nil, err
	}

	if w, err = w.renameWAL(tmpdirpath); err != nil { // 将临时目录重命名，并创建wal实例关联的file-pipeline实例
		if lg != nil {
			lg.Warn(
				"failed to rename the temporary WAL directory",
				zap.String("tmp-dir-path", tmpdirpath),
				zap.String("dir-path", w.dir),
				zap.Error(err),
			)
		}
		return nil, err
	}

	var perr error
	defer func() {
		if perr != nil {
			w.cleanupWAL(lg)
		}
	}()

	// directory was renamed; sync parent dir to persist rename
	// 临时目录重命名之后，需要将重命名操作刷新到磁盘上
	pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir))
	if perr != nil {
		if lg != nil {
			lg.Warn(
				"failed to open the parent data directory",
				zap.String("parent-dir-path", filepath.Dir(w.dir)),
				zap.String("dir-path", w.dir),
				zap.Error(perr),
			)
		}
		return nil, perr
	}
	start := time.Now()
	if perr = fileutil.Fsync(pdir); perr != nil {
		if lg != nil {
			lg.Warn(
				"failed to fsync the parent data directory file",
				zap.String("parent-dir-path", filepath.Dir(w.dir)),
				zap.String("dir-path", w.dir),
				zap.Error(perr),
			)
		}
		return nil, perr
	}
	walFsyncSec.Observe(time.Since(start).Seconds())

	if perr = pdir.Close(); perr != nil {
		if lg != nil {
			lg.Warn(
				"failed to close the parent data directory file",
				zap.String("parent-dir-path", filepath.Dir(w.dir)),
				zap.String("dir-path", w.dir),
				zap.Error(perr),
			)
		}
		return nil, perr
	}

	return w, nil
}

func (w *WAL) SetUnsafeNoFsync() {
	w.unsafeNoSync = true
}

func (w *WAL) cleanupWAL(lg *zap.Logger) {
	var err error
	if err = w.Close(); err != nil {
		if lg != nil {
			lg.Panic("failed to close WAL during cleanup", zap.Error(err))
		} else {
			plog.Panicf("failed to close WAL during cleanup: %v", err)
		}
	}
	brokenDirName := fmt.Sprintf("%s.broken.%v", w.dir, time.Now().Format("20060102.150405.999999"))
	if err = os.Rename(w.dir, brokenDirName); err != nil {
		if lg != nil {
			lg.Panic(
				"failed to rename WAL during cleanup",
				zap.Error(err),
				zap.String("source-path", w.dir),
				zap.String("rename-path", brokenDirName),
			)
		} else {
			plog.Panicf("failed to rename WAL during cleanup: %v", err)
		}
	}
}

func (w *WAL) renameWAL(tmpdirpath string) (*WAL, error) {
	if err := os.RemoveAll(w.dir); err != nil { // 清空wal文件夹
		return nil, err
	}
	// On non-Windows platforms, hold the lock while renaming. Releasing
	// the lock and trying to reacquire it quickly can be flaky because
	// it's possible the process will fork to spawn a process while this is
	// happening. The fds are set up as close-on-exec by the Go runtime,
	// but there is a window between the fork and the exec where another
	// process holds the lock.
	if err := os.Rename(tmpdirpath, w.dir); err != nil { // 重命名临时文件夹
		if _, ok := err.(*os.LinkError); ok {
			return w.renameWALUnlock(tmpdirpath)
		}
		return nil, err
	}
	w.fp = newFilePipeline(w.lg, w.dir, SegmentSizeBytes) // 创建wal实例关联的file-pipeline实例
	df, err := fileutil.OpenDir(w.dir)
	w.dirFile = df // wal-dir-file字段记录来wal日志目录对应的文件句柄
	return w, err
}

func (w *WAL) renameWALUnlock(tmpdirpath string) (*WAL, error) {
	// rename of directory with locked files doesn't work on windows/cifs;
	// close the WAL to release the locks so the directory can be renamed.
	if w.lg != nil {
		w.lg.Info(
			"closing WAL to release flock and retry directory renaming",
			zap.String("from", tmpdirpath),
			zap.String("to", w.dir),
		)
	} else {
		plog.Infof("releasing file lock to rename %q to %q", tmpdirpath, w.dir)
	}
	w.Close()

	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		return nil, err
	}

	// reopen and relock
	newWAL, oerr := Open(w.lg, w.dir, walpb.Snapshot{})
	if oerr != nil {
		return nil, oerr
	}
	if _, _, _, err := newWAL.ReadAll(); err != nil {
		newWAL.Close()
		return nil, err
	}
	return newWAL, nil
}

// Open opens the WAL at the given snap.
// The snap SHOULD have been previously saved to the WAL, or the following
// ReadAll will fail.
// The returned WAL is ready to read and the first record will be the one after
// the given snap. The WAL cannot be appended to before reading out all of its
// previous records.
// 打开wal日志文件时，会指定随后的读取操作的起始index，而不是每次都从第一个日志文件开始读取
func Open(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(lg, dirpath, snap, true)
	if err != nil {
		return nil, err
	}
	if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil {
		return nil, err
	}
	return w, nil
}

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
func OpenForRead(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	return openAtIndex(lg, dirpath, snap, false)
}

// 入参的snap-index指定了日志读取的起始位置，write参数指定了打开日志文件的模式
func openAtIndex(lg *zap.Logger, dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
	names, nameIndex, err := selectWALFiles(lg, dirpath, snap)
	if err != nil {
		return nil, err
	}

	rs, ls, closer, err := openWALFiles(lg, dirpath, names, nameIndex, write)
	if err != nil {
		return nil, err
	}

	// create a WAL ready for reading
	w := &WAL{ // 创建wal实例
		lg:        lg,
		dir:       dirpath,
		start:     snap, // 记录snapshot信息
		decoder:   newDecoder(rs...), // 创建用于读取日志记录的decoder实例，这里并没有初始化encoder，所以还不能写入日志记录
		readClose: closer, // 如果是只读模式，在读取完全部日志文件后，会调用该方法关闭所有日志文件
		locks:     ls, // 当前wal实例管理的日志文件
	}

	// 如果是读写模式，读取完全部日志文件后，由于后续有追加操作，所以不需要关闭日志文件；
	// 另外，还要为wal实例创建关联的file-pipeline实例，用于产生新的日志文件
	if write {
		// write reuses the file descriptors from read; don't close so
		// WAL can append without dropping the file lock
		w.readClose = nil
		if _, _, err := parseWALName(filepath.Base(w.tail().Name())); err != nil {
			closer()
			return nil, err
		}
		w.fp = newFilePipeline(lg, w.dir, SegmentSizeBytes)
	}

	return w, nil
}

func selectWALFiles(lg *zap.Logger, dirpath string, snap walpb.Snapshot) ([]string, int, error) {
	names, err := readWALNames(lg, dirpath) // 获取全部的wal日志文件名，并对这些文件名进行排序
	if err != nil {
		return nil, -1, err
	}

	// 根据wal日志文件名的规则，查找上面得到的所有文件名，
	// 找到index最大且index小于snap-index的wal日志文件，
	// 并返回该文件在names数组中的索引name-index
	nameIndex, ok := searchIndex(lg, names, snap.Index)
	if !ok || !isValidSeq(lg, names[nameIndex:]) {
		err = ErrFileNotFound
		return nil, -1, err
	}

	return names, nameIndex, nil
}

func openWALFiles(lg *zap.Logger, dirpath string, names []string, nameIndex int, write bool) ([]io.Reader, []*fileutil.LockedFile, func() error, error) {
	rcs := make([]io.ReadCloser, 0)
	rs := make([]io.Reader, 0) // 注意该切片中元素类型，在decoder中会用到
	ls := make([]*fileutil.LockedFile, 0)
	for _, name := range names[nameIndex:] { // 从name-index开始读取剩余的wal日志文件
		p := filepath.Join(dirpath, name) // 获取wal日志文件的绝对路径
		if write { // 以读写模式打开wal日志文件
			l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode) // 打开wal日志文件并且加文件锁
			if err != nil {
				closeAll(rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, l) // 文件句柄记录到ls和rcs这两个切片中
			rcs = append(rcs, l)
		} else { // 以只读模式打开日志文件
			rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, nil)
			rcs = append(rcs, rf) // 只将文件句柄记录到rcs切片中
		}
		rs = append(rs, rcs[len(rcs)-1]) // 将文件句柄记录到rs切片中
	}

	closer := func() error { return closeAll(rcs...) } // 后边关闭文件时，会调用该函数

	return rs, ls, closer, nil
}

// ReadAll reads out records of the current WAL.
// If opened in write mode, it must read out all records until EOF. Or an error
// will be returned.
// If opened in read mode, it will try to read all records if possible.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If loaded snap doesn't match with the expected one, it will return
// all the records and error ErrSnapshotMismatch.
// TODO: detect not-last-snap error.
// TODO: maybe loose the checking of match.
// After ReadAll, the WAL will be ready for appending new records.
// 1、该方法首先从wal-start字段指定的位置开始读取日志记录，读取完毕之后，会根据读取的情况进行异常处理；
// 2、然后根据当前wal实例的模式进行不同的处理：
//   2.1、如果处于读写模式，则需要先对后续的wal日志文件进行填充并初始化wal-encoder字段，为后面写入日志做准备；
//   2.2、如果处于只读模式，则需要关闭所有的日志文件；
// 3、该方法的几个返回值都是从日志记录中读取到的；
// 4、通过该方法读取完全部日志记录后，wal-encoder字段才会被初始化，之后，才能通过该wal实例向日志文件追加记录；
func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{} // 创建record实例

	if w.decoder == nil {
		return nil, state, nil, ErrDecoderNotFound
	}
	decoder := w.decoder // 解码器，负责读取日志文件，并将日志数据反序列化成record实例

	var match bool // 标识是否找到了start字段对应的日志记录
	// 循环读取wal日志文件中的数据，多个wal日志文件的切换是在decoder中完成的
	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type { // 根据日志记录的类型进行分类处理
		case entryType:
			e := mustUnmarshalEntry(rec.Data) // 反序列化record-data中记录的数据，得到entry实例
			// 0 <= e.Index-w.start.Index - 1 < len(ents)
			if e.Index > w.start.Index { // 将start之后的entry记录添加到ents中保存
				// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
				up := e.Index - w.start.Index - 1
				if up > uint64(len(ents)) {
					// return error before append call causes runtime panic
					return nil, state, nil, ErrSliceOutOfRange
				}
				ents = append(ents[:up], e)
			}
			w.enti = e.Index // 记录读取到的最后一条entry记录的索引值

		case stateType:
			state = mustUnmarshalState(rec.Data) // 更新待返回的hard-state状态信息

		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) { // 检测metadata数据是否发生冲突，如果冲突则抛出异常
				state.Reset()
				return nil, state, nil, ErrMetadataConflict
			}
			metadata = rec.Data // 更新待返回的元数据

		case crcType: // 读取到crc-type日志记录，则更新decoder中的crc信息
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				state.Reset()
				return nil, state, nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc) // 更新record的crc字段

		case snapshotType:
			var snap walpb.Snapshot
			pbutil.MustUnmarshal(&snap, rec.Data) // 解析快照相关数据
			if snap.Index == w.start.Index { // 异常检测
				if snap.Term != w.start.Term {
					state.Reset()
					return nil, state, nil, ErrSnapshotMismatch
				}
				match = true
			}

		default: // 其他未知类型
			state.Reset()
			return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	// 读取wal日志文件的操作上边已完成，然后根据wal-locks字段是否有值判断当前wal是哪个模式
	switch w.tail() {
	case nil:
		// We do not have to read out all entries in read mode.
		// The last record maybe a partial written one, so
		// ErrunexpectedEOF might be returned.
		// 对于只读模式，并不需要将全部日志都读出来，因为以只读模式打开wal日志文件时，并没有加锁，
		// 所以最后一条日志记录可能只写了一半，从而导致异常
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			state.Reset()
			return nil, state, nil, err
		}
	default:
		// We must read all of the entries if WAL is opened in write mode.
		// 对于读写模式，则需要将日志记录全部读出来，所以此处不是eof异常就报错；
		if err != io.EOF {
			state.Reset()
			return nil, state, nil, err
		}
		// decodeRecord() will return io.EOF if it detects a zero record,
		// but this zero record may be followed by non-zero records from
		// a torn write. Overwriting some of these non-zero records, but
		// not all, will cause CRC errors on WAL open. Since the records
		// were never fully synced to disk in the first place, it's safe
		// to zero them out to avoid any CRC errors from new writes.
		// 将文件指针移动到读取结束的位置，并将文件后续部分全部填充为0
		if _, err = w.tail().Seek(w.decoder.lastOffset(), io.SeekStart); err != nil {
			return nil, state, nil, err
		}
		if err = fileutil.ZeroToEnd(w.tail().File); err != nil {
			return nil, state, nil, err
		}
	}

	err = nil
	if !match { // 如果在读取过程中没有找到与start对应的日志记录，则异常
		err = ErrSnapshotNotFound
	}

	// close decoder, disable reading
	if w.readClose != nil { // 如果是只读模式，则关闭所有日志文件
		w.readClose() // wal-read-close实际指向的是wal-close-all方法
		w.readClose = nil
	}
	w.start = walpb.Snapshot{} // 清空start字段

	w.metadata = metadata

	if w.tail() != nil { // 如果是读写模式，则初始化wal-encoder字段，为后面写入日志做准备
		// create encoder (chain crc with the decoder), enable appending
		w.encoder, err = newFileEncoder(w.tail().File, w.decoder.lastCRC())
		if err != nil {
			return
		}
	}
	w.decoder = nil // 清空wal-decoder字段，后续不能再用该wal实例进行读取了

	return metadata, state, ents, err
}

// ValidSnapshotEntries returns all the valid snapshot entries in the wal logs in the given directory.
// Snapshot entries are valid if their index is less than or equal to the most recent committed hardstate.
func ValidSnapshotEntries(lg *zap.Logger, walDir string) ([]walpb.Snapshot, error) {
	var snaps []walpb.Snapshot
	var state raftpb.HardState
	var err error

	rec := &walpb.Record{}
	names, err := readWALNames(lg, walDir)
	if err != nil {
		return nil, err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(lg, walDir, names, 0, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			snaps = append(snaps, loadedSnap)
		case stateType:
			state = mustUnmarshalState(rec.Data)
		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				return nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		}
	}
	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	// filter out any snaps that are newer than the committed hardstate
	n := 0
	for _, s := range snaps {
		if s.Index <= state.Commit {
			snaps[n] = s
			n++
		}
	}
	snaps = snaps[:n:n]

	return snaps, nil
}

// Verify reads through the given WAL and verifies that it is not corrupted.
// It creates a new decoder to read through the records of the given WAL.
// It does not conflict with any open WAL, but it is recommended not to
// call this function after opening the WAL for writing.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If the loaded snap doesn't match with the expected one, it will
// return error ErrSnapshotMismatch.
func Verify(lg *zap.Logger, walDir string, snap walpb.Snapshot) error {
	var metadata []byte
	var err error
	var match bool

	rec := &walpb.Record{}

	names, nameIndex, err := selectWALFiles(lg, walDir, snap)
	if err != nil {
		return err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(lg, walDir, names, nameIndex, false)
	if err != nil {
		return err
	}

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				return ErrMetadataConflict
			}
			metadata = rec.Data
		case crcType:
			crc := decoder.crc.Sum32()
			// Current crc of decoder must match the crc of the record.
			// We need not match 0 crc, since the decoder is a new one at this point.
			if crc != 0 && rec.Validate(crc) != nil {
				return ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			if loadedSnap.Index == snap.Index {
				if loadedSnap.Term != snap.Term {
					return ErrSnapshotMismatch
				}
				match = true
			}
		// We ignore all entry and state type records as these
		// are not necessary for validating the WAL contents
		case entryType:
		case stateType:
		default:
			return fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	if closer != nil {
		closer()
	}

	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}

	if !match {
		return ErrSnapshotNotFound
	}

	return nil
}

// cut closes current file written and creates a new one ready to append.
// cut first creates a temp wal file and writes necessary headers into it.
// Then cut atomically rename temp wal file to a wal file.
// 随着wal日志文件的不断写入，单个日志文件会不断变大，而每个日志文件的大小是有上限的，其
// 阈值由segment-size-bytes指定（默认值为64MB），这个值也是日志文件预分配磁盘空间的大小；
// 当单个日志文件的大小超过该值时，就会触发日志文件的切换，切换过程在该方法实现；
// 1、cut方法首先通过file-pipeline获取一个新建的临时文件，然后写入crc-type、meta-type、state-type等
// 	  必要日志记录（与create方法类似）；
// 2、将临时文件重命名成符合wal日志命名规范的新日志文件；
// 3、创建对应的encoder实例更新到wal-encoder字段；
func (w *WAL) cut() error {
	// close old wal file; truncate to avoid wasting space if an early cut
	off, serr := w.tail().Seek(0, io.SeekCurrent) // 获取当前日志文件的文件指针
	if serr != nil {
		return serr
	}

	// 根据当前的文件指针位置，将后续填充内容trunc掉，用于处理提前切换和预分配空间未使用的情况，
	// trunc后可以释放该日志文件后续未使用的空间
	if err := w.tail().Truncate(off); err != nil {
		return err
	}

	// 执行一次sync方法，将修改同步刷新到磁盘上
	if err := w.sync(); err != nil {
		return err
	}

	// 根据当前最后一个日志文件的名称，确定下一个新日志文件的名称；
	// seq方法返回当前最后一个日志文件的编号，enti记录了当前最后一条日志记录的索引值
	fpath := filepath.Join(w.dir, walName(w.seq()+1, w.enti+1))

	// create a temp wal file with name sequence + 1, or truncate the existing one
	// 从file-pipeline获取新建的临时文件
	newTail, err := w.fp.Open()
	if err != nil {
		return err
	}

	// update writer and save the previous crc
	w.locks = append(w.locks, newTail) // 保存临时文件的句柄
	prevCrc := w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc) // 创建临时文件对应的encoder实例，并更新到wal-encoder字段中
	if err != nil {
		return err
	}

	// 向临时文件中添加一条crc-type日志记录
	if err = w.saveCrc(prevCrc); err != nil {
		return err
	}

	// 向临时文件中添加一条metadata-type日志记录
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: w.metadata}); err != nil {
		return err
	}

	// 向临时文件中添加一条state-type日志记录
	if err = w.saveState(&w.state); err != nil {
		return err
	}

	// atomically move temp wal file to wal file
	// 执行一次sync方法，将修改同步刷新到磁盘上
	if err = w.sync(); err != nil {
		return err
	}

	// 记录当前文件指针的位置，为重命名之后重新打开文件做准备
	off, err = w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// 将临时文件重命名为之前获取的新日志文件名称
	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}
	start := time.Now()
	// 将重命名这一操作同步刷新到磁盘上，fsync操作不仅会将文件数据刷新到磁盘上，
	// 还会将文件的元数据（文件长度和名称等）也刷新到磁盘上
	if err = fileutil.Fsync(w.dirFile); err != nil {
		return err
	}
	walFsyncSec.Observe(time.Since(start).Seconds())

	// reopen newTail with its new path so calls to Name() match the wal filename format
	newTail.Close() // 关闭临时文件对应的句柄

	// 打开重命名之后的新日志文件
	if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return err
	}
	// 将文件指针的位置移动到之前保存的位置
	if _, err = newTail.Seek(off, io.SeekStart); err != nil {
		return err
	}

	w.locks[len(w.locks)-1] = newTail // 将wal-locks中最后一项更新成新日志文件

	prevCrc = w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc) // 创建新日志文件对应的encoder实例，并更新到wal-encoder字段中
	if err != nil {
		return err
	}

	if w.lg != nil {
		w.lg.Info("created a new WAL segment", zap.String("path", fpath))
	} else {
		plog.Infof("segmented wal file %v is created", fpath)
	}
	return nil
}

// 该方法将日志同步刷新到磁盘上
func (w *WAL) sync() error {
	if w.unsafeNoSync {
		return nil
	}
	if w.encoder != nil {
		if err := w.encoder.flush(); err != nil { // 先使用encoder的flush方法进行同步刷新
			return err
		}
	}
	start := time.Now()
	err := fileutil.Fdatasync(w.tail().File) // 使用操作系统的fdatasync将数据真正刷新到磁盘上

	took := time.Since(start)
	if took > warnSyncDuration { // 对该刷新操作的执行时间进行监控，超过默认时间则输出告警日志
		if w.lg != nil {
			w.lg.Warn(
				"slow fdatasync",
				zap.Duration("took", took),
				zap.Duration("expected-duration", warnSyncDuration),
			)
		} else {
			plog.Warningf("sync duration of %v, expected less than %v", took, warnSyncDuration)
		}
	}
	walFsyncSec.Observe(took.Seconds())

	return err
}

func (w *WAL) Sync() error {
	return w.sync()
}

// ReleaseLockTo releases the locks, which has smaller index than the given index
// except the largest one among them.
// For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
// lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
func (w *WAL) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.locks) == 0 {
		return nil
	}

	var smaller int
	found := false
	for i, l := range w.locks {
		_, lockIndex, err := parseWALName(filepath.Base(l.Name()))
		if err != nil {
			return err
		}
		if lockIndex >= index {
			smaller = i - 1
			found = true
			break
		}
	}

	// if no lock index is greater than the release index, we can
	// release lock up to the last one(excluding).
	if !found {
		smaller = len(w.locks) - 1
	}

	if smaller <= 0 {
		return nil
	}

	for i := 0; i < smaller; i++ {
		if w.locks[i] == nil {
			continue
		}
		w.locks[i].Close()
	}
	w.locks = w.locks[smaller:]

	return nil
}

// Close closes the current WAL file and directory.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp != nil {
		w.fp.Close()
		w.fp = nil
	}

	if w.tail() != nil {
		if err := w.sync(); err != nil {
			return err
		}
	}
	for _, l := range w.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			if w.lg != nil {
				w.lg.Warn("failed to close WAL", zap.Error(err))
			} else {
				plog.Errorf("failed to unlock during closing wal: %s", err)
			}
		}
	}

	return w.dirFile.Close()
}

func (w *WAL) saveEntry(e *raftpb.Entry) error {
	// TODO: add MustMarshalTo to reduce one allocation.
	b := pbutil.MustMarshal(e) // 将entry记录序列化
	rec := &walpb.Record{Type: entryType, Data: b} // 将序列化后的数据封装成entry-type类型的record记录
	if err := w.encoder.encode(rec); err != nil { // 追加日志记录
		return err
	}
	w.enti = e.Index // 更新wal-enti字段，其中保存了最后一条entry记录的索引值
	return nil
}

func (w *WAL) saveState(s *raftpb.HardState) error {
	if raft.IsEmptyHardState(*s) {
		return nil
	}
	w.state = *s
	b := pbutil.MustMarshal(s)
	rec := &walpb.Record{Type: stateType, Data: b}
	return w.encoder.encode(rec)
}

// 追加日志的方法：save和save-snapshot
// save方法：
// 		1、将待写入的entry记录封装成entry-type类型的record实例，然后将其序列化并追加到日志文件中；
// 		2、将hard-state封装成state-type类型的record实例，然后将其序列化并追加到日志文件中；
// 		3、将这些日志记录同步刷新到磁盘；
func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// short cut, do not call sync
	// 边界检查，如果待写入的hard-state和entry数组都为空，则直接返回；
	// 否则需要将修改同步到磁盘上；
	if raft.IsEmptyHardState(st) && len(ents) == 0 {
		return nil
	}

	mustSync := raft.MustSync(st, w.state, len(ents))

	// TODO(xiangli): no more reference operator
	// 遍历待写入的entry数组，将每个entry实例序列化并封装成entry-type类型的日志记录，写入日志文件
	for i := range ents {
		if err := w.saveEntry(&ents[i]); err != nil {
			return err
		}
	}
	if err := w.saveState(&st); err != nil {
		return err
	}

	curOff, err := w.tail().Seek(0, io.SeekCurrent) // 获取当前日志段的文件指针
	if err != nil {
		return err
	}
	if curOff < SegmentSizeBytes { // 如果未写满预分配的日志空间，将新日志刷新到磁盘后，即可返回
		if mustSync {
			return w.sync() // 将上述追加的日志记录同步刷新到磁盘上
		}
		return nil
	}

	return w.cut() // 当前文件大小超出了预分配的空间，则需要进行日志文件的切换
}

func (w *WAL) SaveSnapshot(e walpb.Snapshot) error {
	b := pbutil.MustMarshal(&e)

	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{Type: snapshotType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	// update enti only when snapshot is ahead of last index
	if w.enti < e.Index {
		w.enti = e.Index
	}
	return w.sync()
}

func (w *WAL) saveCrc(prevCrc uint32) error {
	return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}

func (w *WAL) tail() *fileutil.LockedFile {
	if len(w.locks) > 0 {
		return w.locks[len(w.locks)-1]
	}
	return nil
}

func (w *WAL) seq() uint64 {
	t := w.tail()
	if t == nil {
		return 0
	}
	seq, _, err := parseWALName(filepath.Base(t.Name()))
	if err != nil {
		if w.lg != nil {
			w.lg.Fatal("failed to parse WAL name", zap.String("name", t.Name()), zap.Error(err))
		} else {
			plog.Fatalf("bad wal name %s (%v)", t.Name(), err)
		}
	}
	return seq
}

func closeAll(rcs ...io.ReadCloser) error {
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
