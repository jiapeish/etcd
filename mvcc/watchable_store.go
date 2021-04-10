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
	"go.etcd.io/etcd/auth"
	"sync"
	"time"

	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/traceutil"
	"go.uber.org/zap"
)

// non-const so modifiable by tests
var (
	// chanBufLen is the length of the buffered chan
	// for sending out watched events.
	// See https://github.com/etcd-io/etcd/issues/11906 for more detail.
	chanBufLen = 128

	// maxWatchersPerSync is the number of watchers to sync in a single batch
	maxWatchersPerSync = 512
)

type watchable interface {
	watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc)
	progress(w *watcher)
	rev() int64
}

type watchableStore struct {
	*store

	// mu protects watcher groups and batches. It should never be locked
	// before locking store.mu to avoid deadlock.
	mu sync.RWMutex

	// victims are watcher batches that were blocked on the watch channel
	victims []watcherBatch
	victimc chan struct{}

	// contains all unsynced watchers that needs to sync with events that have happened
	unsynced watcherGroup

	// contains all synced watchers that are in sync with the progress of the store.
	// The key of the map is the key that the watcher watches on.
	synced watcherGroup

	stopc chan struct{}
	wg    sync.WaitGroup
}

// cancelFunc updates unsynced and synced maps when running
// cancel operations.
type cancelFunc func()

func New(lg *zap.Logger, b backend.Backend, le lease.Lessor, as auth.AuthStore, ig ConsistentIndexGetter, cfg StoreConfig) ConsistentWatchableKV {
	return newWatchableStore(lg, b, le, as, ig, cfg)
}

func newWatchableStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, as auth.AuthStore, ig ConsistentIndexGetter, cfg StoreConfig) *watchableStore {
	s := &watchableStore{
		store:    NewStore(lg, b, le, ig, cfg),
		victimc:  make(chan struct{}, 1),
		unsynced: newWatcherGroup(),
		synced:   newWatcherGroup(),
		stopc:    make(chan struct{}),
	}
	s.store.ReadView = &readView{s}
	s.store.WriteView = &writeView{s}
	if s.le != nil {
		// use this store as the deleter so revokes trigger watch events
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write(traceutil.TODO()) })
	}
	if as != nil {
		// TODO: encapsulating consistentindex into a separate package
		as.SetConsistentIndexSyncer(s.store.saveIndex)
	}
	s.wg.Add(2)
	go s.syncWatchersLoop()
	go s.syncVictimsLoop()
	return s
}

func (s *watchableStore) Close() error {
	close(s.stopc)
	s.wg.Wait()
	return s.store.Close()
}

func (s *watchableStore) NewWatchStream() WatchStream {
	watchStreamGauge.Inc()
	return &watchStream{
		watchable: s,
		ch:        make(chan WatchResponse, chanBufLen),
		cancels:   make(map[WatchID]cancelFunc),
		watchers:  make(map[WatchID]*watcher),
	}
}

func (s *watchableStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc) {
	wa := &watcher{
		key:    key,
		end:    end,
		minRev: startRev,
		id:     id,
		ch:     ch,
		fcs:    fcs,
	}

	s.mu.Lock()
	s.revMu.RLock()
	synced := startRev > s.store.currentRev || startRev == 0
	if synced {
		wa.minRev = s.store.currentRev + 1
		if startRev > wa.minRev {
			wa.minRev = startRev
		}
	}
	// 如果待添加watcher已经同步完成，则将其添加到synced watcher-group中，否则添加到unsynced watcher-group中
	if synced {
		s.synced.add(wa)
	} else {
		slowWatcherGauge.Inc()
		s.unsynced.add(wa)
	}
	s.revMu.RUnlock()
	s.mu.Unlock()

	watcherGauge.Inc()

	return wa, func() { s.cancelWatcher(wa) }
}

// cancelWatcher removes references of the watcher from the watchableStore
func (s *watchableStore) cancelWatcher(wa *watcher) {
	for {
		s.mu.Lock()
		if s.unsynced.delete(wa) {
			slowWatcherGauge.Dec()
			break
		} else if s.synced.delete(wa) {
			break
		} else if wa.compacted {
			break
		} else if wa.ch == nil {
			// already canceled (e.g., cancel/close race)
			break
		}

		if !wa.victim {
			panic("watcher not victim but not in watch groups")
		}

		var victimBatch watcherBatch
		for _, wb := range s.victims {
			if wb[wa] != nil {
				victimBatch = wb
				break
			}
		}
		if victimBatch != nil {
			slowWatcherGauge.Dec()
			delete(victimBatch, wa)
			break
		}

		// 可能待删除的watcher刚刚从synced watcher-group中删除且未添加到victim中，进行重试
		// victim being processed so not accessible; retry
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
	}

	watcherGauge.Dec()
	wa.ch = nil
	s.mu.Unlock()
}

func (s *watchableStore) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.store.Restore(b)
	if err != nil {
		return err
	}

	for wa := range s.synced.watchers {
		wa.restore = true
		s.unsynced.add(wa)
	}
	s.synced = newWatcherGroup()
	return nil
}

// syncWatchersLoop syncs the watcher in the unsynced map every 100ms.
func (s *watchableStore) syncWatchersLoop() {
	defer s.wg.Done()

	for {
		s.mu.RLock()
		st := time.Now()
		lastUnsyncedWatchers := s.unsynced.size()
		s.mu.RUnlock()

		unsyncedWatchers := 0
		if lastUnsyncedWatchers > 0 {
			unsyncedWatchers = s.syncWatchers()
		}
		syncDuration := time.Since(st)

		waitDuration := 100 * time.Millisecond
		// more work pending?
		if unsyncedWatchers != 0 && lastUnsyncedWatchers > unsyncedWatchers {
			// be fair to other store operations by yielding time taken
			waitDuration = syncDuration
		}

		select {
		case <-time.After(waitDuration):
		case <-s.stopc:
			return
		}
	}
}

// syncVictimsLoop tries to write precomputed watcher responses to
// watchers that had a blocked watcher channel
func (s *watchableStore) syncVictimsLoop() {
	defer s.wg.Done()

	for {
		for s.moveVictims() != 0 {
			// try to update all victim watchers
		}
		s.mu.RLock()
		isEmpty := len(s.victims) == 0
		s.mu.RUnlock()

		var tickc <-chan time.Time
		if !isEmpty {
			tickc = time.After(10 * time.Millisecond)
		}

		select {
		case <-tickc:
		case <-s.victimc:
		case <-s.stopc:
			return
		}
	}
}

// moveVictims tries to update watches with already pending event data
func (s *watchableStore) moveVictims() (moved int) {
	s.mu.Lock()
	victims := s.victims
	s.victims = nil // 清空
	s.mu.Unlock()

	var newVictim watcherBatch
	for _, wb := range victims { // 遍历victim中记录的watcher-batch
		// try to send responses again
		for w, eb := range wb { // 从watcher-batch中获取event-batch实例
			// watcher has observed the store up to, but not including, w.minRev
			rev := w.minRev - 1
			// 将event-batch封装成watch-response，并调用watcher send方法尝试发送出去
			if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
				pendingEventsGauge.Add(float64(len(eb.evs)))
			} else {
				// 如果watcher channel依然阻塞，则将对应的event实例重新放回new victim中保存
				if newVictim == nil {
					newVictim = make(watcherBatch)
				}
				newVictim[w] = eb
				continue
			}
			moved++ // 记录重试成功的event-batch个数
		}

		// assign completed victim watchers to unsync/sync
		s.mu.Lock()
		s.store.revMu.RLock()
		curRev := s.store.currentRev
		for w, eb := range wb { // 遍历watcher-batch
			// 1、如果event-batch实例被记录到new victim中，则表示watcher channel依然阻塞，watch response发送失败
			if newVictim != nil && newVictim[w] != nil {
				// couldn't send watch response; stays victim
				continue
			}
			// 2、如果event-batch实例未被记录到new victim中，则表示watch response发送成功
			w.victim = false
			if eb.moreRev != 0 { // 检测当前watcher后续是否还有未同步的event事件
				w.minRev = eb.moreRev
			}
			if w.minRev <= curRev { // 当前watcher未完成同步，移动到unsynced watcher-group中
				s.unsynced.add(w)
			} else { // 当前watcher已经完成同步，移动到synced watcher-group中
				slowWatcherGauge.Dec()
				s.synced.add(w)
			}
		}
		s.store.revMu.RUnlock()
		s.mu.Unlock()
	}

	if len(newVictim) > 0 { // 使用new victim更新watchable-store victim字段
		s.mu.Lock()
		s.victims = append(s.victims, newVictim)
		s.mu.Unlock()
	}

	return moved // 返回重试成功的event-batch个数
}

// syncWatchers syncs unsynced watchers by:
//	1. choose a set of watchers from the unsynced watcher group
//	2. iterate over the set to get the minimum revision and remove compacted watchers
//	3. use minimum revision to get all key-value pairs and send those events to watchers
//	4. remove synced watchers in set from unsynced group and move to synced group
func (s *watchableStore) syncWatchers() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 如果unsynced watcher-group为空，则直接返回
	if s.unsynced.size() == 0 {
		return 0
	}

	s.store.revMu.RLock()
	defer s.store.revMu.RUnlock()

	// in order to find key-value pairs from unsynced watchers, we need to
	// find min revision index, and these revisions can be used to
	// query the backend store of key-value pairs
	curRev := s.store.currentRev
	compactionRev := s.store.compactMainRev

	// 从unsyned watcher-group中查找一批此次需要同步到watcher实例，
	// 并将这些watcher实例封装成watcher-group实例返回，返回的min-rev是这批待同步的watcher实例中min-rev字段最小值
	wg, minRev := s.unsynced.choose(maxWatchersPerSync, curRev, compactionRev)
	// 将min-rev和cur-rev写入min-bytes和max-bytes中
	minBytes, maxBytes := newRevBytes(), newRevBytes()
	revToBytes(revision{main: minRev}, minBytes)
	revToBytes(revision{main: curRev + 1}, maxBytes)

	// UnsafeRange returns keys and values. And in boltdb, keys are revisions.
	// values are actual key-value pairs in backend.
	tx := s.store.b.ReadTx() // 获取只读事务
	tx.RLock()
	revs, vs := tx.UnsafeRange(keyBucketName, minBytes, maxBytes, 0) // 对key-bucket进行范围查找
	var evs []mvccpb.Event
	if s.store != nil && s.store.lg != nil {
		evs = kvsToEvents(s.store.lg, wg, revs, vs) // 将查询到的键值对转换成对应的event事件
	} else {
		// TODO: remove this in v3.5
		evs = kvsToEvents(nil, wg, revs, vs)
	}
	tx.RUnlock()

	var victims watcherBatch
	wb := newWatcherBatch(wg, evs) // 将上述watcher集合及event事件封装成watcher-batch
	for w := range wg.watchers {
		w.minRev = curRev + 1 // 更新min-rev，这样就不会被同一个修改操作触发两次

		eb, ok := wb[w]
		if !ok {
			// 该watcher实例所监听的键值对没有更新操作（在min-rev ~ cur-rev之间），
			// 所以没有触发，同步完毕，并将其转移到unsynced watcher-group中
			// bring un-notified watcher to synced
			s.synced.add(w)
			s.unsynced.delete(w)
			continue
		}

		if eb.moreRev != 0 {
			// 在min-rev ~ cur-rev之间触发当前watcher的更新操作太多，无法全部放到一个event-batch中，
			// 这里只将该watcher min-rev设置成more-rev，则下次处理unsynced watcher-group时，
			// 会将其后的更新操作查询出来继续处理
			w.minRev = eb.moreRev
		}

		// 将前面创建的event事件集合封装成watch-response实例，然后写入到watcher channel中
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: curRev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else { // 如果watcher channel阻塞，则将该watcher victim字段置为true
			if victims == nil {
				victims = make(watcherBatch) // 初始化victim
			}
			w.victim = true
		}

		if w.victim { // 如果watcher channel阻塞，则将触发它的event事件记录到victim中
			victims[w] = eb
		} else {
			if eb.moreRev != 0 {
				// 如果后续还有其他未处理的event事件，则当前watcher依然未同步完成
				// stay unsynced; more to read
				continue
			}
			s.synced.add(w) // 当前watcher已经同步完成，加入到synced watcher-group中
		}
		s.unsynced.delete(w) // 将当前watcher从unsynced watcher-group中删除
	}
	s.addVictim(victims) // 将victim中记录的watcher-batch添加到watchable-store-victim中等待处理

	vsz := 0
	for _, v := range s.victims {
		vsz += len(v)
	}
	slowWatcherGauge.Set(float64(s.unsynced.size() + vsz))

	return s.unsynced.size() // 返回此次处理之后的unsynced watcher-group长度
}

// kvsToEvents gets all events for the watchers from all key-value pairs
func kvsToEvents(lg *zap.Logger, wg *watcherGroup, revs, vals [][]byte) (evs []mvccpb.Event) {
	for i, v := range vals { // 键值对数据
		var kv mvccpb.KeyValue
		if err := kv.Unmarshal(v); err != nil {
			if lg != nil {
				lg.Panic("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
			} else {
				plog.Panicf("cannot unmarshal event: %v", err)
			}
		}

		// 在这批处理的所有watcher实例中，是否有监听了该key的实例
		// 注意：unsynced watcher-group中可能同时记录了监听单个key的watcher和监听范围的watcher，
		// 所以要同时查找这两种
		if !wg.contains(string(kv.Key)) {
			continue
		}

		ty := mvccpb.PUT // 默认为put
		if isTombstone(revs[i]) {
			ty = mvccpb.DELETE // 如果是tomb-stone，则是delete
			// patch in mod revision so watchers won't skip
			kv.ModRevision = bytesToRev(revs[i]).main
		}
		// 将该键值对转换成对应的event实例，并记录
		evs = append(evs, mvccpb.Event{Kv: &kv, Type: ty})
	}
	return evs
}

// notify notifies the fact that given event at the given rev just happened to
// watchers that watch on the key of the event.
func (s *watchableStore) notify(rev int64, evs []mvccpb.Event) {
	var victim watcherBatch
	// 将传入的event实例转换成watcher-batch，之后进行遍历，逐个watcher进行处理
	for w, eb := range newWatcherBatch(&s.synced, evs) {
		if eb.revs != 1 {
			if s.store != nil && s.store.lg != nil {
				s.store.lg.Panic(
					"unexpected multiple revisions in watch notification",
					zap.Int("number-of-revisions", eb.revs),
				)
			} else {
				plog.Panicf("unexpected multiple revisions in notification")
			}
		}
		// 将当前watcher对应的event实例封装成watch-response实例，并尝试写入watcher channel
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else { // 如果watcher channel阻塞，则将这些event实例记录到watchable-store victim字段中
			// move slow watcher to victims
			w.minRev = rev + 1
			if victim == nil {
				victim = make(watcherBatch)
			}
			w.victim = true
			victim[w] = eb
			s.synced.delete(w) // 将该watcher从synced watcher-group中删除
			slowWatcherGauge.Inc()
		}
	}
	s.addVictim(victim)
}

func (s *watchableStore) addVictim(victim watcherBatch) {
	if victim == nil {
		return
	}
	s.victims = append(s.victims, victim)
	select {
	case s.victimc <- struct{}{}:
	default:
	}
}

func (s *watchableStore) rev() int64 { return s.store.Rev() }

func (s *watchableStore) progress(w *watcher) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.synced.watchers[w]; ok {
		w.send(WatchResponse{WatchID: w.id, Revision: s.rev()})
		// If the ch is full, this watcher is receiving events.
		// We do not need to send progress at all.
	}
}

type watcher struct {
	// the watcher key
	key []byte
	// end indicates the end of the range to watch.
	// If end is set, the watcher is on a range.
	end []byte

	// victim is set when ch is blocked and undergoing victim processing
	victim bool

	// compacted is set when the watcher is removed because of compaction
	compacted bool

	// restore is true when the watcher is being restored from leader snapshot
	// which means that this watcher has just been moved from "synced" to "unsynced"
	// watcher group, possibly with a future revision when it was first added
	// to the synced watcher
	// "unsynced" watcher revision must always be <= current revision,
	// except when the watcher were to be moved from "synced" watcher group
	restore bool

	// minRev is the minimum revision update the watcher will accept
	minRev int64
	id     WatchID

	fcs []FilterFunc // 记录了过滤event实例的过滤器
	// a chan to send out the watch response.
	// The chan might be shared with other watchers.
	ch chan<- WatchResponse
}

func (w *watcher) send(wr WatchResponse) bool {
	progressEvent := len(wr.Events) == 0

	if len(w.fcs) != 0 {
		ne := make([]mvccpb.Event, 0, len(wr.Events))
		for i := range wr.Events { // 遍历watch response中封装的event实例，逐个进行过滤
			filtered := false
			for _, filter := range w.fcs {
				if filter(wr.Events[i]) {
					filtered = true
					break
				}
			}
			if !filtered { // 通过所有过滤器检测的event实例才能被记录到watch response中
				ne = append(ne, wr.Events[i])
			}
		}
		wr.Events = ne
	}

	// if all events are filtered out, we should send nothing.
	if !progressEvent && len(wr.Events) == 0 {
		return true
	}
	select {
	case w.ch <- wr:
		return true
	default: // 通道阻塞
		return false
	}
}
