package glock

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// fakeDriver 是遵守 Driver 契约的内存实现，仅用于编排层单测。
// 完整契约测试见 locktest 模块。
type fakeDriver struct {
	mu      sync.Mutex
	recs    map[string]*fakeRec
	sems    map[string]*fakeSem // 信号量记录：键空间与锁隔离
	counter uint64
	refresh int // Refresh 成功次数

	tries   map[string][]time.Time   // 每键 TryAcquire 时间戳（等待策略断言用）
	changed map[string]chan struct{} // 每键唤醒广播：变更时关闭并重置
	waiting atomic.Int64             // 正在 WaitForChange 中停靠的等待者数（测试用）

	nextRefreshErr error // 单次续期故障注入（选主/丢失路径测试用），消耗即清空
}

type fakeRec struct {
	mode    Mode
	owner   string // 仅 Write 模式使用
	writer  *fakeHoldRec
	readers map[string]*fakeHoldRec // 仅 Read 模式使用
}

// fakeSem 是一个信号量池：cap 在首个获取时固定，holders 为 Owner → 槽位
// 计数与租约（同 Owner 的全部槽位共享一份租约）。
type fakeSem struct {
	cap     int
	holders map[string]*fakeHoldRec
}

type fakeHoldRec struct {
	count   int
	fence   uint64
	expires time.Time
}

func newFakeDriver() *fakeDriver {
	return &fakeDriver{recs: map[string]*fakeRec{}, sems: map[string]*fakeSem{},
		tries: map[string][]time.Time{}, changed: map[string]chan struct{}{}}
}

// tryTimes 返回 key 的全部 TryAcquire 时间戳（等待策略断言用）。
func (d *fakeDriver) tryTimes(key string) []time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]time.Time(nil), d.tries[key]...)
}

func (d *fakeDriver) live(r *fakeRec, now time.Time) bool {
	if r == nil {
		return false
	}
	if r.mode == Write {
		return r.writer != nil && r.writer.expires.After(now)
	}
	for _, h := range r.readers {
		if h.expires.After(now) {
			return true
		}
	}
	return false
}

func (d *fakeDriver) purge(r *fakeRec, now time.Time) {
	if r.mode == Write {
		if r.writer != nil && !r.writer.expires.After(now) {
			r.writer = nil
		}
		return
	}
	for o, h := range r.readers {
		if !h.expires.After(now) {
			delete(r.readers, o)
		}
	}
}

func (d *fakeDriver) TryAcquire(_ context.Context, key string, mode Mode, owner string, lease time.Duration) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	d.tries[key] = append(d.tries[key], now)
	r := d.recs[key]
	if r != nil {
		d.purge(r, now)
	}
	if d.live(r, now) {
		switch {
		case mode == Write && r.mode == Write && r.owner == owner:
			r.writer.count++
			r.writer.expires = now.Add(lease)
			return r.writer.fence, nil
		case mode == Write && r.mode == Write:
			return 0, fmt.Errorf("fake: %w", ErrHeld)
		case mode == Write && r.mode == Read:
			if _, mine := r.readers[owner]; mine {
				return 0, fmt.Errorf("fake: %w", ErrModeConflict) // 持读取写：升级，明确不做
			}
			return 0, fmt.Errorf("fake: %w", ErrHeld)
		case mode == Read && r.mode == Write:
			if r.owner == owner {
				return 0, fmt.Errorf("fake: %w", ErrModeConflict) // 持写取读：应降级
			}
			return 0, fmt.Errorf("fake: %w", ErrHeld)
		default: // Read on Read
			if h, ok := r.readers[owner]; ok {
				h.count++
				h.expires = now.Add(lease)
				return h.fence, nil
			}
			// 新读者加入是获取事件：递增栅栏。
			d.counter++
			r.readers[owner] = &fakeHoldRec{count: 1, fence: d.counter, expires: now.Add(lease)}
			return d.counter, nil
		}
	}
	// 全新获取（记录不存在或已全部过期）。
	d.counter++
	nr := &fakeRec{mode: mode}
	if mode == Write {
		nr.owner = owner
		nr.writer = &fakeHoldRec{count: 1, fence: d.counter, expires: now.Add(lease)}
	} else {
		nr.readers = map[string]*fakeHoldRec{owner: {count: 1, fence: d.counter, expires: now.Add(lease)}}
	}
	d.recs[key] = nr
	return d.counter, nil
}

func (d *fakeDriver) holdOf(r *fakeRec, mode Mode, owner string) *fakeHoldRec {
	if mode == Read {
		return r.readers[owner]
	}
	if r.owner != owner {
		return nil
	}
	return r.writer
}

// semLiveOf 取信号量池中 owner 的存活槽位记录，无效返回 nil。
func (d *fakeDriver) semLiveOf(r *fakeSem, owner string, fence uint64, now time.Time) *fakeHoldRec {
	if !d.semLive(r, now) {
		return nil
	}
	h := r.holders[owner]
	if h == nil || h.fence != fence || !h.expires.After(now) {
		return nil
	}
	return h
}

// TryAcquireSemaphore 实现 glock.SemaphoreDriver。容量在池首个获取时固定；
// 存活槽位已满返回 ErrHeld（重入同样受容量约束），容量声明不符返回
// ErrCapacityMismatch；全部槽位过期清空后允许重新声明。
func (d *fakeDriver) TryAcquireSemaphore(_ context.Context, key string, n int, owner string, lease time.Duration) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	r := d.sems[key]
	if r != nil {
		d.semPurge(r, now)
	}
	if d.semLive(r, now) {
		if r.cap != n {
			return 0, fmt.Errorf("fake: %w", ErrCapacityMismatch)
		}
		inUse := 0
		for _, h := range r.holders {
			inUse += h.count
		}
		if inUse >= n {
			return 0, fmt.Errorf("fake: %w", ErrHeld)
		}
		if h, ok := r.holders[owner]; ok {
			h.count++
			h.expires = now.Add(lease)
			return h.fence, nil // 重入多占槽位，沿用代际
		}
	} else {
		// 全新池（记录不存在或槽位全部过期）：容量在此固定。
		r = &fakeSem{cap: n, holders: map[string]*fakeHoldRec{}}
		d.sems[key] = r
	}
	d.counter++
	r.holders[owner] = &fakeHoldRec{count: 1, fence: d.counter, expires: now.Add(lease)}
	return d.counter, nil
}

func (d *fakeDriver) semLive(r *fakeSem, now time.Time) bool {
	if r == nil {
		return false
	}
	for _, h := range r.holders {
		if h.expires.After(now) {
			return true
		}
	}
	return false
}

func (d *fakeDriver) semPurge(r *fakeSem, now time.Time) {
	for o, h := range r.holders {
		if !h.expires.After(now) {
			delete(r.holders, o)
		}
	}
}

func (d *fakeDriver) Refresh(_ context.Context, key string, mode Mode, owner string, fence uint64, lease time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.nextRefreshErr; err != nil {
		d.nextRefreshErr = nil
		return err
	}
	now := time.Now()
	if mode == Semaphore {
		r := d.sems[key]
		if d.semLiveOf(r, owner, fence, now) == nil {
			return fmt.Errorf("fake: %w", ErrNotOwner)
		}
		r.holders[owner].expires = now.Add(lease)
		d.refresh++
		return nil
	}
	r := d.recs[key]
	if r == nil || r.mode != mode || !d.live(r, now) {
		return fmt.Errorf("fake: %w", ErrNotOwner)
	}
	h := d.holdOf(r, mode, owner)
	if h == nil || h.fence != fence || !h.expires.After(now) {
		return fmt.Errorf("fake: %w", ErrNotOwner)
	}
	h.expires = now.Add(lease)
	d.refresh++
	return nil
}

func (d *fakeDriver) Release(_ context.Context, key string, mode Mode, owner string, fence uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if mode == Semaphore {
		r := d.sems[key]
		if d.semLiveOf(r, owner, fence, now) == nil {
			return fmt.Errorf("fake: %w", ErrNotOwner)
		}
		h := r.holders[owner]
		h.count--
		d.broadcast(key) // 槽位腾出：唤醒等待者（记录是否删净都广播）
		if h.count <= 0 {
			delete(r.holders, owner)
			if len(r.holders) == 0 {
				delete(d.sems, key)
			}
		}
		return nil
	}
	r := d.recs[key]
	if r == nil || r.mode != mode || !d.live(r, now) {
		return fmt.Errorf("fake: %w", ErrNotOwner)
	}
	h := d.holdOf(r, mode, owner)
	if h == nil || h.fence != fence || !h.expires.After(now) {
		return fmt.Errorf("fake: %w", ErrNotOwner)
	}
	h.count--
	if h.count <= 0 {
		if mode == Read {
			delete(r.readers, owner)
			if len(r.readers) == 0 {
				delete(d.recs, key)
				d.broadcast(key)
			}
		} else {
			delete(d.recs, key)
			d.broadcast(key)
		}
	}
	return nil
}

func (d *fakeDriver) Downgrade(_ context.Context, key, owner string, fence uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	r := d.recs[key]
	if r == nil || r.mode != Write || r.owner != owner || !d.live(r, now) || r.writer.fence != fence {
		return fmt.Errorf("fake: %w", ErrNotOwner)
	}
	if r.writer.count > 1 {
		return fmt.Errorf("fake: %w", ErrModeConflict)
	}
	r.mode = Read
	r.owner = ""
	r.readers = map[string]*fakeHoldRec{owner: {count: 1, fence: r.writer.fence, expires: r.writer.expires}}
	r.writer = nil
	d.broadcast(key) // 写降读放行读者
	return nil
}

// broadcast 关闭 key 的唤醒通道（须持 d.mu 调用）：所有停靠的等待者立即
// 返回，后续等待者拿新通道。对齐真实后端：只在放行性变更（记录删除、降级、
// 信号量槽位腾出）时广播，TryAcquire/Refresh 不广播。
func (d *fakeDriver) broadcast(key string) {
	if ch, ok := d.changed[key]; ok {
		close(ch)
		delete(d.changed, key)
	}
}

// WaitForChange 实现 glock.NotifyingDriver。
func (d *fakeDriver) WaitForChange(ctx context.Context, key string, maxWait time.Duration) error {
	d.mu.Lock()
	ch, ok := d.changed[key]
	if !ok {
		ch = make(chan struct{})
		d.changed[key] = ch
	}
	d.mu.Unlock()
	d.waiting.Add(1)
	defer d.waiting.Add(-1)
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	case <-ch:
		return nil
	}
}

// failNextRefresh 注入一次续期失败（模拟传输故障），消耗即清空。
func (d *fakeDriver) failNextRefresh(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextRefreshErr = err
}

// drop 删除记录（锁与信号量键空间一并），模拟后端数据消失（watchdog 丢失路径）。
func (d *fakeDriver) drop(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.recs, key)
	delete(d.sems, key)
	d.broadcast(key)
}

// Probe 实现 glock.ProbingDriver：只读快照，不做惰性清理（清理属获取路径）。
func (d *fakeDriver) Probe(_ context.Context, key string) ([]HoldInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	r := d.recs[key]
	if r == nil {
		return nil, nil
	}
	if r.mode == Write {
		if r.writer != nil && r.writer.expires.After(now) {
			return []HoldInfo{{Owner: r.owner, Mode: Write, Fence: r.writer.fence, Remaining: r.writer.expires.Sub(now)}}, nil
		}
		return nil, nil
	}
	owners := make([]string, 0, len(r.readers))
	for o := range r.readers {
		owners = append(owners, o)
	}
	sort.Strings(owners)
	var out []HoldInfo
	for _, o := range owners {
		if h := r.readers[o]; h.expires.After(now) {
			out = append(out, HoldInfo{Owner: o, Mode: Read, Fence: h.fence, Remaining: h.expires.Sub(now)})
		}
	}
	return out, nil
}

// holdsKey 报告 key 当前是否有存活记录（测试断言用）。
func (d *fakeDriver) holdsKey(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.live(d.recs[key], time.Now())
}
