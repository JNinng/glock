package glock

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// fakeDriver 是遵守 Driver 契约的内存实现，仅用于编排层单测。
// 完整契约测试见 locktest 模块。
type fakeDriver struct {
	mu      sync.Mutex
	recs    map[string]*fakeRec
	counter uint64
	refresh int // Refresh 成功次数
}

type fakeRec struct {
	mode    Mode
	owner   string // 仅 Write 模式使用
	writer  *fakeHoldRec
	readers map[string]*fakeHoldRec // 仅 Read 模式使用
}

type fakeHoldRec struct {
	count   int
	fence   uint64
	expires time.Time
}

func newFakeDriver() *fakeDriver { return &fakeDriver{recs: map[string]*fakeRec{}} }

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

func (d *fakeDriver) Refresh(_ context.Context, key string, mode Mode, owner string, fence uint64, lease time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
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
			}
		} else {
			delete(d.recs, key)
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
	return nil
}

// drop 删除记录，模拟后端数据消失（watchdog 丢失路径）。
func (d *fakeDriver) drop(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.recs, key)
}

// holdsKey 报告 key 当前是否有存活记录（测试断言用）。
func (d *fakeDriver) holdsKey(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.live(d.recs[key], time.Now())
}
