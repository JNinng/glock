package locktest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	glock "github.com/jninng/glock"
)

// InmemDriver 是 glock.Driver 的内存参考实现：验证契约语义、供无 Docker
// 环境跑一致性测试，也可用作 Driver 实现者的对照样例。非持久，进程外无互斥效果。
type InmemDriver struct {
	mu      sync.Mutex
	recs    map[string]*inmemRec
	sems    map[string]*inmemSem // 信号量记录：键空间与锁隔离
	counter uint64

	changed map[string]chan struct{} // 每键唤醒广播：变更时关闭并重置
}

type inmemRec struct {
	mode    glock.Mode
	owner   string // 仅写模式
	writer  *inmemHold
	readers map[string]*inmemHold
}

// inmemSem 是一个信号量池：cap 在首个获取时固定，holders 为 Owner → 槽位
// 计数与租约（同 Owner 的全部槽位共享一份租约）。
type inmemSem struct {
	cap     int
	holders map[string]*inmemHold
}

type inmemHold struct {
	count   int
	fence   uint64
	expires time.Time
}

// NewInmemDriver 构造内存 Driver。
func NewInmemDriver() *InmemDriver {
	return &InmemDriver{recs: map[string]*inmemRec{}, sems: map[string]*inmemSem{}, changed: map[string]chan struct{}{}}
}

// broadcast 关闭 key 的唤醒通道（须持 d.mu 调用）。对齐真实后端：只在
// 放行性变更（记录删除、降级、信号量槽位腾出）时广播，TryAcquire/Refresh
// 不广播。
func (d *InmemDriver) broadcast(key string) {
	if ch, ok := d.changed[key]; ok {
		close(ch)
		delete(d.changed, key)
	}
}

// WaitForChange 实现 glock.NotifyingDriver。
func (d *InmemDriver) WaitForChange(ctx context.Context, key string, maxWait time.Duration) error {
	d.mu.Lock()
	ch, ok := d.changed[key]
	if !ok {
		ch = make(chan struct{})
		d.changed[key] = ch
	}
	d.mu.Unlock()
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

// live 报告记录存在且至少一份持有租约未过期；整体过期不算存活，可被接管。
func (d *InmemDriver) live(r *inmemRec, now time.Time) bool {
	if r == nil {
		return false
	}
	if r.mode == glock.Write {
		return r.writer != nil && r.writer.expires.After(now)
	}
	for _, h := range r.readers {
		if h.expires.After(now) {
			return true
		}
	}
	return false
}

// purge 清除记录中已过期的持有（获取路径的惰性清理）。
func (d *InmemDriver) purge(r *inmemRec, now time.Time) {
	if r.mode == glock.Write {
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

// TryAcquire 实现 glock.Driver。
func (d *InmemDriver) TryAcquire(_ context.Context, key string, mode glock.Mode, owner string, lease time.Duration) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	r := d.recs[key]
	if r != nil {
		d.purge(r, now)
	}
	if d.live(r, now) {
		switch {
		case mode == glock.Write && r.mode == glock.Write && r.owner == owner:
			r.writer.count++
			r.writer.expires = now.Add(lease)
			return r.writer.fence, nil
		case mode == glock.Write && r.mode == glock.Write:
			return 0, fmt.Errorf("inmem: %w", glock.ErrHeld)
		case mode == glock.Write && r.mode == glock.Read:
			if _, mine := r.readers[owner]; mine {
				return 0, fmt.Errorf("inmem: %w", glock.ErrModeConflict) // 持读取写
			}
			return 0, fmt.Errorf("inmem: %w", glock.ErrHeld)
		case mode == glock.Read && r.mode == glock.Write:
			if r.owner == owner {
				return 0, fmt.Errorf("inmem: %w", glock.ErrModeConflict) // 持写取读
			}
			return 0, fmt.Errorf("inmem: %w", glock.ErrHeld)
		default:
			if h, ok := r.readers[owner]; ok {
				h.count++
				h.expires = now.Add(lease)
				return h.fence, nil
			}
			d.counter++
			r.readers[owner] = &inmemHold{count: 1, fence: d.counter, expires: now.Add(lease)}
			return d.counter, nil
		}
	}
	d.counter++
	nr := &inmemRec{mode: mode}
	if mode == glock.Write {
		nr.owner = owner
		nr.writer = &inmemHold{count: 1, fence: d.counter, expires: now.Add(lease)}
	} else {
		nr.readers = map[string]*inmemHold{owner: {count: 1, fence: d.counter, expires: now.Add(lease)}}
	}
	d.recs[key] = nr
	return d.counter, nil
}

func (d *InmemDriver) holdOf(r *inmemRec, mode glock.Mode, owner string) *inmemHold {
	if mode == glock.Read {
		return r.readers[owner]
	}
	if r.owner != owner {
		return nil
	}
	return r.writer
}

// semLiveOf 取信号量池中 owner 的存活槽位记录，无效返回 nil。
func (d *InmemDriver) semLiveOf(r *inmemSem, owner string, fence uint64, now time.Time) *inmemHold {
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
func (d *InmemDriver) TryAcquireSemaphore(_ context.Context, key string, n int, owner string, lease time.Duration) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	r := d.sems[key]
	if r != nil {
		d.semPurge(r, now)
	}
	if d.semLive(r, now) {
		if r.cap != n {
			return 0, fmt.Errorf("inmem: %w", glock.ErrCapacityMismatch)
		}
		inUse := 0
		for _, h := range r.holders {
			inUse += h.count
		}
		if inUse >= n {
			return 0, fmt.Errorf("inmem: %w", glock.ErrHeld)
		}
		if h, ok := r.holders[owner]; ok {
			h.count++
			h.expires = now.Add(lease)
			return h.fence, nil // 重入多占槽位，沿用代际
		}
	} else {
		// 全新池（记录不存在或槽位全部过期）：容量在此固定。
		r = &inmemSem{cap: n, holders: map[string]*inmemHold{}}
		d.sems[key] = r
	}
	d.counter++
	r.holders[owner] = &inmemHold{count: 1, fence: d.counter, expires: now.Add(lease)}
	return d.counter, nil
}

func (d *InmemDriver) semLive(r *inmemSem, now time.Time) bool {
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

func (d *InmemDriver) semPurge(r *inmemSem, now time.Time) {
	for o, h := range r.holders {
		if !h.expires.After(now) {
			delete(r.holders, o)
		}
	}
}

// Refresh 实现 glock.Driver：信号量槽位（Mode.Semaphore）路由到信号量记录。
func (d *InmemDriver) Refresh(_ context.Context, key string, mode glock.Mode, owner string, fence uint64, lease time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if mode == glock.Semaphore {
		r := d.sems[key]
		if d.semLiveOf(r, owner, fence, now) == nil {
			return fmt.Errorf("inmem: %w", glock.ErrNotOwner)
		}
		r.holders[owner].expires = now.Add(lease)
		return nil
	}
	r := d.recs[key]
	if r == nil || r.mode != mode || !d.live(r, now) {
		return fmt.Errorf("inmem: %w", glock.ErrNotOwner)
	}
	h := d.holdOf(r, mode, owner)
	if h == nil || h.fence != fence || !h.expires.After(now) {
		return fmt.Errorf("inmem: %w", glock.ErrNotOwner)
	}
	h.expires = now.Add(lease)
	return nil
}

// Release 实现 glock.Driver：信号量槽位释放即腾出（广播唤醒等待者）。
func (d *InmemDriver) Release(_ context.Context, key string, mode glock.Mode, owner string, fence uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if mode == glock.Semaphore {
		r := d.sems[key]
		if d.semLiveOf(r, owner, fence, now) == nil {
			return fmt.Errorf("inmem: %w", glock.ErrNotOwner)
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
		return fmt.Errorf("inmem: %w", glock.ErrNotOwner)
	}
	h := d.holdOf(r, mode, owner)
	if h == nil || h.fence != fence || !h.expires.After(now) {
		return fmt.Errorf("inmem: %w", glock.ErrNotOwner)
	}
	h.count--
	if h.count <= 0 {
		if mode == glock.Read {
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

// Downgrade 实现 glock.Driver。
func (d *InmemDriver) Downgrade(_ context.Context, key, owner string, fence uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	r := d.recs[key]
	if r == nil || r.mode != glock.Write || r.owner != owner || !d.live(r, now) || r.writer.fence != fence {
		return fmt.Errorf("inmem: %w", glock.ErrNotOwner)
	}
	if r.writer.count > 1 {
		return fmt.Errorf("inmem: %w", glock.ErrModeConflict)
	}
	r.mode = glock.Read
	r.owner = ""
	r.readers = map[string]*inmemHold{owner: {count: 1, fence: r.writer.fence, expires: r.writer.expires}}
	r.writer = nil
	d.broadcast(key) // 写降读放行读者
	return nil
}

// Drop 删除记录（锁与信号量键空间一并），模拟后端数据消失。
func (d *InmemDriver) Drop(_ context.Context, key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.recs, key)
	delete(d.sems, key)
	d.broadcast(key)
	return nil
}

// Probe 实现 glock.ProbingDriver：只读快照，不做惰性清理（清理属获取路径）。
func (d *InmemDriver) Probe(_ context.Context, key string) ([]glock.HoldInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	r := d.recs[key]
	if r == nil {
		return nil, nil
	}
	if r.mode == glock.Write {
		if r.writer != nil && r.writer.expires.After(now) {
			return []glock.HoldInfo{{Owner: r.owner, Mode: glock.Write, Fence: r.writer.fence, Remaining: r.writer.expires.Sub(now)}}, nil
		}
		return nil, nil
	}
	owners := make([]string, 0, len(r.readers))
	for o := range r.readers {
		owners = append(owners, o)
	}
	sort.Strings(owners)
	var out []glock.HoldInfo
	for _, o := range owners {
		if h := r.readers[o]; h.expires.After(now) {
			out = append(out, glock.HoldInfo{Owner: o, Mode: glock.Read, Fence: h.fence, Remaining: h.expires.Sub(now)})
		}
	}
	return out, nil
}
