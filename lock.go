package glock

import (
	"context"
	"errors"
	"fmt"
)

// Lock 是互斥（写）持有的句柄，由 Locker.Acquire/TryAcquire 返回。
type Lock struct {
	h *hold
}

// Release 释放一次持有计数。持有已终结（重复释放、已降级转移）返回包装
// ErrLost 的错误；锁已被他人接管返回包装 ErrNotOwner 的错误；丢失后绝不误删。
func (l *Lock) Release(ctx context.Context) error { return l.h.release(ctx) }

// Refresh 手动续一个完整租约（watchdog 开启与否都可用）。
func (l *Lock) Refresh(ctx context.Context) error { return l.h.refresh(ctx) }

// Downgrade 原子地把写持有转为读持有：不暴露无锁窗口，Fencing Token 不变。
// 旧句柄随之终结（后续操作返回 ErrLost），新 ReadLock 承接同一持有。
// 写计数大于 1（有重入）时返回 ErrModeConflict，应先释放一层。
func (l *Lock) Downgrade(ctx context.Context) (*ReadLock, error) {
	h := l.h
	if err := h.ended(); err != nil {
		return nil, err
	}
	err := h.drv.Downgrade(ctx, h.fullKey, h.owner, h.fence)
	if err != nil {
		if errors.Is(err, ErrNotOwner) {
			h.markLost(err)
			return nil, fmt.Errorf("%w: %w", ErrLost, err)
		}
		return nil, err // ErrModeConflict 或传输错误，持有状态不变
	}
	// 持有转移：旧句柄终结，watchdog 停止后由新读句柄重启。
	if h.wd != nil {
		h.wd.stopAndWait()
	}
	if !h.dead.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("%w: hold already ended", ErrLost)
	}
	h.closeLost()
	next := &hold{
		drv: h.drv, met: h.met, log: h.log,
		lease: h.lease, wdOn: h.wdOn,
		fullKey: h.fullKey, pubKey: h.pubKey,
		owner: h.owner, fence: h.fence, mode: Read,
		lost: make(chan struct{}),
	}
	if next.wdOn {
		startWatchdog([]*hold{next})
	}
	return &ReadLock{h: next}, nil
}

// Lost 在持有终结（丢失、释放或降级转移）时关闭。
func (l *Lock) Lost() <-chan struct{} { return l.h.Lost() }

// OwnerToken 返回持有者身份，供业务持久化（WAL 恢复场景）。
func (l *Lock) OwnerToken() string { return l.h.OwnerToken() }

// FencingToken 返回本次获取事件的栅栏令牌（强制契约，单调递增）。
func (l *Lock) FencingToken() uint64 { return l.h.FencingToken() }

// ReadLock 是共享（读）持有的句柄。无 Downgrade（升级方向明确不做）。
type ReadLock struct {
	h *hold
}

// Release 释放一次持有计数，语义同 Lock.Release。
func (r *ReadLock) Release(ctx context.Context) error { return r.h.release(ctx) }

// Refresh 手动续一个完整租约。
func (r *ReadLock) Refresh(ctx context.Context) error { return r.h.refresh(ctx) }

// Lost 在持有终结时关闭。
func (r *ReadLock) Lost() <-chan struct{} { return r.h.Lost() }

// OwnerToken 返回持有者身份。
func (r *ReadLock) OwnerToken() string { return r.h.OwnerToken() }

// FencingToken 返回本次获取事件的栅栏令牌。
func (r *ReadLock) FencingToken() uint64 { return r.h.FencingToken() }
