package glock

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// withLockReleaseTimeout 是 WithLock 自动释放的时间预算：fn 返回后调用方 ctx
// 可能已取消，释放不能与之同生共死；同时避免后端失联时无限挂起。
const withLockReleaseTimeout = 5 * time.Second

// WithLock 封装最常用的样板：阻塞获取（Acquire）→ 执行 fn → 释放。
//
// fn 收到本次持有的句柄与一个派生 ctx。持有丢失（续期失败、租约被接管）时
// 派生 ctx 被取消，context.Cause(fnCtx) 为 ErrLost，fn 应尽快返回；调用方
// 取消则保持原 cause。能否及时发现丢失取决于 watchdog——固定租约模式只在
// 释放时暴露 ErrLost。
//
// fn 返回后总会释放，释放不依赖调用方 ctx（即使已取消，使用独立的 5 秒
// 预算）；fn 不应自行 Release/Downgrade 句柄。fn 或释放任一失败时错误用
// errors.Join 合并：fn 忽略取消、越过丢失继续工作时，返回值包含 ErrLost，
// 调用方应据此判断本次临界区结果可能与接管者交错。
func (l *BasicLocker) WithLock(ctx context.Context, key string, fn func(ctx context.Context, lock *Lock) error, opts ...AcquireOption) (err error) {
	lk, err := l.Acquire(ctx, key, opts...)
	if err != nil {
		return err
	}

	fnCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	// 丢失信号 → 取消 fnCtx，cause 标记为 ErrLost。
	stop := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-lk.Lost():
			cancel(ErrLost)
		case <-stop:
		}
	}()

	// fn 返回后先停掉 watcher 并检查 Lost，再走释放——释放本身会关闭 Lost，
	// 之后无法区分“fn 期间丢失”与“正常释放”。
	defer func() {
		close(stop)
		<-watcherDone
		lostBeforeRelease := false
		select {
		case <-lk.Lost():
			lostBeforeRelease = true
		default:
		}
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), withLockReleaseTimeout)
		defer releaseCancel()
		if relErr := lk.Release(releaseCtx); relErr != nil {
			err = errors.Join(err, relErr)
		}
		// 释放成功也要上报 fn 期间已发生的丢失（如 watchdog 因传输错误
		// 误判、记录实际仍在）。释放被防误删拒绝时错误链已含 ErrLost，
		// 不重复包装。
		if lostBeforeRelease && !errors.Is(err, ErrLost) {
			err = errors.Join(err, fmt.Errorf("%w: body returned after loss", ErrLost))
		}
	}()

	return fn(fnCtx, lk)
}
