package glock

import "context"

// SemaphoreLock 是信号量槽位的句柄，由 SemaphoreLocker.AcquireSemaphore/
// TryAcquireSemaphore 返回。一个句柄占一个槽位；同 Owner Token 的多个句柄
// 各占一个槽位、共享一份栅栏与租约。
type SemaphoreLock struct {
	h *hold
}

var _ SemaphoreLocker = (*BasicLocker)(nil)

// Release 释放一个槽位（腾出配额）。持有已终结，或被防误删拒绝（租约已
// 过期、记录已被接管——错误内含 ErrNotOwner），均返回包装 ErrLost 的错误；
// 任何情况下绝不误删他人的记录。
func (s *SemaphoreLock) Release(ctx context.Context) error { return s.h.release(ctx) }

// Refresh 手动续一个完整租约（作用于本 Owner 在该池的全部槽位，watchdog
// 开启与否都可用）。
func (s *SemaphoreLock) Refresh(ctx context.Context) error { return s.h.refresh(ctx) }

// Lost 在持有终结（丢失或释放）时关闭。
func (s *SemaphoreLock) Lost() <-chan struct{} { return s.h.Lost() }

// OwnerToken 返回持有者身份，供业务持久化（WAL 恢复场景）。
func (s *SemaphoreLock) OwnerToken() string { return s.h.OwnerToken() }

// FencingToken 返回本次获取事件的栅栏令牌：全新持有者递增，重入沿用。
func (s *SemaphoreLock) FencingToken() uint64 { return s.h.FencingToken() }
