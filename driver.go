package glock

import (
	"context"
	"time"
)

// Driver 是后端缝合点：每次调用是一次原子操作，编排（阻塞重试、watchdog、
// 多锁回滚、句柄生命周期）由 BasicLocker 统一完成。
//
// 实现约定：
//
//   - TryAcquire 的结果是获取事件或重入：全新获取（含从过期记录接管）必须
//     递增栅栏并返回新 Fencing Token；同 Owner 同模式且租约存活的重入返回
//     既有 Token 且不递增。租约已过期的记录不算存活：接管走全新获取。
//   - Refresh/Release/Downgrade 校验 Owner Token、Fencing Token 与模式，
//     任一不符或租约已过期返回 ErrNotOwner，绝不改动记录（防误删）。
//   - 所有比较以后端时间为准（单一时钟源）。
type Driver interface {
	// TryAcquire 尝试一次获取。
	// 返回 (fence, nil) 表示成功；竞争失败返回 (0, ErrHeld)；
	// 同 Owner 反模式（持写取读、持读取写）返回 (0, ErrModeConflict)。
	TryAcquire(ctx context.Context, key string, mode Mode, owner string, lease time.Duration) (uint64, error)

	// Refresh 把租约延长一个完整 lease。校验失败返回 ErrNotOwner。
	Refresh(ctx context.Context, key string, mode Mode, owner string, fence uint64, lease time.Duration) error

	// Release 释放一次持有计数，计数归零则删除记录。
	// 校验失败返回 ErrNotOwner。
	Release(ctx context.Context, key string, mode Mode, owner string, fence uint64) error

	// Downgrade 原子地把调用者的写持有（计数必须为 1）转为读持有，
	// Owner 与 Fencing Token 保持不变。计数大于 1 返回 ErrModeConflict，
	// 校验失败返回 ErrNotOwner。
	Downgrade(ctx context.Context, key, owner string, fence uint64) error
}
