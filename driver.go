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
//   - 过期比较基于单一时钟源：能用后端时间的实现必须用后端时间
//     （如 SQL now()）；后端不可取时的实现可用客户端时间，但须在包文档
//     中写明对部署时钟的要求（如 redisc 要求 NTP 对时）。
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

// ProbingDriver 是 Driver 的可选扩展：只读状态内省。实现绝不改动任何记录、
// 绝不影响租约（多次调用与零次调用等价），失败也不改变持有状态。
// BasicLocker 经类型断言探测此接口，对外透出 ProbingLocker 能力。
type ProbingDriver interface {
	// Probe 返回 Key 当前全部存活持有，按 Owner 排序；记录不存在或全部
	// 租约已过期时返回空列表而非错误。过期残余的清理仍属获取路径，Probe 不做。
	Probe(ctx context.Context, key string) ([]HoldInfo, error)
}

// HoldInfo 是 Probe 观测到的一份存活持有快照。
type HoldInfo struct {
	Owner     string        // Owner Token（持有者身份）
	Mode      Mode          // 读或写
	Fence     uint64        // 当前 Fencing Token
	Remaining time.Duration // 剩余租约，恒为正（只报存活持有）
}
