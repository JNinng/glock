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

// NotifyingDriver 是 Driver 的可选扩展：让阻塞获取在 Key 状态变化时被
// 唤醒，而不是纯轮询。信号是尽力而为的加速器：漏收（订阅生效前发生）、
// 重复、虚假唤醒都允许——BasicLocker 在任何返回后都会重走 TryAcquire，
// 轮询间隔（WithRetryInterval）是等待时长的上限。实现不得因通知机制故障
// 而以非 ctx 错误返回：故障时按剩余预算等满即可，保住轮询节律。
type NotifyingDriver interface {
	// WaitForChange 至多阻塞 maxWait：观测到 key 的相关变化（释放、过期、
	// 降级等，可能虚假）即提前返回 nil；maxWait 用尽返回 nil；ctx 结束
	// 返回 ctx 错误。
	WaitForChange(ctx context.Context, key string, maxWait time.Duration) error
}

// SemaphoreDriver 是 Driver 的可选扩展：N 上限资源池（并发槽位、连接配额）。
// 槽位即持有：租约、可重入、防误删、栅栏契约与锁完全一致——释放即腾出槽位，
// 过期即自动腾出（获取路径惰性清理），重入（同 Owner 再取）多占一个槽位且
// 沿用既有栅栏，只有全新持有者是获取事件、递增栅栏；同一 Owner 的全部槽位
// 共享一份租约（与读模式的共享记录一致）。信号量记录的键空间与锁隔离：
// 同名的锁与信号量互不相干。
//
// 实现约定：
//
//   - 容量在池首个获取时固定。记录尚有存活槽位时，容量声明与既有记录不符
//     返回 ErrCapacityMismatch（不可等待自愈）；全部槽位过期清空后允许重新
//     声明（记录消失即容量消失，与锁的过期接管同构）。
//   - BasicLocker 以 Mode.Semaphore 调用继承的 Refresh/Release 操作信号量
//     记录：实现 SemaphoreDriver 即必须让这两个方法按 Mode 路由。
type SemaphoreDriver interface {
	// TryAcquireSemaphore 尝试获取一个槽位（n 为容量上限）。
	// 返回 (fence, nil) 表示成功；存活槽位已满返回 (0, ErrHeld)；
	// 容量声明与既有记录不符返回 (0, ErrCapacityMismatch)。
	TryAcquireSemaphore(ctx context.Context, key string, n int, owner string, lease time.Duration) (uint64, error)
}
