package glock

import "errors"

var (
	// ErrHeld 表示资源不可用：互斥锁被其他持有者持有（租约存活），或信号量
	// 的存活槽位已满。TryAcquire/TryAcquireSemaphore 竞争失败时立即返回；
	// Acquire/AcquireSemaphore 阻塞到 ctx 结束仍未获得时，返回同时包装
	// ErrHeld 与 ctx 错误的错误。
	ErrHeld = errors.New("glock: key held by another owner")

	// ErrLost 表示本地持有已终结——丢失（租约到期或续期失败）、已释放或
	// 已降级转移——句柄不再背书互斥。
	ErrLost = errors.New("glock: hold lost")

	// ErrNotOwner 表示变更操作被后端拒绝：Owner Token、栅栏代际或模式与当前
	// 记录不符，或租约已过期（防误删）。出现即意味着持有已丢失。
	ErrNotOwner = errors.New("glock: mutation rejected, not owner")

	// ErrModeConflict 表示同 Owner 在同一 Key 上的模式冲突：持写再取读、
	// 持读再取写，或写计数大于 1 时降级。
	ErrModeConflict = errors.New("glock: mode conflict")

	// ErrCapacityMismatch 表示信号量的容量声明与既有记录不一致。容量在池
	// 首个获取时固定，尚有存活槽位时不得改声明；全部槽位过期清空后可重新
	// 声明。此错误不可通过等待自愈，阻塞获取也立即失败。
	ErrCapacityMismatch = errors.New("glock: semaphore capacity mismatch")

	// ErrUnsupported 表示 Driver 未实现被调用的可选能力（如状态内省、信号量）。
	// v1 核心契约（Locker/RWLocker/MultiLocker）不涉及此错误。
	ErrUnsupported = errors.New("glock: capability not supported by this driver")
)
