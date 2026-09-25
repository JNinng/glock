package glock

import "errors"

var (
	// ErrHeld 表示 Key 被其他持有者持有（租约存活）。
	// TryAcquire 竞争失败时立即返回；Acquire 阻塞到 ctx 结束仍未获得时，
	// 返回同时包装 ErrHeld 与 ctx 错误的错误。
	ErrHeld = errors.New("glock: key held by another owner")

	// ErrLost 表示本地持有已终结——丢失（租约到期或续期失败）、已释放或
	// 已降级转移——句柄不再背书互斥。
	ErrLost = errors.New("glock: hold lost")

	// ErrNotOwner 表示变更操作被后端拒绝：Owner Token 或栅栏代际与当前记录不符
	//（防误删）。出现即意味着持有已丢失。
	ErrNotOwner = errors.New("glock: mutation rejected, not owner")

	// ErrModeConflict 表示同 Owner 在同一 Key 上的模式冲突：持写再取读、
	// 持读再取写，或写计数大于 1 时降级。
	ErrModeConflict = errors.New("glock: mode conflict")
)
