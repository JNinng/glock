package glock

import (
	"context"
	"fmt"
)

// ProbingLocker 声明状态内省能力（只读诊断）。后端 Locker 按支持度实现，
// 使用者用类型断言探测；基于 BasicLocker 的 Locker 在 Driver 实现
// ProbingDriver 时自动具备，否则运行时返回 ErrUnsupported。
type ProbingLocker interface {
	// Probe 返回 Key 当前全部存活持有，按 Owner 排序，回答"这个 key 被
	// 谁锁着、还剩多久"。读模式下同一 Key 可有多个持有者。
	Probe(ctx context.Context, key string) ([]HoldInfo, error)
}

var _ ProbingLocker = (*BasicLocker)(nil)

// Probe 实现 ProbingLocker：把使用者 Key 加命名空间前缀后转交 Driver。
// Driver 未实现 ProbingDriver 时返回 ErrUnsupported。
func (l *BasicLocker) Probe(ctx context.Context, key string) ([]HoldInfo, error) {
	pd, ok := l.drv.(ProbingDriver)
	if !ok {
		return nil, fmt.Errorf("glock: probe %q: %w", key, ErrUnsupported)
	}
	return pd.Probe(ctx, l.prefix+key)
}
