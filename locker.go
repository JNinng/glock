package glock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"
)

// Locker 是最小核心能力：互斥锁的阻塞与非阻塞获取。
// 后端 Locker 同时按支持度实现 RWLocker、MultiLocker，使用者用类型断言探测能力。
type Locker interface {
	// TryAcquire 非阻塞获取：一次尝试，竞争失败立即返回 ErrHeld。
	TryAcquire(ctx context.Context, key string, opts ...AcquireOption) (*Lock, error)

	// Acquire 阻塞获取：直到成功、ctx 结束或重试上限（WithMaxRetries）用尽。
	// ctx 结束仍未获得时返回同时包装 ErrHeld 与 ctx 错误的错误；重试上限
	// 用尽返回同时包装 ErrHeld 与 ErrExhausted 的错误。不保证公平与获得
	// 顺序。Driver 实现 NotifyingDriver 时由释放信号唤醒，否则按间隔轮询；
	// 轮询间隔（WithRetryInterval 固定或 WithBackoff 退避）是任一实现的
	// 等待时长上限。
	Acquire(ctx context.Context, key string, opts ...AcquireOption) (*Lock, error)
}

// RWLocker 声明读写锁能力。
type RWLocker interface {
	Locker
	// TryAcquireRead 非阻塞获取读锁。
	TryAcquireRead(ctx context.Context, key string, opts ...AcquireOption) (*ReadLock, error)
	// AcquireRead 阻塞获取读锁。
	AcquireRead(ctx context.Context, key string, opts ...AcquireOption) (*ReadLock, error)
}

// MultiLocker 声明多锁能力（仅互斥模式）。
type MultiLocker interface {
	Locker
	// TryAcquireAll 非阻塞全量获取：任一 Key 竞争失败即回滚已获取的并返回 ErrHeld。
	TryAcquireAll(ctx context.Context, keys []string, opts ...AcquireOption) (*MultiLock, error)
	// AcquireAll 阻塞全量获取：按 Key 字典序逐个获取以避免交叉死锁，
	// 任一 Key 阻塞等待时已获取的 Key 仍在计租约。
	AcquireAll(ctx context.Context, keys []string, opts ...AcquireOption) (*MultiLock, error)
}

// SemaphoreLocker 声明信号量能力（N 上限资源池）。每个槽位是一次持有：
// 有界租约、可重入、防误删、Fencing Token 与锁一致。Driver 实现
// SemaphoreDriver 时 BasicLocker 自动具备，否则运行时返回 ErrUnsupported。
type SemaphoreLocker interface {
	Locker
	// TryAcquireSemaphore 非阻塞获取一个槽位（n 为容量上限，容量在池首个
	// 获取时固定）：槽位已满立即返回 ErrHeld；容量声明与既有记录不符返回
	// ErrCapacityMismatch。
	TryAcquireSemaphore(ctx context.Context, key string, n int, opts ...AcquireOption) (*SemaphoreLock, error)
	// AcquireSemaphore 阻塞获取一个槽位：直到成功、ctx 结束或重试上限
	// （WithMaxRetries）用尽。放弃契约同 Acquire：ctx 结束返回包装
	// ErrHeld+ctx 错误的错误，上限用尽返回包装 ErrHeld+ErrExhausted 的
	// 错误。重试与唤醒编排同 Acquire。
	AcquireSemaphore(ctx context.Context, key string, n int, opts ...AcquireOption) (*SemaphoreLock, error)
}

// BasicLocker 基于 Driver 提供全部编排，实现 Locker、RWLocker、MultiLocker。
// 后端模块用它对外暴露统一的 Locker 类型。
type BasicLocker struct {
	drv    Driver
	log    *slog.Logger
	met    metrics
	prefix string

	defLease    time.Duration // Locker 级默认租约
	defWatchdog bool          // Locker 级默认 watchdog
}

// NewBasicLocker 基于 Driver 构造 Locker。日志默认静默（discard handler），
// 指标默认经由 otel 全局 MeterProvider（未配置即 noop，SetMeterProvider
// 后自动接入），租约默认 DefaultLease、无后台续期，可用 WithLogger/
// WithMeter/WithNamespace/WithDefaultLease/WithDefaultWatchdog 覆盖。
func NewBasicLocker(drv Driver, opts ...Option) *BasicLocker {
	cfg := newLockerConfig(opts)
	return &BasicLocker{
		drv:         drv,
		log:         cfg.log,
		met:         newMetrics(cfg.meter),
		prefix:      cfg.prefix,
		defLease:    cfg.defaultLease,
		defWatchdog: cfg.defaultWatchdog,
	}
}

func (l *BasicLocker) TryAcquire(ctx context.Context, key string, opts ...AcquireOption) (*Lock, error) {
	h, err := l.tryAcquire(ctx, key, Write, opts, false)
	if err != nil {
		return nil, err
	}
	return &Lock{h: h}, nil
}

func (l *BasicLocker) Acquire(ctx context.Context, key string, opts ...AcquireOption) (*Lock, error) {
	h, err := l.tryAcquire(ctx, key, Write, opts, true)
	if err != nil {
		return nil, err
	}
	return &Lock{h: h}, nil
}

func (l *BasicLocker) TryAcquireRead(ctx context.Context, key string, opts ...AcquireOption) (*ReadLock, error) {
	h, err := l.tryAcquire(ctx, key, Read, opts, false)
	if err != nil {
		return nil, err
	}
	return &ReadLock{h: h}, nil
}

func (l *BasicLocker) AcquireRead(ctx context.Context, key string, opts ...AcquireOption) (*ReadLock, error) {
	h, err := l.tryAcquire(ctx, key, Read, opts, true)
	if err != nil {
		return nil, err
	}
	return &ReadLock{h: h}, nil
}

// tryAcquire 完成单 Key 获取（重试循环在 acquireOne），成功后按配置启动单锁 watchdog。
func (l *BasicLocker) tryAcquire(ctx context.Context, key string, mode Mode, opts []AcquireOption, blocking bool) (*hold, error) {
	cfg, err := l.newAcquireConfig(opts)
	if err != nil {
		return nil, err
	}
	try := func(ctx context.Context, fullKey, owner string, lease time.Duration) (uint64, error) {
		return l.drv.TryAcquire(ctx, fullKey, mode, owner, lease)
	}
	h, err := l.acquireOne(ctx, key, mode, cfg, blocking, try)
	if err != nil {
		return nil, err
	}
	if h.wdOn {
		startWatchdog([]*hold{h})
	}
	return h, nil
}

// trySemaphore 完成信号量槽位获取：容量校验与能力探测（缺失返回
// ErrUnsupported）后，槽位走与锁相同的获取循环与 watchdog 编排。
func (l *BasicLocker) trySemaphore(ctx context.Context, key string, n int, opts []AcquireOption, blocking bool) (*hold, error) {
	if n <= 0 {
		return nil, fmt.Errorf("glock: semaphore capacity must be positive, got %d", n)
	}
	sd, ok := l.drv.(SemaphoreDriver)
	if !ok {
		return nil, fmt.Errorf("glock: acquire semaphore %q: %w", key, ErrUnsupported)
	}
	cfg, err := l.newAcquireConfig(opts)
	if err != nil {
		return nil, err
	}
	try := func(ctx context.Context, fullKey, owner string, lease time.Duration) (uint64, error) {
		return sd.TryAcquireSemaphore(ctx, fullKey, n, owner, lease)
	}
	h, err := l.acquireOne(ctx, key, Semaphore, cfg, blocking, try)
	if err != nil {
		return nil, err
	}
	if h.wdOn {
		startWatchdog([]*hold{h})
	}
	return h, nil
}

// acquireTry 抽象一次获取尝试：锁走 Driver.TryAcquire，信号量走
// SemaphoreDriver.TryAcquireSemaphore（n 闭包绑定）。编排循环共用。
type acquireTry func(ctx context.Context, fullKey, owner string, lease time.Duration) (uint64, error)

// acquireOne 执行单 Key 获取循环，不启动 watchdog（多锁路径由整组统一启动）。
func (l *BasicLocker) acquireOne(ctx context.Context, key string, mode Mode, cfg acquireConfig, blocking bool, try acquireTry) (*hold, error) {
	fullKey := l.prefix + key
	nd, wake := l.drv.(NotifyingDriver)
	start := time.Now()
	failures := 0 // 已失败尝试数：初次获取 + WithMaxRetries 次重试
	for {
		fence, err := try(ctx, fullKey, cfg.owner, cfg.lease)
		if err == nil {
			l.met.acquired.Add(ctx, 1)
			l.met.acqDur.Record(ctx, time.Since(start).Seconds())
			h := newHold(l.drv, &l.met, l.log, cfg, fullKey, key, mode, fence)
			l.log.InfoContext(ctx, "glock: acquired",
				slog.String("key", key),
				slog.String("mode", mode.String()),
				slog.String("owner", cfg.owner),
				slog.Uint64("fence", fence),
				slog.Bool("watchdog", cfg.watchdog))
			return h, nil
		}
		if !errors.Is(err, ErrHeld) {
			// ErrModeConflict 等不可通过等待自愈，直接失败。
			if errors.Is(err, ErrModeConflict) {
				l.met.conflict.Add(ctx, 1)
			}
			return nil, err
		}
		if !blocking {
			l.met.held.Add(ctx, 1)
			return nil, err
		}
		failures++
		// 重试上限（WithMaxRetries）：ctx 之外的显式放弃路径，放弃契约与
		// ctx 路径同构——同时包装 ErrHeld 与放弃原因，放弃前不再等待。
		if cfg.maxRetries >= 0 && failures > cfg.maxRetries {
			l.met.held.Add(ctx, 1)
			return nil, fmt.Errorf("%w: %w", ErrHeld, ErrExhausted)
		}
		wait := cfg.waitAt(failures)
		// 释放唤醒（NotifyingDriver 可选）：信号优先，轮询兜底。信号是尽力
		// 而为的——漏收、多收、虚假唤醒都无害，任何返回后都重走 TryAcquire。
		if wake {
			if werr := nd.WaitForChange(ctx, fullKey, wait); werr == nil {
				continue
			} else if ctx.Err() != nil {
				l.met.held.Add(ctx, 1)
				return nil, fmt.Errorf("%w: %w", ErrHeld, ctx.Err())
			}
			// 契约外错误（不该发生）：落到下方纯轮询等待，保住节律与活性。
		}
		select {
		case <-ctx.Done():
			l.met.held.Add(ctx, 1)
			return nil, fmt.Errorf("%w: %w", ErrHeld, ctx.Err())
		case <-time.After(wait):
		}
	}
}

func (l *BasicLocker) TryAcquireSemaphore(ctx context.Context, key string, n int, opts ...AcquireOption) (*SemaphoreLock, error) {
	h, err := l.trySemaphore(ctx, key, n, opts, false)
	if err != nil {
		return nil, err
	}
	return &SemaphoreLock{h: h}, nil
}

func (l *BasicLocker) AcquireSemaphore(ctx context.Context, key string, n int, opts ...AcquireOption) (*SemaphoreLock, error) {
	h, err := l.trySemaphore(ctx, key, n, opts, true)
	if err != nil {
		return nil, err
	}
	return &SemaphoreLock{h: h}, nil
}

func (l *BasicLocker) TryAcquireAll(ctx context.Context, keys []string, opts ...AcquireOption) (*MultiLock, error) {
	return l.acquireAll(ctx, keys, opts, false)
}

func (l *BasicLocker) AcquireAll(ctx context.Context, keys []string, opts ...AcquireOption) (*MultiLock, error) {
	return l.acquireAll(ctx, keys, opts, true)
}

func (l *BasicLocker) acquireAll(ctx context.Context, keys []string, opts []AcquireOption, blocking bool) (*MultiLock, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("glock: AcquireAll requires at least one key")
	}
	cfg, err := l.newAcquireConfig(opts)
	if err != nil {
		return nil, err
	}
	sorted := sortedUnique(keys)
	holds := make([]*hold, 0, len(sorted))
	try := func(ctx context.Context, fullKey, owner string, lease time.Duration) (uint64, error) {
		return l.drv.TryAcquire(ctx, fullKey, Write, owner, lease)
	}
	for _, k := range sorted {
		h, err := l.acquireOne(ctx, k, Write, cfg, blocking, try)
		if err != nil {
			// 回滚已获取的（逆序），让竞争者看到全有或全无。
			for i := len(holds) - 1; i >= 0; i-- {
				holds[i].releaseQuiet(cfg.lease)
			}
			return nil, err
		}
		holds = append(holds, h)
	}
	if cfg.watchdog {
		startWatchdog(holds)
	}
	return newMultiLock(holds), nil
}

func sortedUnique(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
