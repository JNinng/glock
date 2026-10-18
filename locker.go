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

	// Acquire 阻塞获取：直到成功或 ctx 结束。ctx 结束仍未获得时返回
	// 同时包装 ErrHeld 与 ctx 错误的错误。不保证公平与获得顺序。
	// Driver 实现 NotifyingDriver 时由释放信号唤醒，否则按间隔轮询；
	// 轮询间隔（WithRetryInterval）是任一实现的等待时长上限。
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
// WithMeter/WithDefaultLease/WithDefaultWatchdog 覆盖。
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

// tryAcquire 是单 Key 获取主循环（含重试），成功后按配置启动单锁 watchdog。
func (l *BasicLocker) tryAcquire(ctx context.Context, key string, mode Mode, opts []AcquireOption, blocking bool) (*hold, error) {
	cfg, err := l.newAcquireConfig(opts)
	if err != nil {
		return nil, err
	}
	h, err := l.acquireOne(ctx, key, mode, cfg, blocking)
	if err != nil {
		return nil, err
	}
	if h.wdOn {
		startWatchdog([]*hold{h})
	}
	return h, nil
}

// acquireOne 执行单 Key 获取循环，不启动 watchdog（多锁路径由整组统一启动）。
func (l *BasicLocker) acquireOne(ctx context.Context, key string, mode Mode, cfg acquireConfig, blocking bool) (*hold, error) {
	fullKey := l.prefix + key
	nd, wake := l.drv.(NotifyingDriver)
	start := time.Now()
	for {
		fence, err := l.drv.TryAcquire(ctx, fullKey, mode, cfg.owner, cfg.lease)
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
		// 释放唤醒（NotifyingDriver 可选）：信号优先，轮询兜底。信号是尽力
		// 而为的——漏收、多收、虚假唤醒都无害，任何返回后都重走 TryAcquire。
		if wake {
			if werr := nd.WaitForChange(ctx, fullKey, jitter(cfg.retryEvery)); werr == nil {
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
		case <-time.After(jitter(cfg.retryEvery)):
		}
	}
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
	for _, k := range sorted {
		h, err := l.acquireOne(ctx, k, Write, cfg, blocking)
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
