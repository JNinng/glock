package glock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jninng/observ"
)

// Locker 是最小核心能力：互斥锁的阻塞与非阻塞获取。
// 后端 Locker 同时按支持度实现 RWLocker、MultiLocker，使用者用类型断言探测能力。
type Locker interface {
	// TryAcquire 非阻塞获取：一次尝试，竞争失败立即返回 ErrHeld。
	TryAcquire(ctx context.Context, key string, opts ...AcquireOption) (*Lock, error)

	// Acquire 阻塞获取：轮询直到成功或 ctx 结束。ctx 结束仍未获得时返回
	// 同时包装 ErrHeld 与 ctx 错误的错误。不保证公平与获得顺序。
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
	log    observ.Logger
	met    metrics
	prefix string
}

// NewBasicLocker 基于 Driver 构造 Locker。日志与指标默认取构造时刻的
// observ.DefaultLogger/DefaultMeter，可用 WithLogger/WithMeter 覆盖。
func NewBasicLocker(drv Driver, opts ...Option) *BasicLocker {
	cfg := newLockerConfig(opts)
	return &BasicLocker{
		drv:    drv,
		log:    cfg.log,
		met:    newMetrics(cfg.meter),
		prefix: cfg.prefix,
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
	cfg, err := newAcquireConfig(opts)
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
	start := time.Now()
	for {
		fence, err := l.drv.TryAcquire(ctx, fullKey, mode, cfg.owner, cfg.lease)
		if err == nil {
			if l.met.enabled {
				l.met.acquired.Inc()
				l.met.acqDur.Observe(time.Since(start).Seconds())
			}
			h := newHold(l.drv, &l.met, l.log, cfg, fullKey, key, mode, fence)
			l.log.Log(ctx, slog.LevelInfo, "glock: acquired",
				slog.String("key", key),
				slog.String("mode", mode.String()),
				slog.String("owner", cfg.owner),
				slog.Uint64("fence", fence),
				slog.Bool("watchdog", cfg.watchdog))
			return h, nil
		}
		if !errors.Is(err, ErrHeld) {
			// ErrModeConflict 等不可通过等待自愈，直接失败。
			if errors.Is(err, ErrModeConflict) && l.met.enabled {
				l.met.conflict.Inc()
			}
			return nil, err
		}
		if !blocking {
			if l.met.enabled {
				l.met.held.Inc()
			}
			return nil, err
		}
		select {
		case <-ctx.Done():
			if l.met.enabled {
				l.met.held.Inc()
			}
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
	cfg, err := newAcquireConfig(opts)
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
