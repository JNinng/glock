package glock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jninng/observ"
)

// metrics 是 Locker 级预创建的指标集合（observ 约定：构造期注册，热路径只打点）。
type metrics struct {
	acquired observ.Counter
	held     observ.Counter
	conflict observ.Counter
	lost     observ.Counter
	rejected observ.Counter
	acqDur   observ.Histogram
	enabled  bool
}

func newMetrics(m observ.Meter) metrics {
	if m == nil || m == observ.NoopMeter {
		return metrics{}
	}
	return metrics{
		acquired: m.NewCounter("glock_acquired_total", "成功获取次数"),
		held:     m.NewCounter("glock_acquire_held_total", "竞争失败（被他人持有）次数"),
		conflict: m.NewCounter("glock_acquire_conflict_total", "同 owner 模式冲突次数"),
		lost:     m.NewCounter("glock_lost_total", "持有丢失次数"),
		rejected: m.NewCounter("glock_release_rejected_total", "防误删拒绝次数"),
		acqDur:   m.NewHistogram("glock_acquire_duration_seconds", "阻塞获取耗时", []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30}),
		enabled:  true,
	}
}

// hold 是一次持有在本地侧的完整状态，Lock/ReadLock/MultiLock 成员都基于它。
type hold struct {
	drv     Driver
	met     *metrics
	log     observ.Logger
	lease   time.Duration
	wdOn    bool   // 获取时是否要求 watchdog
	fullKey string // 带命名空间前缀的 Key，用于后端操作
	pubKey  string // 使用者传入的原始 Key，用于 FencingTokens
	owner   string
	fence   uint64
	mode    Mode

	lost     chan struct{}
	lostOnce sync.Once
	dead     atomic.Bool // 已 Release 或已降级转移，本地持有终结
	wd       *watchdog   // 由 startWatchdog 设置；nil 表示无后台续约
}

func newHold(drv Driver, met *metrics, log observ.Logger, cfg acquireConfig, fullKey, pubKey string, mode Mode, fence uint64) *hold {
	return &hold{
		drv:     drv,
		met:     met,
		log:     log,
		lease:   cfg.lease,
		wdOn:    cfg.watchdog,
		fullKey: fullKey,
		pubKey:  pubKey,
		owner:   cfg.owner,
		fence:   fence,
		mode:    mode,
		lost:    make(chan struct{}),
	}
}

// Lost 在持有终结时关闭：丢失、正常释放或降级转移都会关闭。
// 用它等待“不再持有”信号；区分丢失与释放请看操作返回值。
func (h *hold) Lost() <-chan struct{} { return h.lost }

// OwnerToken 返回持有者身份，供业务持久化（WAL 恢复场景）。
func (h *hold) OwnerToken() string { return h.owner }

// FencingToken 返回本次获取事件的栅栏令牌，单调递增，交给下游存储
// 拒绝旧持有者的迟到写入。
func (h *hold) FencingToken() uint64 { return h.fence }

// closeLost 关闭 Lost 通道（幂等）。
func (h *hold) closeLost() { h.lostOnce.Do(func() { close(h.lost) }) }

// markLost 标记持有丢失：关闭 Lost、记一次日志与指标。
func (h *hold) markLost(reason error) {
	h.lostOnce.Do(func() {
		close(h.lost)
		if h.met.enabled {
			h.met.lost.Inc()
		}
		h.log.Log(context.Background(), slog.LevelWarn, "glock: hold lost",
			slog.String("key", h.pubKey),
			slog.String("mode", h.mode.String()),
			slog.String("owner", h.owner),
			slog.String(observ.AttrErrorType, reason.Error()))
	})
}

func (h *hold) ended() error {
	if h.dead.Load() {
		return fmt.Errorf("%w: hold already ended", ErrLost)
	}
	select {
	case <-h.lost:
		return fmt.Errorf("%w: hold already ended", ErrLost)
	default:
		return nil
	}
}

// refresh 手动续一个完整租约。
func (h *hold) refresh(ctx context.Context) error {
	if err := h.ended(); err != nil {
		return err
	}
	err := h.drv.Refresh(ctx, h.fullKey, h.mode, h.owner, h.fence, h.lease)
	if errors.Is(err, ErrNotOwner) {
		h.markLost(err)
		return fmt.Errorf("%w: %w", ErrLost, err)
	}
	if err != nil {
		h.markLost(err) // 传输失败：租约状态未知，保守按丢失处理
		return err
	}
	return nil
}

// release 释放一次计数：先停 watchdog（等待在途回调结束），再走后端。
func (h *hold) release(ctx context.Context) error {
	if !h.dead.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: hold already ended", ErrLost)
	}
	if h.wd != nil {
		h.wd.stopAndWait()
	}
	h.closeLost()
	err := h.drv.Release(ctx, h.fullKey, h.mode, h.owner, h.fence)
	if err != nil {
		if errors.Is(err, ErrNotOwner) {
			if h.met.enabled {
				h.met.rejected.Inc()
			}
			h.markLost(err)
			return fmt.Errorf("%w: %w", ErrLost, err)
		}
		h.markLost(err)
		return err
	}
	h.log.Log(ctx, slog.LevelInfo, "glock: released",
		slog.String("key", h.pubKey), slog.String("mode", h.mode.String()), slog.String("owner", h.owner))
	return nil
}

// releaseQuiet 供多锁回滚使用：尽力释放并记录失败，不返回错误。
func (h *hold) releaseQuiet(lease time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), lease)
	defer cancel()
	if err := h.release(ctx); err != nil {
		h.log.Log(ctx, slog.LevelError, "glock: rollback release failed",
			slog.String("key", h.pubKey), slog.String("owner", h.owner),
			slog.String(observ.AttrErrorType, err.Error()))
	}
}
