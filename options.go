package glock

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// 默认值。租约必须有界；watchdog 间隔取租约的三分之一，轮询与续期都带 ±20% 抖动。
const (
	DefaultLease         = 30 * time.Second
	DefaultRetryInterval = 200 * time.Millisecond

	watchdogIntervalDiv = 3
	jitterRatio         = 0.2
)

// AcquireOption 配置单次获取。
type AcquireOption func(*acquireConfig)

type acquireConfig struct {
	owner      string
	lease      time.Duration
	watchdog   bool
	retryEvery time.Duration
}

func newAcquireConfig(opts []AcquireOption) (acquireConfig, error) {
	c := acquireConfig{lease: DefaultLease, retryEvery: DefaultRetryInterval}
	for _, o := range opts {
		o(&c)
	}
	if c.lease <= 0 {
		return c, fmt.Errorf("glock: lease must be positive, got %s", c.lease)
	}
	if c.retryEvery <= 0 {
		return c, fmt.Errorf("glock: retry interval must be positive, got %s", c.retryEvery)
	}
	if c.owner == "" {
		c.owner = NewOwnerToken()
	}
	return c, nil
}

// WithOwner 指定 Owner Token（持有者身份），默认自动生成。
// 崩溃恢复场景：业务把 token 持久化（如写入 WAL），恢复后以同一 token 重新
// Acquire，结果可能是重入、竞争或全新获取。
func WithOwner(token string) AcquireOption {
	return func(c *acquireConfig) { c.owner = token }
}

// WithLease 设置租约时长，到期由后端自动释放，默认 30s。
// 固定租约模式下业务必须在此之前完成并 Release，否则持有丢失。
func WithLease(d time.Duration) AcquireOption {
	return func(c *acquireConfig) { c.lease = d }
}

// WithWatchdog 开启自动续期：后台按租约/3（±20% 抖动）周期续约，进程死亡后
// 租约自然到期、锁自动让出。不开启则零后台活动。
func WithWatchdog() AcquireOption {
	return func(c *acquireConfig) { c.watchdog = true }
}

// WithRetryInterval 设置阻塞获取的轮询间隔，默认 200ms（±20% 抖动）。
func WithRetryInterval(d time.Duration) AcquireOption {
	return func(c *acquireConfig) { c.retryEvery = d }
}

// Option 配置 Locker 构造。
type Option func(*lockerConfig)

type lockerConfig struct {
	log    *slog.Logger
	meter  metric.Meter
	prefix string
}

func newLockerConfig(opts []Option) lockerConfig {
	// 默认值在构造期确定，日志与指标的语义不同：日志固定为静默快照
	//（discard handler），之后 slog.SetDefault 不影响已构造组件；指标默认走
	// otel 全局委托——未配置即 noop（零打点），SetMeterProvider 即便发生在
	// 构造之后也会追溯生效。prefix 默认空：物理键布局（如 redisc 的
	// glock: 前缀）由后端自管。
	c := lockerConfig{log: slog.New(slog.DiscardHandler)}
	for _, o := range opts {
		o(&c)
	}
	if c.meter == nil {
		c.meter = otel.GetMeterProvider().Meter("github.com/jninng/glock")
	}
	return c
}

// WithLogger 指定日志器，默认静默；不跟随 slog.SetDefault，
// 需全局联动时显式传 WithLogger(slog.Default())。
func WithLogger(l *slog.Logger) Option {
	return func(c *lockerConfig) { c.log = l }
}

// WithMeter 指定指标器；默认（含显式传 nil）经由 otel 全局 MeterProvider，
// 未配置即 noop，otel.SetMeterProvider 后自动接入（含构造之后的迟到配置）。
// 需要与全局装配解耦时传入具体 provider 的 Meter。
func WithMeter(m metric.Meter) Option {
	return func(c *lockerConfig) { c.meter = m }
}

// WithNamespace 设置 Key 的逻辑命名空间前缀（默认无），用于在同一后端
// 实例上隔离多套锁。物理键布局由后端自管（如 redisc 默认前缀 glock:，
// 效果 glock:{<key>}）。句柄上的 FencingTokens 仍返回未加前缀的 Key。
func WithNamespace(prefix string) Option {
	return func(c *lockerConfig) { c.prefix = prefix }
}

// NewOwnerToken 生成全局唯一的 Owner Token。
// WAL 恢复模式建议：先取 token、先持久化、再 WithOwner 获取，
// 消除“先获取后持久化”的窗口。
func NewOwnerToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 在现代操作系统上不会失败；失败即进程环境已不可信。
		panic("glock: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
