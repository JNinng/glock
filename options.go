package glock

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jninng/observ"
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

// WithWatchdog 开启自动续期：后台按约租约/3（±20% 抖动）周期续约，进程死亡后
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
	log    observ.Logger
	meter  observ.Meter
	prefix string
}

func newLockerConfig(opts []Option) lockerConfig {
	// observ 约定：构造期对默认值做一次快照，之后替换全局默认不影响本组件。
	// prefix 默认空：物理键布局（如 redisc 的 glock: 前缀）由后端自管。
	c := lockerConfig{log: observ.DefaultLogger(), meter: observ.DefaultMeter()}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithLogger 指定日志器，默认取构造时刻的 observ.DefaultLogger()。
func WithLogger(l observ.Logger) Option {
	return func(c *lockerConfig) { c.log = l }
}

// WithMeter 指定指标器，默认取构造时刻的 observ.DefaultMeter()。
func WithMeter(m observ.Meter) Option {
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
