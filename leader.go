package glock

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// resignReleaseTimeout 是领导权终结收尾自动释放的独立时间预算：终结时调用方
// ctx 可能已取消，释放不能与之同生共死，同时避免后端失联时无限挂起。
const resignReleaseTimeout = 5 * time.Second

// LeaderElector 声明选主编排能力：竞选 → 在任保活 → 失位自动再竞选。纯核心
// 编排，不依赖 Driver 扩展，BasicLocker 恒定实现（与 WithLock 同层）。
type LeaderElector interface {
	// Campaign 阻塞直到首次当选（Key 上的写持有），返回跨任期领导权句柄。
	Campaign(ctx context.Context, key string, opts ...AcquireOption) (*Leadership, error)
}

var _ LeaderElector = (*BasicLocker)(nil)

// Leadership 是一次选主的跨任期句柄：在任期间丢失（被接管、租约过期、续期
// 失败）后自动重新竞选，直到 Resign 或 Campaign 的 ctx 结束——领导权只终结于
// 这两条路径，其余一切（失位、后端抖动）都在监督循环内部消化并经回调报告。
// 同一时刻至多一个任期，任期间不持有（重新当选前 Leadership 处于"不在任"）。
//
// 安全语义与单锁一致：租约式选主在时钟漂移或进程暂停下存在双主窗口，
// Leadership 是便利编排而非更强一致性原语，下游操作的正确性仍由 Fencing
// Token 防护兜底（ADR-0004/0007）。重新当选的栅栏在真实接管后必然递增；
// 续期误判（记录实际仍在）后的重入恢复沿用既有栅栏——回调不得假设递增。
type Leadership struct {
	locker *BasicLocker
	key    string        // 使用者传入的原始 Key
	cfg    acquireConfig // 再竞选沿用首次获取配置（重试上限强制放开）
	log    *slog.Logger

	intCtx    context.Context // 监督生命周期：Campaign ctx 派生，Resign 提前取消
	intCancel context.CancelFunc

	mu          sync.Mutex
	hold        *hold // 当前任期；nil 表示不在任
	resigned    bool  // 已终结（Resign 或终结收尾），此后不再派发回调
	lostFns     []func(reason error)
	regainedFns []func(fence uint64)

	leading atomic.Bool
	done    chan struct{}
}

// Campaign 实现 LeaderElector：阻塞直到首次当选，获取语义与 Acquire 一致
//（放弃契约同构：ctx 结束返回包装 ErrHeld 与 ctx 错误的错误，WithMaxRetries
// 用尽返回包装 ErrHeld 与 ErrExhausted 的错误）。成功后进入受监督状态：
// watchdog 强制开启（无续期的固定租约会让领导权按租约周期抖动），失位自动
// 再竞选——再竞选不受 WithMaxRetries 约束，直到 ctx 结束。
func (l *BasicLocker) Campaign(ctx context.Context, key string, opts ...AcquireOption) (*Leadership, error) {
	cfg, err := l.newAcquireConfig(opts)
	if err != nil {
		return nil, err
	}
	cfg.watchdog = true
	recfg := cfg
	recfg.maxRetries = -1

	try := func(ctx context.Context, fullKey, owner string, lease time.Duration) (uint64, error) {
		return l.drv.TryAcquire(ctx, fullKey, Write, owner, lease)
	}
	h, err := l.acquireOne(ctx, key, Write, cfg, true, try)
	if err != nil {
		return nil, err
	}
	if h.wdOn {
		startWatchdog([]*hold{h})
	}

	intCtx, cancel := context.WithCancel(ctx)
	ls := &Leadership{
		locker:    l,
		key:       key,
		cfg:       recfg,
		log:       l.log,
		intCtx:    intCtx,
		intCancel: cancel,
		hold:      h,
		done:      make(chan struct{}),
	}
	ls.leading.Store(true)
	go ls.supervise()
	return ls, nil
}

// supervise 是监督 goroutine：守望当前任期，失位后派发 OnLost 并自动再竞选，
// 重新当选派发 OnRegained；终结（Resign 或 intCtx 结束）时释放残余持有并关闭
// done。全部回调都在本 goroutine 上同步有序派发。
func (ls *Leadership) supervise() {
	defer close(ls.done)
	for {
		if ls.terminated() {
			ls.finish()
			return
		}
		ls.mu.Lock()
		h := ls.hold
		ls.mu.Unlock()
		if h == nil {
			ls.finish()
			return
		}
		select {
		case <-ls.intCtx.Done():
			ls.finish()
			return
		case <-h.Lost():
		}
		// 失位：先摘下任期再派发——回调内查询 Leading() 已是失位状态。
		ls.mu.Lock()
		if ls.hold == h {
			ls.hold = nil
		}
		ls.mu.Unlock()
		ls.leading.Store(false)
		if !ls.terminated() {
			ls.dispatchLost(h.lostReason())
		}
		if ls.terminated() {
			ls.finish()
			return
		}
		h2, err := ls.recampaign()
		if err != nil {
			// 仅 intCtx 结束（Resign 或 Campaign ctx）会到达这里。
			ls.finish()
			return
		}
		ls.mu.Lock()
		if ls.resigned {
			// 竞选期间发生终结：弃选，刚到手的持有立即释放（独立预算）。
			ls.mu.Unlock()
			ls.releaseQuiet(h2)
			ls.finish()
			return
		}
		ls.hold = h2
		ls.mu.Unlock()
		ls.leading.Store(true)
		ls.dispatchRegained(h2.fence)
	}
}

// recampaign 执行再竞选直到当选或 intCtx 结束。ErrHeld 由 acquireOne 的阻塞
// 循环内部消化（重试上限已强制放开）；其余错误（传输等）按等待策略外层重试
// ——领导权不因后端抖动终结。新任期按监督配置重启 watchdog。
func (ls *Leadership) recampaign() (*hold, error) {
	try := func(ctx context.Context, fullKey, owner string, lease time.Duration) (uint64, error) {
		return ls.locker.drv.TryAcquire(ctx, fullKey, Write, owner, lease)
	}
	attempts := 0
	for {
		h, err := ls.locker.acquireOne(ls.intCtx, ls.key, Write, ls.cfg, true, try)
		if err == nil {
			if h.wdOn {
				startWatchdog([]*hold{h})
			}
			return h, nil
		}
		if ls.intCtx.Err() != nil {
			return nil, ls.intCtx.Err()
		}
		attempts++
		ls.log.WarnContext(ls.intCtx, "glock: leadership recampaign failed, retrying",
			slog.String("key", ls.key),
			slog.Int("attempt", attempts),
			slog.String(attrErrorType, err.Error()))
		select {
		case <-ls.intCtx.Done():
			return nil, ls.intCtx.Err()
		case <-time.After(ls.cfg.waitAt(attempts)):
		}
	}
}

// Resign 退位：停止自动再竞选并释放当前任期，终结领导权。终结后 Done 关闭，
// 后续回调不再派发；重复退位返回包装 ErrLost 的错误。释放使用调用方 ctx（与
// Release 同契约：ctx 已取消则释放失败并返回错误，残余持有随租约自然过期）。
// 在回调内退位合法：当前回调返回后不再竞选、不再派发。返回时监督 goroutine
// 可能仍在收尾（放弃在途竞选），join 用 Done。
func (ls *Leadership) Resign(ctx context.Context) error {
	ls.mu.Lock()
	if ls.resigned {
		ls.mu.Unlock()
		return fmt.Errorf("%w: leadership already ended", ErrLost)
	}
	ls.resigned = true
	h := ls.hold
	ls.hold = nil
	ls.mu.Unlock()
	ls.intCancel()
	ls.leading.Store(false)
	if h == nil {
		return nil // 不在任（失位再竞选中）：无可释放，残余记录由租约自然过期
	}
	return h.release(ctx)
}

// terminated 报告领导权是否已进入终结（Resign、终结收尾或 intCtx 结束）。
func (ls *Leadership) terminated() bool {
	ls.mu.Lock()
	r := ls.resigned
	ls.mu.Unlock()
	return r || ls.intCtx.Err() != nil
}

// finish 终结收尾：标记终结（此后不再派发回调），释放残余持有（调用方 ctx
// 可能已取消，用独立预算）。Resign 已释放的路径 hold 为 nil，天然幂等。
func (ls *Leadership) finish() {
	ls.mu.Lock()
	ls.resigned = true
	h := ls.hold
	ls.hold = nil
	ls.mu.Unlock()
	ls.leading.Store(false)
	if h == nil {
		return
	}
	relCtx, cancel := context.WithTimeout(context.WithoutCancel(ls.intCtx), resignReleaseTimeout)
	defer cancel()
	if err := h.release(relCtx); err != nil {
		ls.log.ErrorContext(relCtx, "glock: leadership release on end failed",
			slog.String("key", ls.key),
			slog.String(attrErrorType, err.Error()))
	}
}

// releaseQuiet 供弃选路径使用：尽力释放刚到手的持有并记录失败，不返回错误。
func (ls *Leadership) releaseQuiet(h *hold) {
	relCtx, cancel := context.WithTimeout(context.WithoutCancel(ls.intCtx), resignReleaseTimeout)
	defer cancel()
	if err := h.release(relCtx); err != nil {
		ls.log.ErrorContext(relCtx, "glock: leadership resign release failed",
			slog.String("key", ls.key),
			slog.String(attrErrorType, err.Error()))
	}
}

// OnLost 注册失位回调：每次失位（被接管、租约过期、续期失败——传输故障同样
// 保守按失位报告）触发一次，reason 为丢失原因。回调在监督 goroutine 上同步
// 有序派发：OnLost 返回前不开始再竞选，第 n 次 OnLost 先于第 n+1 次
// OnRegained；派发时 Leading() 已为 false。回调不应长时间阻塞（会推迟重新
// 当选）；panic 被捕获并按 Error 记录，不中断派发。终结后注册的回调不触发。
func (ls *Leadership) OnLost(fn func(reason error)) {
	if fn == nil {
		return
	}
	ls.mu.Lock()
	ls.lostFns = append(ls.lostFns, fn)
	ls.mu.Unlock()
}

// OnRegained 注册重新当选回调：每次重新当选触发（首次当选以 Campaign 返回
// 为准，不触发）。fence 为当前任期 Fencing Token：真实接管后必然递增，续期
// 误判后的重入恢复沿用既有栅栏。派发时 Leading() 已为 true。
func (ls *Leadership) OnRegained(fn func(fence uint64)) {
	if fn == nil {
		return
	}
	ls.mu.Lock()
	ls.regainedFns = append(ls.regainedFns, fn)
	ls.mu.Unlock()
}

// dispatchLost 同步派发全部失位回调（快照后逐个调用，注册并发安全）。
func (ls *Leadership) dispatchLost(reason error) {
	ls.mu.Lock()
	fns := slices.Clone(ls.lostFns)
	ls.mu.Unlock()
	for _, fn := range fns {
		ls.runCallback(func() { fn(reason) })
	}
}

// dispatchRegained 同步派发全部重新当选回调。
func (ls *Leadership) dispatchRegained(fence uint64) {
	ls.mu.Lock()
	fns := slices.Clone(ls.regainedFns)
	ls.mu.Unlock()
	for _, fn := range fns {
		ls.runCallback(func() { fn(fence) })
	}
}

// runCallback 执行单个回调，捕获 panic 保证派发连续（回调跑在监督 goroutine
// 上，任其传播会带崩整个进程）。
func (ls *Leadership) runCallback(body func()) {
	defer func() {
		if r := recover(); r != nil {
			ls.log.ErrorContext(context.Background(), "glock: leadership callback panicked",
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())))
		}
	}()
	body()
}

// Leading 报告当前是否在任。true 只代表检查瞬间的租约状态，检查与后续操作
// 之间存在失位窗口，下游正确性仍由栅栏防护兜底。
func (ls *Leadership) Leading() bool { return ls.leading.Load() }

// FencingToken 返回当前任期的 Fencing Token，仅在 Leading() 为 true 时有意义；
// 不在任时返回 0。
func (ls *Leadership) FencingToken() uint64 {
	ls.mu.Lock()
	h := ls.hold
	ls.mu.Unlock()
	if h == nil {
		return 0
	}
	return h.fence
}

// OwnerToken 返回本领导权的 Owner Token（跨任期稳定：失位后以同一身份再竞选，
// 续期误判下走重入恢复）。
func (ls *Leadership) OwnerToken() string { return ls.cfg.owner }

// Done 在领导权完全终结（Resign 或 Campaign ctx 结束，含终结收尾）时关闭。
// 需要等待监督 goroutine 退出（如以同一 Owner Token 重新竞选同一 Key）时 join 它。
func (ls *Leadership) Done() <-chan struct{} { return ls.done }
