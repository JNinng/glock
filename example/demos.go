package main

// 能力 ↔ 检查对照。新增能力时：写一个 demoXxx 函数并在 demos 登记。
//
// 单持有者语义：
//
//	互斥竞争 / ErrHeld             demoMutex
//	固定租约 / 到期自动释放 / 防误删  demoLease
//	watchdog 自动续期              demoWatchdog
//	读写锁 / 模式冲突              demoReadWrite
//	写降读                        demoDowngrade
//	可重入 / WAL 恢复              demoReentrant
//	多锁全有或全无                 demoMultiLock
//	WithLock 执行辅助              demoWithLock
//	Probe 状态内省                 demoProbe
//	Locker 级默认配置              demoDefaults
//	Fencing Token 单调             demoFencing
//	句柄终结语义                   demoHandleTerminal
//	命名空间隔离                   demoNamespaceIsolation
//	Probe 过期残余不算持有          demoProbeExpiryResidue
//	特殊字符 Key                   demoSpecialKeys
//
// 并发对撞（只断言计数/集合等确定性不变量，不依赖时序，不会 flaky）：
//
//	赢家唯一对撞                   demoWinnerUnique
//	计数器对撞（端到端正确性）        demoCounterCollision
//	交叉多锁无死锁                  demoCrossMultiLock
//	多读者对撞                     demoReadersCollision
//	watchdog 压力竞争              demoWatchdogPressure
//	同 token 并发重入              demoReentrantCollision
//
// 阻塞语义：
//
//	阻塞获取 + 释放唤醒             demoBlockingWake
//	阻塞等待过期接管                demoBlockingExpiryWake
//	ctx 取消的错误包装              demoCtxCancel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	glock "github.com/jninng/glock"
)

// collideN 是对撞类演示的并发规模，roundsM 是计数器对撞的临界区次数。
// 本机/常规 CI 无压力（redisc 下阻塞等待者各建一条订阅连接，本地 16 并发
// 远低于连接上限）；若在弱环境把 example 挂进流水线，调小即可。
const (
	collideN = 16
	roundsM  = 25
)

// runCollide 起 n 个 goroutine 在发令枪后同时冲线执行 fn（最大化竞争窗口），
// 返回第 i 个 goroutine 的结果。
func runCollide(n int, fn func(i int) error) []error {
	start := make(chan struct{})
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
	return errs
}

// env 传给每个检查：ns 与基础 Locker 同域，newLocker 供构造级选项的检查
// 另建 Locker 实例。
type env struct {
	newLocker func(opts ...glock.Option) glock.Locker
	ns        string
}

type demo struct {
	name string
	run  func(ctx context.Context, e env, lg glock.Locker) error
}

var demos = []demo{
	{"互斥与 ErrHeld", demoMutex},
	{"阻塞获取与释放唤醒", demoBlockingWake},
	{"固定租约与防误删", demoLease},
	{"watchdog 自动续期", demoWatchdog},
	{"读写锁与模式冲突", demoReadWrite},
	{"写降读", demoDowngrade},
	{"可重入与 WAL 恢复", demoReentrant},
	{"多锁全有或全无", demoMultiLock},
	{"WithLock 执行辅助", demoWithLock},
	{"Probe 状态内省", demoProbe},
	{"Locker 级默认配置", demoDefaults},
	{"Fencing Token 单调", demoFencing},
	{"赢家唯一对撞", demoWinnerUnique},
	{"计数器对撞", demoCounterCollision},
	{"交叉多锁无死锁", demoCrossMultiLock},
	{"多读者对撞", demoReadersCollision},
	{"watchdog 压力竞争", demoWatchdogPressure},
	{"同 token 并发重入", demoReentrantCollision},
	{"阻塞等待过期接管", demoBlockingExpiryWake},
	{"ctx 取消的错误包装", demoCtxCancel},
	{"句柄终结语义", demoHandleTerminal},
	{"命名空间隔离", demoNamespaceIsolation},
	{"Probe 过期残余不算持有", demoProbeExpiryResidue},
	{"特殊字符 Key", demoSpecialKeys},
}

// wantOK / wantErr 把每个检查写成一列可读断言：断言失败即返回带上下文的
// 错误，由 runSuite 统一汇报。
func wantOK(what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", what, err)
}

func wantErr(what string, err error, target error) error {
	if errors.Is(err, target) {
		return nil
	}
	return fmt.Errorf("%s: want %w, got %v", what, target, err)
}

func demoMutex(ctx context.Context, _ env, lg glock.Locker) error {
	h1, err := lg.TryAcquire(ctx, "mutex", glock.WithOwner("me"))
	if err := wantOK("首次 TryAcquire", err); err != nil {
		return err
	}
	if err := wantErr("他人竞争被拒", func() error {
		_, err := lg.TryAcquire(ctx, "mutex", glock.WithOwner("other"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	if err := wantOK("释放", h1.Release(ctx)); err != nil {
		return err
	}
	h2, err := lg.TryAcquire(ctx, "mutex", glock.WithOwner("other"))
	if err := wantOK("释放后他人可获取", err); err != nil {
		return err
	}
	return wantOK("清理", h2.Release(ctx))
}

func demoBlockingWake(ctx context.Context, _ env, lg glock.Locker) error {
	holder, err := lg.TryAcquire(ctx, "wake", glock.WithOwner("holder"), glock.WithLease(30*time.Second))
	if err := wantOK("holder TryAcquire", err); err != nil {
		return err
	}
	acquired := make(chan error, 1)
	go func() {
		// 轮询间隔 10s：时限内醒来只能靠释放信号（NotifyingDriver）。
		h, err := lg.Acquire(ctx, "wake", glock.WithOwner("waiter"), glock.WithRetryInterval(10*time.Second))
		if err != nil {
			acquired <- err
			return
		}
		acquired <- nil
		_ = h.Release(context.Background())
	}()
	time.Sleep(250 * time.Millisecond) // 等待者应已完成订阅（信号尽力而为，漏收会落回 10s 轮询）
	if err := wantOK("holder Release", holder.Release(ctx)); err != nil {
		return err
	}
	select {
	case err := <-acquired:
		return wantOK("waiter 靠释放信号醒来", err)
	case <-time.After(5 * time.Second):
		return errors.New("waiter 5s 内未醒来：释放唤醒未生效（10s 轮询不可能在时限内触发）")
	}
}

func demoLease(ctx context.Context, _ env, lg glock.Locker) error {
	h, err := lg.TryAcquire(ctx, "lease", glock.WithOwner("me"), glock.WithLease(300*time.Millisecond))
	if err := wantOK("TryAcquire 短租约", err); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := lg.TryAcquire(ctx, "lease", glock.WithOwner("other")); err != nil {
		return fmt.Errorf("租约到期后他人应可获取（自动释放）, got %v", err)
	}
	// 到期句柄的迟到释放必须被拒：不能删掉新持有者的记录（防误删）。
	err = h.Release(ctx)
	if !errors.Is(err, glock.ErrLost) || !errors.Is(err, glock.ErrNotOwner) {
		return fmt.Errorf("迟到释放: want ErrLost+ErrNotOwner, got %v", err)
	}
	return nil
}

func demoWatchdog(ctx context.Context, _ env, lg glock.Locker) error {
	h, err := lg.Acquire(ctx, "wd", glock.WithOwner("me"), glock.WithLease(400*time.Millisecond), glock.WithWatchdog())
	if err := wantOK("Acquire 短租约+watchdog", err); err != nil {
		return err
	}
	time.Sleep(1200 * time.Millisecond) // 3 个租约周期
	if err := wantErr("watchdog 续约保住持有", func() error {
		_, err := lg.TryAcquire(ctx, "wd", glock.WithOwner("other"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	return wantOK("释放", h.Release(ctx))
}

func demoReadWrite(ctx context.Context, _ env, lg glock.Locker) error {
	rw, ok := lg.(glock.RWLocker)
	if !ok {
		return errors.New("Locker 不提供读写锁（RWLocker）")
	}
	r1, err := rw.AcquireRead(ctx, "rw", glock.WithOwner("r1"), glock.WithLease(30*time.Second))
	if err := wantOK("读者一 AcquireRead", err); err != nil {
		return err
	}
	r2, err := rw.TryAcquireRead(ctx, "rw", glock.WithOwner("r2"))
	if err := wantOK("读者二共享进入", err); err != nil {
		return err
	}
	if err := wantErr("读持有时写者被拒", func() error {
		_, err := lg.TryAcquire(ctx, "rw", glock.WithOwner("w1"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	if err := wantErr("同 Owner 持读取写被拒", func() error {
		_, err := lg.TryAcquire(ctx, "rw", glock.WithOwner("r1"))
		return err
	}(), glock.ErrModeConflict); err != nil {
		return err
	}
	if err := wantOK("读者一释放", r1.Release(ctx)); err != nil {
		return err
	}
	if err := wantOK("读者二释放", r2.Release(ctx)); err != nil {
		return err
	}
	w, err := lg.TryAcquire(ctx, "rw", glock.WithOwner("w1"))
	if err := wantOK("读者走空后写者进入", err); err != nil {
		return err
	}
	return wantOK("写者释放", w.Release(ctx))
}

func demoDowngrade(ctx context.Context, _ env, lg glock.Locker) error {
	rw, ok := lg.(glock.RWLocker)
	if !ok {
		return errors.New("Locker 不提供读写锁（RWLocker）")
	}
	w, err := lg.Acquire(ctx, "dg", glock.WithOwner("me"), glock.WithLease(30*time.Second))
	if err := wantOK("Acquire 写持有", err); err != nil {
		return err
	}
	fence := w.FencingToken()
	rl, err := w.Downgrade(ctx)
	if err := wantOK("写降读", err); err != nil {
		return err
	}
	if rl.FencingToken() != fence {
		return fmt.Errorf("降级改变了 Fencing Token: %d -> %d", fence, rl.FencingToken())
	}
	if err := wantErr("旧句柄已终结", func() error {
		return w.Release(ctx)
	}(), glock.ErrLost); err != nil {
		return err
	}
	// 降级后读者放行：r2 与降级持有共存，句柄保留待释放。
	r2, err := rw.AcquireRead(ctx, "dg", glock.WithOwner("r2"), glock.WithLease(30*time.Second))
	if err := wantOK("降级后读者放行", err); err != nil {
		return err
	}
	if err := wantErr("读者持有时写者仍被挡", func() error {
		_, err := lg.TryAcquire(ctx, "dg", glock.WithOwner("w2"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	if err := wantOK("释放降级句柄", rl.Release(ctx)); err != nil {
		return err
	}
	if err := wantOK("释放读者 r2", r2.Release(ctx)); err != nil {
		return err
	}
	// 计数应已归零：新 owner 全新进入（非重入），随后写者也放行——
	// 若有任何残留计数，这两步会被 ErrHeld 挡下。
	fresh, err := rw.TryAcquireRead(ctx, "dg", glock.WithOwner("r3"))
	if err := wantOK("释放后新读者全新进入", err); err != nil {
		return err
	}
	if err := wantOK("清理新读者", fresh.Release(ctx)); err != nil {
		return err
	}
	w2, err := lg.TryAcquire(ctx, "dg", glock.WithOwner("w2"))
	if err := wantOK("读者走空后写者放行", err); err != nil {
		return err
	}
	return wantOK("清理写者", w2.Release(ctx))
}

func demoReentrant(ctx context.Context, _ env, lg glock.Locker) error {
	token := glock.NewOwnerToken() // WAL 恢复模式：先取 token 再获取
	h1, err := lg.TryAcquire(ctx, "re", glock.WithOwner(token))
	if err := wantOK("同 token 首次获取", err); err != nil {
		return err
	}
	h2, err := lg.TryAcquire(ctx, "re", glock.WithOwner(token))
	if err := wantOK("同 token 重入", err); err != nil {
		return err
	}
	if h1.FencingToken() != h2.FencingToken() {
		return fmt.Errorf("重入不应递增栅栏: %d vs %d", h1.FencingToken(), h2.FencingToken())
	}
	if err := wantErr("计数内他人被拒", func() error {
		_, err := lg.TryAcquire(ctx, "re", glock.WithOwner("other"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	if err := wantOK("释放一层", h1.Release(ctx)); err != nil {
		return err
	}
	if err := wantErr("还剩一层仍互斥", func() error {
		_, err := lg.TryAcquire(ctx, "re", glock.WithOwner("other"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	if err := wantOK("释放最后一层", h2.Release(ctx)); err != nil {
		return err
	}
	other, err := lg.TryAcquire(ctx, "re", glock.WithOwner("other"))
	if err := wantOK("计数归零后他人可获取", err); err != nil {
		return err
	}
	return wantOK("清理", other.Release(ctx))
}

func demoMultiLock(ctx context.Context, _ env, lg glock.Locker) error {
	mlk, ok := lg.(glock.MultiLocker)
	if !ok {
		return errors.New("Locker 不提供多锁（MultiLocker）")
	}
	blocker, err := lg.TryAcquire(ctx, "multi:c", glock.WithOwner("other"), glock.WithLease(30*time.Second))
	if err := wantOK("外部持有 multi:c", err); err != nil {
		return err
	}
	if err := wantErr("任一失败即整体被拒", func() error {
		_, err := mlk.TryAcquireAll(ctx, []string{"multi:a", "multi:b", "multi:c"}, glock.WithOwner("me"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	// 回滚验证：失败后已到手的 Key 不能残留我的持有。
	for _, k := range []string{"multi:a", "multi:b"} {
		chk, err := lg.TryAcquire(ctx, k, glock.WithOwner("check"))
		if err := wantOK("回滚无残留（"+k+"）", err); err != nil {
			return err
		}
		if err := wantOK("清理 "+k, chk.Release(ctx)); err != nil {
			return err
		}
	}
	if err := wantOK("释放 blocker", blocker.Release(ctx)); err != nil {
		return err
	}
	ml, err := mlk.AcquireAll(ctx, []string{"multi:c", "multi:a", "multi:b"}, glock.WithOwner("me"))
	if err := wantOK("全量获取（乱序传参，内部按字典序防死锁）", err); err != nil {
		return err
	}
	if len(ml.FencingTokens()) != 3 {
		return fmt.Errorf("FencingTokens 应为 3 个, got %v", ml.FencingTokens())
	}
	if err := wantOK("整组释放", ml.Release(ctx)); err != nil {
		return err
	}
	return wantErr("句柄已终结，二次释放被拒", func() error {
		return ml.Release(ctx)
	}(), glock.ErrLost)
}

func demoWithLock(ctx context.Context, _ env, lg glock.Locker) error {
	wl, ok := lg.(interface {
		WithLock(ctx context.Context, key string, fn func(ctx context.Context, lock *glock.Lock) error, opts ...glock.AcquireOption) error
	})
	if !ok {
		return errors.New("Locker 不提供 WithLock")
	}
	var fence uint64
	if err := wl.WithLock(ctx, "wl", func(_ context.Context, lock *glock.Lock) error {
		fence = lock.FencingToken()
		return nil
	}, glock.WithOwner("me")); err != nil {
		return wantOK("正常路径", err)
	}
	if fence == 0 {
		return errors.New("fn 未收到有效 Fencing Token")
	}
	probe, err := lg.TryAcquire(ctx, "wl", glock.WithOwner("check"))
	if err := wantOK("fn 返回后已自动释放", err); err != nil {
		return err
	}
	if err := wantOK("清理", probe.Release(ctx)); err != nil {
		return err
	}
	// fn 出错：错误向上传播，且持有仍被释放。
	sentinel := errors.New("boom")
	err = wl.WithLock(ctx, "wl", func(context.Context, *glock.Lock) error { return sentinel }, glock.WithOwner("me"))
	if !errors.Is(err, sentinel) {
		return fmt.Errorf("fn 错误应向上传播, got %v", err)
	}
	probe, err = lg.TryAcquire(ctx, "wl", glock.WithOwner("check"))
	if err := wantOK("fn 出错仍已释放", err); err != nil {
		return err
	}
	return wantOK("清理", probe.Release(ctx))
}

func demoProbe(ctx context.Context, _ env, lg glock.Locker) error {
	p, ok := lg.(glock.ProbingLocker)
	if !ok {
		return errors.New("Locker 不提供 Probe（Driver 未实现 ProbingDriver）")
	}
	empty, err := p.Probe(ctx, "pf")
	if err := wantOK("空 Key 内省", err); err != nil {
		return err
	}
	if len(empty) != 0 {
		return fmt.Errorf("空 Key 应无持有, got %v", empty)
	}
	h, err := lg.TryAcquire(ctx, "pf", glock.WithOwner("me"))
	if err := wantOK("TryAcquire 写持有", err); err != nil {
		return err
	}
	infos, err := p.Probe(ctx, "pf")
	if err := wantOK("写持有内省", err); err != nil {
		return err
	}
	if len(infos) != 1 || infos[0].Owner != "me" || infos[0].Mode != glock.Write ||
		infos[0].Fence != h.FencingToken() || infos[0].Remaining <= 0 {
		return fmt.Errorf("写持有快照不符: got %+v, want owner=me write fence=%d", infos, h.FencingToken())
	}
	if err := wantOK("释放写持有", h.Release(ctx)); err != nil {
		return err
	}
	r1, err := lg.(glock.RWLocker).AcquireRead(ctx, "pf", glock.WithOwner("r1"), glock.WithLease(30*time.Second))
	if err := wantOK("读者一", err); err != nil {
		return err
	}
	r2, err := lg.(glock.RWLocker).AcquireRead(ctx, "pf", glock.WithOwner("r2"), glock.WithLease(30*time.Second))
	if err := wantOK("读者二", err); err != nil {
		return err
	}
	infos, err = p.Probe(ctx, "pf")
	if err := wantOK("读持有内省", err); err != nil {
		return err
	}
	if len(infos) != 2 || infos[0].Owner != "r1" || infos[1].Owner != "r2" ||
		infos[0].Mode != glock.Read || infos[1].Mode != glock.Read {
		return fmt.Errorf("读持有快照应为按 Owner 排序的两条 read: got %+v", infos)
	}
	if err := wantOK("读者一释放", r1.Release(ctx)); err != nil {
		return err
	}
	if err := wantOK("读者二释放", r2.Release(ctx)); err != nil {
		return err
	}
	infos, err = p.Probe(ctx, "pf")
	if err := wantOK("释放后内省", err); err != nil {
		return err
	}
	if len(infos) != 0 {
		return fmt.Errorf("全部释放后应无持有, got %v", infos)
	}
	return nil
}

func demoDefaults(ctx context.Context, e env, _ glock.Locker) error {
	// 默认租约 + 默认 watchdog：零选项获取跨多个默认租约周期仍存活。
	lg := e.newLocker(
		glock.WithNamespace(e.ns),
		glock.WithDefaultLease(400*time.Millisecond),
		glock.WithDefaultWatchdog(),
	)
	h, err := lg.Acquire(ctx, "df") // 不传任何获取选项
	if err := wantOK("零选项获取（吃到 Locker 级默认）", err); err != nil {
		return err
	}
	time.Sleep(1200 * time.Millisecond)
	if err := wantErr("默认租约+默认 watchdog 保住持有", func() error {
		_, err := lg.TryAcquire(ctx, "df", glock.WithOwner("other"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	if err := wantOK("释放", h.Release(ctx)); err != nil {
		return err
	}

	// 仅默认租约（无 watchdog）：零选项按默认租约过期，单次 WithLease 覆盖默认。
	lg2 := e.newLocker(glock.WithNamespace(e.ns), glock.WithDefaultLease(400*time.Millisecond))
	if _, err := lg2.Acquire(ctx, "df2"); err != nil {
		return wantOK("零选项获取", err)
	}
	time.Sleep(700 * time.Millisecond)
	if _, err := lg2.TryAcquire(ctx, "df2", glock.WithOwner("other")); err != nil {
		return fmt.Errorf("默认租约(400ms)应已到期自动释放, got %v", err)
	}
	h3, err := lg2.Acquire(ctx, "df3", glock.WithLease(30*time.Second))
	if err := wantOK("单次 WithLease 覆盖默认", err); err != nil {
		return err
	}
	time.Sleep(700 * time.Millisecond)
	if err := wantErr("覆盖租约(30s)仍存活", func() error {
		_, err := lg2.TryAcquire(ctx, "df3", glock.WithOwner("other"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	return wantOK("清理", h3.Release(ctx))
}

func demoFencing(ctx context.Context, _ env, lg glock.Locker) error {
	var prev uint64
	for _, owner := range []string{"a", "b", "a", "b"} {
		h, err := lg.Acquire(ctx, "fc", glock.WithOwner(owner), glock.WithRetryInterval(20*time.Millisecond))
		if err := wantOK("获取（"+owner+"）", err); err != nil {
			return err
		}
		if h.FencingToken() <= prev {
			return fmt.Errorf("栅栏未单调递增: %d after %d", h.FencingToken(), prev)
		}
		prev = h.FencingToken()
		if err := wantOK("释放", h.Release(ctx)); err != nil {
			return err
		}
	}
	return nil
}

// ---- 并发对撞：只断言确定性不变量，任何失败都意味着语义被破坏 ----

func demoWinnerUnique(ctx context.Context, _ env, lg glock.Locker) error {
	handles := make([]*glock.Lock, collideN)
	errs := runCollide(collideN, func(i int) error {
		h, err := lg.TryAcquire(ctx, "cc-winner", glock.WithOwner(fmt.Sprintf("c-%d", i)))
		handles[i] = h
		return err
	})
	wins, held := 0, 0
	var winner *glock.Lock
	for i, err := range errs {
		switch {
		case err == nil:
			wins++
			winner = handles[i]
		case errors.Is(err, glock.ErrHeld):
			held++
		default:
			return fmt.Errorf("竞争者 %d: 意外错误 %v", i, err)
		}
	}
	if wins != 1 || held != collideN-1 {
		return fmt.Errorf("%d 并发 TryAcquire: 赢家=%d ErrHeld=%d; want 1/%d", collideN, wins, held, collideN-1)
	}
	return wantOK("清理赢家持有", winner.Release(ctx))
}

func demoCounterCollision(ctx context.Context, _ env, lg glock.Locker) error {
	wl, ok := lg.(interface {
		WithLock(ctx context.Context, key string, fn func(ctx context.Context, lock *glock.Lock) error, opts ...glock.AcquireOption) error
	})
	if !ok {
		return errors.New("Locker 不提供 WithLock")
	}
	// 计数器故意用非原子读-改-写：临界区互斥一旦失效（双持有）必然丢更新、
	// 总数偏小——锁的端到端正确性在黑盒下最强的证据。注意：临界区间的同步
	// 由分布式锁承担，race detector 无法感知它，-race 运行会在此报语义假阳性。
	counter := 0
	errs := runCollide(collideN, func(g int) error {
		for r := 0; r < roundsM; r++ {
			if err := wl.WithLock(ctx, "cc-count", func(_ context.Context, _ *glock.Lock) error {
				v := counter
				counter = v + 1
				return nil
			}, glock.WithOwner(fmt.Sprintf("g-%d", g))); err != nil {
				return err
			}
		}
		return nil
	})
	for i, err := range errs {
		if err != nil {
			return fmt.Errorf("竞争者 %d: %v", i, err)
		}
	}
	if counter != collideN*roundsM {
		return fmt.Errorf("互斥失效：计数器 %d, want %d（存在丢更新）", counter, collideN*roundsM)
	}
	return nil
}

func demoCrossMultiLock(ctx context.Context, _ env, lg glock.Locker) error {
	mlk, ok := lg.(glock.MultiLocker)
	if !ok {
		return errors.New("Locker 不提供多锁（MultiLocker）")
	}
	const k = 8
	ring := make([]string, k)
	for i := range ring {
		ring[i] = fmt.Sprintf("cc-ring:%d", i)
	}
	errs := runCollide(k, func(g int) error {
		// 首尾相接的重叠对：不按序获取在此必然交叉死锁，
		// AcquireAll 的字典序应让全部 goroutine 在时限内完成。
		ml, err := mlk.AcquireAll(ctx, []string{ring[g], ring[(g+1)%k]},
			glock.WithOwner(fmt.Sprintf("g-%d", g)), glock.WithRetryInterval(20*time.Millisecond))
		if err != nil {
			return err
		}
		if len(ml.FencingTokens()) != 2 {
			return fmt.Errorf("应持有 2 把锁, got %v", ml.FencingTokens())
		}
		return ml.Release(ctx)
	})
	for g, err := range errs {
		if err != nil {
			return fmt.Errorf("goroutine %d: %v（若为超时即交叉死锁）", g, err)
		}
	}
	return nil
}

func demoReadersCollision(ctx context.Context, _ env, lg glock.Locker) error {
	rw, ok := lg.(glock.RWLocker)
	if !ok {
		return errors.New("Locker 不提供读写锁（RWLocker）")
	}
	handles := make([]*glock.ReadLock, collideN)
	errs := runCollide(collideN, func(i int) error {
		h, err := rw.AcquireRead(ctx, "cc-readers",
			glock.WithOwner(fmt.Sprintf("r-%d", i)), glock.WithLease(30*time.Second))
		handles[i] = h
		return err
	})
	for i, err := range errs {
		if err != nil {
			return fmt.Errorf("读者 %d 未进入: %v", i, err)
		}
	}
	if err := wantErr("全体读者持有时写者被拒", func() error {
		_, err := lg.TryAcquire(ctx, "cc-readers", glock.WithOwner("w"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	for i, h := range handles {
		if err := wantOK(fmt.Sprintf("读者 %d 释放", i), h.Release(ctx)); err != nil {
			return err
		}
	}
	w, err := lg.TryAcquire(ctx, "cc-readers", glock.WithOwner("w"))
	if err := wantOK("读者走空后写者放行", err); err != nil {
		return err
	}
	return wantOK("清理", w.Release(ctx))
}

func demoWatchdogPressure(ctx context.Context, _ env, lg glock.Locker) error {
	h, err := lg.Acquire(ctx, "cc-wd", glock.WithOwner("me"), glock.WithLease(500*time.Millisecond), glock.WithWatchdog())
	if err := wantOK("watchdog 持有", err); err != nil {
		return err
	}
	const rounds = 5
	errs := runCollide(collideN, func(g int) error {
		for r := 0; r < rounds; r++ {
			if _, err := lg.TryAcquire(ctx, "cc-wd", glock.WithOwner(fmt.Sprintf("c-%d-%d", g, r))); !errors.Is(err, glock.ErrHeld) {
				return fmt.Errorf("第 %d 轮: want ErrHeld, got %v", r, err)
			}
		}
		return nil
	})
	for g, err := range errs {
		if err != nil {
			return fmt.Errorf("挑战者 %d 攻破互斥: %v", g, err)
		}
	}
	time.Sleep(1500 * time.Millisecond) // 3 个租约周期：压力下续约持续成功
	if err := wantErr("压力过后持有仍在", func() error {
		_, err := lg.TryAcquire(ctx, "cc-wd", glock.WithOwner("late"))
		return err
	}(), glock.ErrHeld); err != nil {
		return err
	}
	if err := wantOK("释放", h.Release(ctx)); err != nil {
		return err
	}
	winner, err := lg.TryAcquire(ctx, "cc-wd", glock.WithOwner("late"))
	if err := wantOK("释放后可获取", err); err != nil {
		return err
	}
	return wantOK("清理", winner.Release(ctx))
}

func demoReentrantCollision(ctx context.Context, _ env, lg glock.Locker) error {
	token := glock.NewOwnerToken()
	handles := make([]*glock.Lock, collideN)
	errs := runCollide(collideN, func(i int) error {
		h, err := lg.TryAcquire(ctx, "cc-re", glock.WithOwner(token))
		if err == nil {
			handles[i] = h
		}
		return err
	})
	for i, err := range errs {
		if err != nil {
			return fmt.Errorf("并发重入 %d 失败: %v", i, err)
		}
	}
	fence := handles[0].FencingToken()
	for i, h := range handles {
		if h.FencingToken() != fence {
			return fmt.Errorf("重入 %d 改变了栅栏: %d vs %d", i, h.FencingToken(), fence)
		}
	}
	if _, err := lg.TryAcquire(ctx, "cc-re", glock.WithOwner("other")); !errors.Is(err, glock.ErrHeld) {
		return fmt.Errorf("计数=%d 期间他人应被拒, got %v", collideN, err)
	}
	for i, h := range handles {
		if err := wantOK(fmt.Sprintf("释放第 %d 层", i+1), h.Release(ctx)); err != nil {
			return err
		}
	}
	other, err := lg.TryAcquire(ctx, "cc-re", glock.WithOwner("other"))
	if err := wantOK("计数归零后他人可获取", err); err != nil {
		return err
	}
	return wantOK("清理", other.Release(ctx))
}

// ---- 经典场景：README 承诺过的契约，黑盒侧补齐 ----

func demoBlockingExpiryWake(ctx context.Context, _ env, lg glock.Locker) error {
	// 持有者只设短租约、从不 Release：阻塞方须在租约过期后靠接管拿到锁。
	// postgrec 走过期行删除的触发器通知，redisc 走轮询兜底——两个后端机制
	// 不同，同一断言都应通过。
	if _, err := lg.TryAcquire(ctx, "bexpire", glock.WithOwner("holder"), glock.WithLease(600*time.Millisecond)); err != nil {
		return wantOK("holder TryAcquire", err)
	}
	h, err := lg.Acquire(ctx, "bexpire", glock.WithOwner("waiter"), glock.WithRetryInterval(300*time.Millisecond))
	if err := wantOK("阻塞等待方在租约过期后拿到锁", err); err != nil {
		return err
	}
	return wantOK("清理", h.Release(ctx))
}

func demoCtxCancel(ctx context.Context, _ env, lg glock.Locker) error {
	holder, err := lg.TryAcquire(ctx, "cancel", glock.WithOwner("holder"), glock.WithLease(30*time.Second))
	if err := wantOK("holder TryAcquire", err); err != nil {
		return err
	}
	ctx2, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()
	_, aerr := lg.Acquire(ctx2, "cancel", glock.WithOwner("waiter"), glock.WithRetryInterval(50*time.Millisecond))
	if !errors.Is(aerr, glock.ErrHeld) || !errors.Is(aerr, context.DeadlineExceeded) {
		return fmt.Errorf("ctx 取消应同时包装 ErrHeld 与 DeadlineExceeded, got %v", aerr)
	}
	return wantOK("清理", holder.Release(ctx))
}

func demoHandleTerminal(ctx context.Context, _ env, lg glock.Locker) error {
	h, err := lg.TryAcquire(ctx, "ht", glock.WithOwner("me"))
	if err := wantOK("TryAcquire", err); err != nil {
		return err
	}
	if err := wantOK("Release", h.Release(ctx)); err != nil {
		return err
	}
	err = h.Refresh(ctx)
	if !errors.Is(err, glock.ErrLost) {
		return fmt.Errorf("释放后 Refresh: want ErrLost, got %v", err)
	}
	return nil
}

func demoNamespaceIsolation(ctx context.Context, e env, _ glock.Locker) error {
	// 同一后端上两个命名空间取同名 Key：互不阻塞；各自域内仍互斥。
	lgA := e.newLocker(glock.WithNamespace(e.ns + "A/"))
	lgB := e.newLocker(glock.WithNamespace(e.ns + "B/"))
	ha, err := lgA.TryAcquire(ctx, "nsiso", glock.WithOwner("me"))
	if err := wantOK("A 域获取", err); err != nil {
		return err
	}
	hb, err := lgB.TryAcquire(ctx, "nsiso", glock.WithOwner("me"))
	if err := wantOK("B 域同名 Key 不受 A 阻塞", err); err != nil {
		return err
	}
	for _, d := range []struct {
		name string
		lg   glock.Locker
	}{{"A", lgA}, {"B", lgB}} {
		if _, err := d.lg.TryAcquire(ctx, "nsiso", glock.WithOwner("other")); !errors.Is(err, glock.ErrHeld) {
			return fmt.Errorf("%s 域内应仍互斥, got %v", d.name, err)
		}
	}
	if err := wantOK("A 释放", ha.Release(ctx)); err != nil {
		return err
	}
	return wantOK("B 释放", hb.Release(ctx))
}

func demoProbeExpiryResidue(ctx context.Context, _ env, lg glock.Locker) error {
	p, ok := lg.(glock.ProbingLocker)
	if !ok {
		return errors.New("Locker 不提供 Probe（Driver 未实现 ProbingDriver）")
	}
	if _, err := lg.TryAcquire(ctx, "pexp", glock.WithOwner("me"), glock.WithLease(300*time.Millisecond)); err != nil {
		return wantOK("TryAcquire 短租约", err)
	}
	time.Sleep(500 * time.Millisecond) // 过期且无人释放、无人接管
	infos, err := p.Probe(ctx, "pexp")
	if err := wantOK("过期后内省", err); err != nil {
		return err
	}
	if len(infos) != 0 {
		return fmt.Errorf("过期残余不算持有: got %v", infos)
	}
	return nil
}

func demoSpecialKeys(ctx context.Context, _ env, lg glock.Locker) error {
	// 常见踩坑：中文与空格、含 {}（会改变 redisc 的 hash tag 提取）、长 Key。
	// 断言功能语义不受影响：获取、互斥、释放。
	for _, key := range []string{
		"中文 键:示例",
		"brace:{key}:with-tag",
		"long:" + strings.Repeat("k", 200),
	} {
		h, err := lg.TryAcquire(ctx, key, glock.WithOwner("me"))
		if err := wantOK(fmt.Sprintf("获取 %q", key), err); err != nil {
			return err
		}
		if _, err := lg.TryAcquire(ctx, key, glock.WithOwner("other")); !errors.Is(err, glock.ErrHeld) {
			return fmt.Errorf("特殊 Key %q 应互斥, got %v", key, err)
		}
		if err := wantOK(fmt.Sprintf("释放 %q", key), h.Release(ctx)); err != nil {
			return err
		}
	}
	return nil
}
