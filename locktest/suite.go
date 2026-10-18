package locktest

import (
	"context"
	"errors"
	"testing"
	"time"

	glock "github.com/jninng/glock"
)

const (
	// 短租约与短轮询控制套件时长；watchdog 间隔 = 租约/3。
	leaseFast  = 180 * time.Millisecond
	retryFast  = 40 * time.Millisecond
	leaseLong  = 30 * time.Second
	holdForDag = 400 * time.Millisecond
)

func testMutex(t *testing.T, env Env) {
	ctx := context.Background()
	h, err := env.Locker.TryAcquire(ctx, "k")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("other")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("second acquire want ErrHeld, got %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("other"), glock.WithLease(leaseFast)); err != nil {
		t.Fatalf("after release: %v", err)
	}
}

func testAcquireBlockingRetries(t *testing.T, env Env) {
	ctx := context.Background()
	h0, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("holder"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(250 * time.Millisecond)
		if err := h0.Release(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	start := time.Now()
	h, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("waiter"), glock.WithRetryInterval(retryFast))
	if err != nil {
		t.Fatalf("blocking acquire: %v", err)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("acquired too fast; retry loop suspicious")
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func testAcquireCtxDone(t *testing.T, env Env) {
	ctx := context.Background()
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("holder"), glock.WithLease(leaseFast)); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
	defer cancel()
	_, err := env.Locker.Acquire(ctx2, "k", glock.WithOwner("waiter"), glock.WithRetryInterval(20*time.Millisecond))
	if !errors.Is(err, glock.ErrHeld) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrHeld+DeadlineExceeded, got %v", err)
	}
}

func testLeaseAutoRelease(t *testing.T, env Env) {
	ctx := context.Background()
	h, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("me"), glock.WithLease(leaseFast))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(holdForDag)
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("other")); err != nil {
		t.Fatalf("expired lease should auto-release: %v", err)
	}
	// 过期句柄的释放必须被拒绝：不能删掉新持有者的记录（防误删）。
	err = h.Release(ctx)
	if !errors.Is(err, glock.ErrLost) || !errors.Is(err, glock.ErrNotOwner) {
		t.Fatalf("stale release want ErrLost+ErrNotOwner, got %v", err)
	}
}

func testFencingMonotonic(t *testing.T, env Env) {
	ctx := context.Background()
	var prev uint64
	for i := 0; i < 3; i++ {
		for _, owner := range []string{"a", "b"} {
			h, err := env.Locker.Acquire(ctx, "k", glock.WithOwner(owner), glock.WithRetryInterval(retryFast))
			if err != nil {
				t.Fatal(err)
			}
			if h.FencingToken() <= prev {
				t.Fatalf("fence %d not increasing (prev %d)", h.FencingToken(), prev)
			}
			prev = h.FencingToken()
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func testSafeReleaseStaleFence(t *testing.T, env Env) {
	ctx := context.Background()
	h1, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("x"), glock.WithLease(leaseFast))
	if err != nil {
		t.Fatal(err)
	}
	f1 := h1.FencingToken()
	time.Sleep(holdForDag) // 租约过期，无接管者
	h2, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("x"), glock.WithLease(leaseFast))
	if err != nil {
		t.Fatalf("re-acquire same owner after expiry: %v", err)
	}
	if h2.FencingToken() <= f1 {
		t.Fatalf("fresh acquire fence %d should exceed stale %d", h2.FencingToken(), f1)
	}
	// 旧代际句柄的操作被拒绝，新持有不受影响（ADR-0003）。
	if err := h1.Release(ctx); !errors.Is(err, glock.ErrNotOwner) {
		t.Fatalf("stale-fence release want ErrNotOwner, got %v", err)
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("y")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("h2 must still hold after stale release: %v", err)
	}
}

func testReentrancy(t *testing.T, env Env) {
	ctx := context.Background()
	h1, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("me"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("me"))
	if err != nil {
		t.Fatalf("reentrant: %v", err)
	}
	if h1.FencingToken() != h2.FencingToken() {
		t.Fatalf("reentrant fence changed: %d vs %d", h1.FencingToken(), h2.FencingToken())
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("other")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("count must still be held: %v", err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("other")); err != nil {
		t.Fatalf("free after both releases: %v", err)
	}
}

func testWALRecovery(t *testing.T, env Env) {
	ctx := context.Background()
	// 业务先取 token、持久化，再获取（消除先获取后持久化的窗口）。
	token := glock.NewOwnerToken()
	h1, err := env.Locker.Acquire(ctx, "k", glock.WithOwner(token), glock.WithLease(leaseLong), glock.WithWatchdog())
	if err != nil {
		t.Fatal(err)
	}
	if h1.OwnerToken() != token {
		t.Fatalf("OwnerToken mismatch: %q vs %q", h1.OwnerToken(), token)
	}
	// 崩溃重启后以 WAL 里的 token 重新获取：租约存活 → 重入。
	h2, err := env.Locker.Acquire(ctx, "k", glock.WithOwner(token))
	if err != nil {
		t.Fatalf("recovery acquire: %v", err)
	}
	if h2.FencingToken() != h1.FencingToken() {
		t.Fatalf("reentrant fence mismatch")
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("other")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("other owner must be blocked: %v", err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func testReadShared(t *testing.T, env Env) {
	ctx := context.Background()
	r1, err := env.RW.AcquireRead(ctx, "k", glock.WithOwner("r1"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := env.RW.AcquireRead(ctx, "k", glock.WithOwner("r2"))
	if err != nil {
		t.Fatalf("second reader should share: %v", err)
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("w")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("writer must be blocked by readers: %v", err)
	}
	if err := r1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("w"), glock.WithLease(leaseFast)); err != nil {
		t.Fatalf("writer after readers gone: %v", err)
	}
}

func testWriteThenReadConflict(t *testing.T, env Env) {
	ctx := context.Background()
	if _, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("me"), glock.WithLease(leaseFast)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.RW.AcquireRead(ctx, "k", glock.WithOwner("me")); !errors.Is(err, glock.ErrModeConflict) {
		t.Fatalf("want ErrModeConflict, got %v", err)
	}
}

func testReadThenWriteConflict(t *testing.T, env Env) {
	ctx := context.Background()
	if _, err := env.RW.AcquireRead(ctx, "k", glock.WithOwner("me"), glock.WithLease(leaseFast)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("me")); !errors.Is(err, glock.ErrModeConflict) {
		t.Fatalf("want ErrModeConflict, got %v", err)
	}
}

func testDowngrade(t *testing.T, env Env) {
	ctx := context.Background()
	h, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("me"), glock.WithLease(leaseFast), glock.WithWatchdog())
	if err != nil {
		t.Fatal(err)
	}
	rl, err := h.Downgrade(ctx)
	if err != nil {
		t.Fatalf("downgrade: %v", err)
	}
	if rl.FencingToken() != h.FencingToken() {
		t.Fatal("fence must not change on downgrade")
	}
	if err := h.Release(ctx); !errors.Is(err, glock.ErrLost) {
		t.Fatalf("old handle after downgrade want ErrLost, got %v", err)
	}
	// 降级后是读持有：其他读者可进，写者仍被挡。
	if _, err := env.RW.AcquireRead(ctx, "k", glock.WithOwner("r2")); err != nil {
		t.Fatalf("other reader after downgrade: %v", err)
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("w2")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("writer after downgrade want ErrHeld, got %v", err)
	}
	// 降级后的读持有由重启的 watchdog 保活。
	time.Sleep(holdForDag * 2)
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("w3")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("downgraded read must stay alive via watchdog: %v", err)
	}
	if err := rl.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func testDowngradeCountGuard(t *testing.T, env Env) {
	ctx := context.Background()
	if _, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("me"), glock.WithLease(leaseFast)); err != nil {
		t.Fatal(err)
	}
	h2, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("me"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Downgrade(ctx); !errors.Is(err, glock.ErrModeConflict) {
		t.Fatalf("downgrade with count 2 want ErrModeConflict, got %v", err)
	}
}

func testMixedLeaseReadHold(t *testing.T, env Env) {
	ctx := context.Background()
	// 长租约读者 A 先入场；短租约读者 B 加入、续约后离开。
	// B 的任何操作不得缩短 A 的服务端存活窗口（后端若把共享记录的
	// TTL 重置为 2×B 的租约，A 会在本测试的等待期内被提前回收）。
	a, err := env.RW.AcquireRead(ctx, "k", glock.WithOwner("a"), glock.WithLease(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	b, err := env.RW.AcquireRead(ctx, "k", glock.WithOwner("b"), glock.WithLease(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Release(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)

	// A 仍持有：写者被挡，A 可正常释放。
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("w")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("writer must stay blocked by A after B (200ms lease) left: %v", err)
	}
	if err := a.Release(ctx); err != nil {
		t.Fatalf("A (30s lease) lost its hold after B left: %v", err)
	}
}

func testDowngradeAfterExpiry(t *testing.T, env Env) {
	ctx := context.Background()
	// 写计数 2 的持有整体过期后，降级必须按"持有已丢失"拒绝，
	// 而不是按"计数大于 1"给出模式冲突。
	h1, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("me"), glock.WithLease(leaseFast))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("me"), glock.WithLease(leaseFast))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(holdForDag)
	if _, err := h2.Downgrade(ctx); !errors.Is(err, glock.ErrLost) {
		t.Fatalf("downgrade after expiry want ErrLost, got %v", err)
	}
	// 记录已整体过期：新持有者可接管。
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("other"), glock.WithLease(leaseFast)); err != nil {
		t.Fatalf("expired record must be takeover-able: %v", err)
	}
	if err := h1.Release(ctx); !errors.Is(err, glock.ErrLost) {
		t.Fatalf("stale release want ErrLost, got %v", err)
	}
}

func testMultiAllOrNothing(t *testing.T, env Env) {
	ctx := context.Background()
	if _, err := env.Locker.Acquire(ctx, "k2", glock.WithOwner("other"), glock.WithLease(leaseFast)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Multi.TryAcquireAll(ctx, []string{"k1", "k2", "k3"}, glock.WithOwner("me")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("want ErrHeld, got %v", err)
	}
	// 回滚后 k1/k3 不残留 me 的持有。
	for _, k := range []string{"k1", "k3"} {
		if _, err := env.Locker.TryAcquire(ctx, k, glock.WithOwner("third"), glock.WithLease(leaseFast)); err != nil {
			t.Fatalf("key %q not rolled back: %v", k, err)
		}
	}
}

func testMultiSharedWatchdog(t *testing.T, env Env) {
	ctx := context.Background()
	// 先让 other 挡住 a，200ms 后放行，验证阻塞全量获取。
	blocker, err := env.Locker.Acquire(ctx, "a", glock.WithOwner("other"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	mlCh := make(chan *glock.MultiLock, 1)
	go func() {
		ml, err := env.Multi.AcquireAll(ctx, []string{"a", "b", "c"}, glock.WithOwner("me"),
			glock.WithLease(leaseFast), glock.WithWatchdog(), glock.WithRetryInterval(retryFast))
		if err != nil {
			t.Error(err)
			close(mlCh)
			return
		}
		mlCh <- ml
	}()
	time.Sleep(200 * time.Millisecond)
	if err := blocker.Release(ctx); err != nil {
		t.Fatal(err)
	}
	ml := <-mlCh
	if ml == nil {
		t.Fatal("AcquireAll failed")
	}
	if len(ml.FencingTokens()) != 3 {
		t.Fatalf("want 3 fences, got %v", ml.FencingTokens())
	}
	// 整组 watchdog 保活。
	time.Sleep(holdForDag * 2)
	for k := range ml.FencingTokens() {
		if _, err := env.Locker.TryAcquire(ctx, k, glock.WithOwner("x")); !errors.Is(err, glock.ErrHeld) {
			t.Fatalf("key %q lost by shared watchdog", k)
		}
	}
	// 一个成员消失 → 整组丢失。
	if err := env.Saboteur.Drop("b"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ml.Lost():
	case <-time.After(3 * time.Second):
		t.Fatal("MultiLock.Lost not closed after member dropped")
	}
}

func testMultiNoCrossDeadlock(t *testing.T, env Env) {
	ctx := context.Background()
	// 两个竞争者以不同顺序请求同一组 Key；字典序获取应避免死锁。
	done := make(chan error, 2)
	acquire := func(owner string) {
		ml, err := env.Multi.AcquireAll(ctx, []string{"d1", "d2", "d3"}, glock.WithOwner(owner),
			glock.WithLease(leaseLong), glock.WithRetryInterval(20*time.Millisecond))
		if err != nil {
			done <- err
			return
		}
		done <- ml.Release(context.Background())
	}
	go acquire("p1")
	go acquire("p2")
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("deadlock or failure: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("cross deadlock: AcquireAll did not finish in 10s")
		}
	}
}

func testWatchdogLostSingle(t *testing.T, env Env) {
	ctx := context.Background()
	h, err := env.Locker.Acquire(ctx, "k", glock.WithOwner("me"), glock.WithLease(leaseFast), glock.WithWatchdog())
	if err != nil {
		t.Fatal(err)
	}
	if err := env.Saboteur.Drop("k"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.Lost():
	case <-time.After(3 * time.Second):
		t.Fatal("Lost not closed after watchdog refresh failed")
	}
	if err := h.Release(ctx); !errors.Is(err, glock.ErrLost) {
		t.Fatalf("release after lost want ErrLost, got %v", err)
	}
	if _, err := env.Locker.TryAcquire(ctx, "k", glock.WithOwner("other"), glock.WithLease(leaseFast)); err != nil {
		t.Fatalf("lock must be free after loss: %v", err)
	}
}

func testProbe(t *testing.T, env Env) {
	if env.Prober == nil {
		t.Skip("Env 未提供 Prober：Driver 未实现 glock.ProbingDriver")
	}
	ctx := context.Background()
	p := env.Prober

	// 无持有：空列表而非错误。
	if got, err := p.Probe(ctx, "pk"); err != nil || len(got) != 0 {
		t.Fatalf("probe fresh key = %v, %v; want empty list", got, err)
	}

	// 写持有：owner/mode/fence 逐项对上，剩余租约为正。
	h, err := env.Locker.TryAcquire(ctx, "pk", glock.WithOwner("me"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Probe(ctx, "pk")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("probe write hold = %+v; want exactly one entry", got)
	}
	if info := got[0]; info.Owner != "me" || info.Mode != glock.Write ||
		info.Fence != h.FencingToken() || info.Remaining <= 0 {
		t.Fatalf("bad hold info %+v (want owner me, write, fence %d, positive remaining)",
			info, h.FencingToken())
	}

	// 只读：反复 Probe 不影响互斥，也不给持有续期。
	for i := 0; i < 3; i++ {
		if _, err := p.Probe(ctx, "pk"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := env.Locker.TryAcquire(ctx, "pk", glock.WithOwner("other"), glock.WithLease(leaseLong)); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("probe must not disturb mutual exclusion: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := p.Probe(ctx, "pk"); err != nil || len(got) != 0 {
		t.Fatalf("probe after release = %v, %v; want empty list", got, err)
	}

	// 读持有：多读者全部列出，按 Owner 排序。
	hr1, err := env.RW.AcquireRead(ctx, "pk", glock.WithOwner("r2"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	defer hr1.Release(ctx)
	hr2, err := env.RW.AcquireRead(ctx, "pk", glock.WithOwner("r1"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	defer hr2.Release(ctx)
	got, err = p.Probe(ctx, "pk")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Owner != "r1" || got[1].Owner != "r2" ||
		got[0].Mode != glock.Read || got[1].Mode != glock.Read ||
		got[0].Remaining <= 0 || got[1].Remaining <= 0 {
		t.Fatalf("probe read holds = %+v; want [r1 r2] both read with positive remaining", got)
	}

	// 过期即消失：短租约到期（无人释放）后不再列出。
	he, err := env.Locker.Acquire(ctx, "px", glock.WithOwner("me"), glock.WithLease(leaseFast))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Probe(ctx, "px"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(holdForDag)
	got, err = p.Probe(ctx, "px")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("probe after expiry = %+v; want empty list (expired holds are not live)", got)
	}
	// 上面 Probe 过的租约确实到期了：Probe 没有替它续期。
	if _, err := env.Locker.TryAcquire(ctx, "px", glock.WithOwner("other"), glock.WithLease(leaseFast)); err != nil {
		t.Fatalf("probe must not extend lease: %v", err)
	}
	if err := he.Release(ctx); !errors.Is(err, glock.ErrLost) {
		t.Fatalf("stale release after expiry want ErrLost, got %v", err)
	}
}
