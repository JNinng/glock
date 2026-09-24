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
