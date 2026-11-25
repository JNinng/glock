package locktest

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

// 全新 Locker 被多 goroutine 同时首次使用：初始化（建表、预编译、建连）的
// 并发不得使任何调用失败。各取互不相干的 Key，排除竞争失败。
func testColdStartConcurrent(t *testing.T, env Env) {
	const n = 16
	ctx := context.Background()
	start := make(chan struct{})
	holds := make([]*glock.Lock, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			holds[i], errs[i] = env.Locker.TryAcquire(ctx, fmt.Sprintf("cold%02d", i))
		}()
	}
	close(start)
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Errorf("concurrent first use %d: %v", i, errs[i])
			continue
		}
		if err := holds[i].Release(ctx); err != nil {
			t.Errorf("release %d: %v", i, err)
		}
	}
}

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

func testAcquireMaxRetries(t *testing.T, env Env) {
	ctx := context.Background()
	h, err := env.Locker.TryAcquire(ctx, "mr", glock.WithOwner("holder"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	// 上限用尽：ErrHeld+ErrExhausted，放弃不等 ctx，迅速返回。
	start := time.Now()
	_, err = env.Locker.Acquire(ctx, "mr", glock.WithOwner("waiter"),
		glock.WithRetryInterval(retryFast), glock.WithMaxRetries(2))
	if !errors.Is(err, glock.ErrHeld) || !errors.Is(err, glock.ErrExhausted) {
		t.Fatalf("want ErrHeld+ErrExhausted, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("exhaustion must give up promptly, took %s", elapsed)
	}
	// n=0 即单次尝试（TryAcquire 语义），不进入等待路径。
	start = time.Now()
	_, err = env.Locker.Acquire(ctx, "mr", glock.WithOwner("waiter"), glock.WithMaxRetries(0))
	if !errors.Is(err, glock.ErrHeld) || !errors.Is(err, glock.ErrExhausted) {
		t.Fatalf("n=0 want ErrHeld+ErrExhausted, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("n=0 must not enter the wait path, took %s", elapsed)
	}
	// 上限内锁释放：照常拿到（上限只加放弃路径，不改获取语义）。
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := h.Release(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	h2, err := env.Locker.Acquire(ctx, "mr", glock.WithOwner("waiter"),
		glock.WithRetryInterval(retryFast), glock.WithMaxRetries(20))
	if err != nil {
		t.Fatalf("acquire within retry limit: %v", err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func testAcquireBackoff(t *testing.T, env Env) {
	ctx := context.Background()
	// 封顶低于初始间隔：获取时校验报错。
	h0, err := env.Locker.TryAcquire(ctx, "bk", glock.WithBackoff(50*time.Millisecond, 20*time.Millisecond))
	if err == nil {
		h0.Release(ctx)
		t.Fatal("backoff cap below initial must be rejected")
	}
	// 退避等待下照常拿到（活性）：持有 250ms 后释放，等待者以 10ms→80ms
	// 退避逐轮重试，须在释放后的等待窗口内成功。
	h, err := env.Locker.TryAcquire(ctx, "bk", glock.WithOwner("holder"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan error, 1)
	go func() {
		h2, err := env.Locker.Acquire(context.Background(), "bk",
			glock.WithOwner("waiter"), glock.WithBackoff(10*time.Millisecond, 80*time.Millisecond), glock.WithMaxRetries(100))
		if err != nil {
			acquired <- err
			return
		}
		acquired <- h2.Release(context.Background())
	}()
	time.Sleep(250 * time.Millisecond)
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("backoff acquire did not finish after release")
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
	// 两个竞争者并发请求同一组 Key；AcquireAll 按字典序加锁，应避免交叉死锁。
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

// ---- 信号量（Env.Sem 可选）：槽位即持有，租约/重入/防误删/栅栏与锁一致 ----

func testSemCapacity(t *testing.T, env Env) {
	if env.Sem == nil {
		t.Skip("Env 未提供 Sem：Driver 未实现 glock.SemaphoreDriver")
	}
	ctx := context.Background()
	s1, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("a"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("b"))
	if err != nil {
		t.Fatalf("second slot: %v", err)
	}
	if _, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("c")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("full pool want ErrHeld, got %v", err)
	}
	// 重入也占槽位：满员时既有持有者再取同样被拒。
	if _, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("a")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("reentrant at full pool want ErrHeld, got %v", err)
	}
	if err := s2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	s3, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("c"))
	if err != nil {
		t.Fatalf("slot freed by release: %v", err)
	}
	if err := s1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s3.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// 全部释放后再取（容量一致）：池可复用。
	s4, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("d"))
	if err != nil {
		t.Fatalf("acquire after pool emptied: %v", err)
	}
	if err := s4.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func testSemReentrantSlots(t *testing.T, env Env) {
	if env.Sem == nil {
		t.Skip("Env 未提供 Sem：Driver 未实现 glock.SemaphoreDriver")
	}
	ctx := context.Background()
	s1, err := env.Sem.AcquireSemaphore(ctx, "sk", 2, glock.WithOwner("me"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := env.Sem.AcquireSemaphore(ctx, "sk", 2, glock.WithOwner("me"))
	if err != nil {
		t.Fatalf("reentrant slot: %v", err)
	}
	if s1.FencingToken() != s2.FencingToken() {
		t.Fatalf("reentrant fence changed: %d vs %d", s1.FencingToken(), s2.FencingToken())
	}
	if _, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("other")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("2/2 slots used, want ErrHeld, got %v", err)
	}
	// 释放一层后有空槽：other 进入占满；me 释放最后一层后持有条目消失，
	// 再取是全新获取事件，栅栏递增。
	if err := s1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	o1, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("other"))
	if err != nil {
		t.Fatalf("other should take freed slot: %v", err)
	}
	if err := s2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	s3, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("me"))
	if err != nil {
		t.Fatalf("re-acquire after holder drained: %v", err)
	}
	if s3.FencingToken() <= s1.FencingToken() {
		t.Fatalf("fresh acquisition fence %d should exceed stale %d", s3.FencingToken(), s1.FencingToken())
	}
	if err := o1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s3.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func testSemCapacityMismatch(t *testing.T, env Env) {
	if env.Sem == nil {
		t.Skip("Env 未提供 Sem：Driver 未实现 glock.SemaphoreDriver")
	}
	ctx := context.Background()
	s, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("me"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 3, glock.WithOwner("other")); !errors.Is(err, glock.ErrCapacityMismatch) {
		t.Fatalf("want ErrCapacityMismatch, got %v", err)
	}
	// 同一声明不受影响；阻塞获取对容量不符也立即失败（不可等待自愈）。
	o, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("other"))
	if err != nil {
		t.Fatalf("same declaration must still work, got %v", err)
	}
	ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := env.Sem.AcquireSemaphore(ctx2, "sk", 3, glock.WithOwner("third")); !errors.Is(err, glock.ErrCapacityMismatch) {
		t.Fatalf("blocking mismatch want ErrCapacityMismatch, got %v", err)
	}
	if err := o.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// 全部槽位过期清空后：容量可重新声明。
	s2, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 2, glock.WithOwner("me"), glock.WithLease(leaseFast))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(holdForDag)
	s3, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 5, glock.WithOwner("me"))
	if err != nil {
		t.Fatalf("re-declare after pool emptied: %v", err)
	}
	if err := s3.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s2.Release(ctx); !errors.Is(err, glock.ErrLost) {
		t.Fatalf("expired slot release want ErrLost, got %v", err)
	}
}

func testSemLeaseAutoRelease(t *testing.T, env Env) {
	if env.Sem == nil {
		t.Skip("Env 未提供 Sem：Driver 未实现 glock.SemaphoreDriver")
	}
	ctx := context.Background()
	s, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 1, glock.WithOwner("me"), glock.WithLease(leaseFast))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(holdForDag)
	// 槽位随租约过期自动腾出（容量 1：他人可接管）。
	w, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 1, glock.WithOwner("other"))
	if err != nil {
		t.Fatalf("expired slot should auto-release: %v", err)
	}
	// 过期句柄的释放被拒绝（防误删：不能删别人的槽位记录）。
	if err := s.Release(ctx); !errors.Is(err, glock.ErrLost) || !errors.Is(err, glock.ErrNotOwner) {
		t.Fatalf("stale release want ErrLost+ErrNotOwner, got %v", err)
	}
	if err := w.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func testSemFencing(t *testing.T, env Env) {
	if env.Sem == nil {
		t.Skip("Env 未提供 Sem：Driver 未实现 glock.SemaphoreDriver")
	}
	ctx := context.Background()
	// 全新持有者是获取事件：栅栏严格递增；重入沿用栅栏（留一空槽给重入）。
	var prev uint64
	handles := make([]*glock.SemaphoreLock, 0, 3)
	for _, owner := range []string{"a", "b", "c"} {
		s, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 4, glock.WithOwner(owner), glock.WithLease(leaseLong))
		if err != nil {
			t.Fatal(err)
		}
		if s.FencingToken() <= prev {
			t.Fatalf("fence %d not increasing (prev %d)", s.FencingToken(), prev)
		}
		prev = s.FencingToken()
		handles = append(handles, s)
	}
	a2, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 4, glock.WithOwner("a"))
	if err != nil {
		t.Fatalf("reentrant slot: %v", err)
	}
	if a2.FencingToken() != handles[0].FencingToken() {
		t.Fatalf("reentrant fence mismatch: %d vs %d", a2.FencingToken(), handles[0].FencingToken())
	}
	for _, s := range handles {
		if err := s.Release(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := a2.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func testSemBlockingAndCtx(t *testing.T, env Env) {
	if env.Sem == nil {
		t.Skip("Env 未提供 Sem：Driver 未实现 glock.SemaphoreDriver")
	}
	ctx := context.Background()
	// 阻塞获取：槽位腾出后在时限内拿到（Driver 实现 NotifyingDriver 时靠信号，
	// 否则靠轮询——同一断言都应通过）。
	h, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 1, glock.WithOwner("holder"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		s, err := env.Sem.AcquireSemaphore(ctx, "sk", 1, glock.WithOwner("waiter"), glock.WithRetryInterval(retryFast))
		if err != nil {
			done <- err
			return
		}
		done <- s.Release(ctx)
	}()
	time.Sleep(200 * time.Millisecond)
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocking semaphore acquire did not finish after release")
	}

	// ctx 结束：返回同时包装 ErrHeld 与 ctx 错误。
	if _, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 1, glock.WithOwner("holder"), glock.WithLease(leaseLong)); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
	defer cancel()
	_, err = env.Sem.AcquireSemaphore(ctx2, "sk", 1, glock.WithOwner("waiter"), glock.WithRetryInterval(20*time.Millisecond))
	if !errors.Is(err, glock.ErrHeld) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrHeld+DeadlineExceeded, got %v", err)
	}
}

func testSemWatchdogLost(t *testing.T, env Env) {
	if env.Sem == nil {
		t.Skip("Env 未提供 Sem：Driver 未实现 glock.SemaphoreDriver")
	}
	ctx := context.Background()
	s, err := env.Sem.AcquireSemaphore(ctx, "sk", 1, glock.WithOwner("me"), glock.WithLease(leaseFast), glock.WithWatchdog())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(holdForDag) // 越过原始租约：保活只能靠 watchdog
	if _, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 1, glock.WithOwner("other")); !errors.Is(err, glock.ErrHeld) {
		t.Fatalf("watchdog should keep slot, got %v", err)
	}
	if err := env.Saboteur.Drop("sk"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.Lost():
	case <-time.After(3 * time.Second):
		t.Fatal("Lost not closed after watchdog refresh failed")
	}
	if err := s.Release(ctx); !errors.Is(err, glock.ErrLost) {
		t.Fatalf("release after lost want ErrLost, got %v", err)
	}
	if _, err := env.Sem.TryAcquireSemaphore(ctx, "sk", 1, glock.WithOwner("other"), glock.WithLease(leaseFast)); err != nil {
		t.Fatalf("after loss other owner should acquire freely: %v", err)
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

func testNotifyRelease(t *testing.T, env Env) {
	if env.Notifier == nil {
		t.Skip("Env 未提供 Notifier：Driver 未实现 glock.NotifyingDriver")
	}
	ctx := context.Background()

	// 释放信号唤醒：轮询间隔 10s——纯轮询不可能在时限内醒来，通过即证明
	// 靠信号。唤醒后重走 TryAcquire 的语义也被覆盖（信号不直接授予锁）。
	h0, err := env.Locker.TryAcquire(ctx, "nk", glock.WithOwner("holder"), glock.WithLease(leaseLong))
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan struct{})
	go func() {
		h, err := env.Locker.Acquire(ctx, "nk", glock.WithOwner("waiter"), glock.WithRetryInterval(10*time.Second))
		if err != nil {
			t.Error(err)
			return
		}
		close(acquired)
		if err := h.Release(ctx); err != nil {
			t.Error(err)
		}
	}()
	// 留足订阅时窗再释放：信号是尽力而为的，漏收会退化为 10s 轮询导致超时。
	time.Sleep(250 * time.Millisecond)
	if err := h0.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking acquire did not wake on release (poll interval 10s)")
	}

	// ctx 结束必须立即穿透等待预算，不等满轮询间隔。
	if _, err := env.Locker.TryAcquire(ctx, "nc", glock.WithOwner("holder"), glock.WithLease(leaseLong)); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = env.Locker.Acquire(ctx2, "nc", glock.WithOwner("waiter"), glock.WithRetryInterval(10*time.Second))
	if !errors.Is(err, glock.ErrHeld) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrHeld+DeadlineExceeded, got %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("ctx done must return promptly, took %s", time.Since(start))
	}
}
