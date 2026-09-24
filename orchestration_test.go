package glock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestLocker(drv *fakeDriver) *BasicLocker {
	return NewBasicLocker(drv, WithNamespace("t:"))
}

func TestTryAcquireMutualExclusion(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h1, err := l.TryAcquire(ctx, "k")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	_, err = l.TryAcquire(ctx, "k", WithOwner("other"))
	if !errors.Is(err, ErrHeld) {
		t.Fatalf("second acquire want ErrHeld, got %v", err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := l.TryAcquire(ctx, "k", WithOwner("other")); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestAcquireBlockingRetriesUntilFreed(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h0, err := l.TryAcquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(250 * time.Millisecond)
		if err := h0.Release(ctx); err != nil {
			t.Error(err)
		}
	}()
	start := time.Now()
	h, err := l.Acquire(ctx, "k", WithOwner("waiter"), WithRetryInterval(50*time.Millisecond))
	if err != nil {
		t.Fatalf("blocking acquire: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("acquired too fast (%s), retry loop suspicious", elapsed)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireCtxDoneWrapsErrHeld(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.TryAcquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_, err := l.Acquire(ctx2, "k", WithOwner("waiter"), WithRetryInterval(30*time.Millisecond))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrHeld+DeadlineExceeded, got %v", err)
	}
}

func TestReentrancySameOwner(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h1, err := l.Acquire(ctx, "k", WithOwner("me"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := l.Acquire(ctx, "k", WithOwner("me"))
	if err != nil {
		t.Fatalf("reentrant acquire: %v", err)
	}
	if h1.FencingToken() != h2.FencingToken() {
		t.Fatalf("reentrant fence mismatch: %d vs %d", h1.FencingToken(), h2.FencingToken())
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := l.TryAcquire(ctx, "k", WithOwner("other")); !errors.Is(err, ErrHeld) {
		t.Fatalf("after one release still held by count, want ErrHeld, got %v", err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if drv.holdsKey("t:k") {
		t.Fatal("record should be gone after both releases")
	}
}

func TestModeConflictWriteThenRead(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.Acquire(ctx, "k", WithOwner("me")); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AcquireRead(ctx, "k", WithOwner("me")); !errors.Is(err, ErrModeConflict) {
		t.Fatalf("want ErrModeConflict, got %v", err)
	}
}

func TestWatchdogKeepsHoldAlive(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h, err := l.Acquire(ctx, "k", WithOwner("me"), WithLease(150*time.Millisecond), WithWatchdog())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond) // 4 个租约周期
	if _, err := l.TryAcquire(ctx, "k", WithOwner("other")); !errors.Is(err, ErrHeld) {
		t.Fatalf("watchdog should keep hold, got %v", err)
	}
	drv.mu.Lock()
	n := drv.refresh
	drv.mu.Unlock()
	if n < 2 {
		t.Fatalf("watchdog refreshed only %d times in 600ms with interval ~50ms", n)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWatchdogMarksLostWhenRecordVanishes(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h, err := l.Acquire(ctx, "k", WithOwner("me"), WithLease(150*time.Millisecond), WithWatchdog())
	if err != nil {
		t.Fatal(err)
	}
	drv.drop("t:k")
	select {
	case <-h.Lost():
	case <-time.After(2 * time.Second):
		t.Fatal("Lost not closed after watchdog refresh failed")
	}
	if err := h.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("release after lost want ErrLost, got %v", err)
	}
	// 丢失后绝不误删：记录由接管者持有，其他 owner 仍可正常竞争。
	if _, err := l.TryAcquire(ctx, "k", WithOwner("other")); err != nil {
		t.Fatalf("after loss other owner should acquire freely: %v", err)
	}
}

func TestLeaseExpiryAutoRelease(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h, err := l.Acquire(ctx, "k", WithOwner("me"), WithLease(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	_ = h
	time.Sleep(200 * time.Millisecond)
	if _, err := l.TryAcquire(ctx, "k", WithOwner("other")); err != nil {
		t.Fatalf("expired lease should auto-release: %v", err)
	}
	// 过期后的释放被拒绝（防误删：不能删别人的记录）。
	if err := h.Release(ctx); !errors.Is(err, ErrLost) || !errors.Is(err, ErrNotOwner) {
		t.Fatalf("stale release want ErrLost+ErrNotOwner, got %v", err)
	}
}

func TestDowngradeTransfersHold(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h, err := l.Acquire(ctx, "k", WithOwner("me"), WithLease(time.Minute), WithWatchdog())
	if err != nil {
		t.Fatal(err)
	}
	rl, err := h.Downgrade(ctx)
	if err != nil {
		t.Fatalf("downgrade: %v", err)
	}
	if rl.FencingToken() != h.FencingToken() {
		t.Fatalf("fence changed on downgrade: %d vs %d", rl.FencingToken(), h.FencingToken())
	}
	// 旧句柄终结。
	if err := h.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("old handle after downgrade want ErrLost, got %v", err)
	}
	// 读模式：其他读者可进，写者仍被挡。
	if _, err := l.AcquireRead(ctx, "k", WithOwner("r2")); err != nil {
		t.Fatalf("other reader after downgrade: %v", err)
	}
	if _, err := l.TryAcquire(ctx, "k", WithOwner("w2")); !errors.Is(err, ErrHeld) {
		t.Fatalf("writer after downgrade want ErrHeld, got %v", err)
	}
}

func TestDowngradeRequiresSingleCount(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.Acquire(ctx, "k", WithOwner("me")); err != nil {
		t.Fatal(err)
	}
	h2, err := l.Acquire(ctx, "k", WithOwner("me"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Downgrade(ctx); !errors.Is(err, ErrModeConflict) {
		t.Fatalf("downgrade with count 2 want ErrModeConflict, got %v", err)
	}
}

func TestMultiLockAllOrNothingRollback(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	// k2 被他人持有。
	if _, err := l.Acquire(ctx, "k2", WithOwner("other"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := l.TryAcquireAll(ctx, []string{"k1", "k2", "k3"}, WithOwner("me")); !errors.Is(err, ErrHeld) {
		t.Fatalf("want ErrHeld, got %v", err)
	}
	// 回滚后 k1/k3 不应残留 me 的持有。
	for _, k := range []string{"k1", "k3"} {
		if _, err := l.TryAcquire(ctx, k, WithOwner("other2")); err != nil {
			t.Fatalf("key %q not rolled back: %v", k, err)
		}
	}
}

func TestMultiLockAcquireAllAndSharedWatchdog(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	ml, err := l.AcquireAll(ctx, []string{"a", "b", "c"}, WithOwner("me"), WithLease(150*time.Millisecond), WithWatchdog())
	if err != nil {
		t.Fatal(err)
	}
	tokens := ml.FencingTokens()
	if len(tokens) != 3 {
		t.Fatalf("want 3 tokens, got %v", tokens)
	}
	time.Sleep(500 * time.Millisecond)
	// watchdog 应保住全部。
	for k := range tokens {
		if _, err := l.TryAcquire(ctx, k, WithOwner("other")); !errors.Is(err, ErrHeld) {
			t.Fatalf("key %q lost by watchdog", k)
		}
	}
	// 一个成员记录消失 → 整组丢失。
	drv.drop("t:a")
	select {
	case <-ml.Lost():
	case <-time.After(2 * time.Second):
		t.Fatal("MultiLock.Lost not closed after one member vanished")
	}
}

func TestLostClosesOnRelease(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h, err := l.Acquire(ctx, "k", WithOwner("me"))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.Lost():
	default:
		t.Fatal("Lost should close after normal release")
	}
}

func TestNewOwnerTokenUnique(t *testing.T) {
	seen := map[string]struct{}{}
	for i := 0; i < 1000; i++ {
		tok := NewOwnerToken()
		if len(tok) != 32 {
			t.Fatalf("token length = %d", len(tok))
		}
		if _, dup := seen[tok]; dup {
			t.Fatal("duplicate token")
		}
		seen[tok] = struct{}{}
	}
}
