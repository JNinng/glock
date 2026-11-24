package glock

import (
	"context"
	"errors"
	"testing"
	"time"
)

var _ SemaphoreDriver = (*fakeDriver)(nil)

func TestSemaphoreCapacityBound(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	s1, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("a"))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("b"))
	if err != nil {
		t.Fatalf("second slot: %v", err)
	}
	if _, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("c")); !errors.Is(err, ErrHeld) {
		t.Fatalf("full pool want ErrHeld, got %v", err)
	}
	// 重入也占槽位：满员时既有持有者再取同样被拒。
	if _, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("a")); !errors.Is(err, ErrHeld) {
		t.Fatalf("reentrant at full pool want ErrHeld, got %v", err)
	}
	if err := s2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	s3, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("c"))
	if err != nil {
		t.Fatalf("slot freed by release: %v", err)
	}
	if err := s1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s3.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if drv.holdsKey("t:pool") {
		t.Fatal("semaphore record should be gone after all slots released")
	}
}

func TestSemaphoreReentrantSlots(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	s1, err := l.AcquireSemaphore(ctx, "pool", 2, WithOwner("me"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := l.AcquireSemaphore(ctx, "pool", 2, WithOwner("me"))
	if err != nil {
		t.Fatalf("reentrant slot: %v", err)
	}
	if s1.FencingToken() != s2.FencingToken() {
		t.Fatalf("reentrant fence changed: %d vs %d", s1.FencingToken(), s2.FencingToken())
	}
	if _, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("other")); !errors.Is(err, ErrHeld) {
		t.Fatalf("2/2 slots used, want ErrHeld, got %v", err)
	}
	// 释放一层后有空槽：other 进入占满。
	if err := s1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	o1, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("other"))
	if err != nil {
		t.Fatalf("other should take freed slot: %v", err)
	}
	// me 释放最后一层：持有条目消失；再取是全新获取事件，栅栏递增。
	if err := s2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	s3, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("me"))
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
	if drv.holdsKey("t:pool") {
		t.Fatal("semaphore record should be gone after all slots released")
	}
}

func TestSemaphoreCapacityMismatch(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	s, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("me"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.TryAcquireSemaphore(ctx, "pool", 3, WithOwner("other")); !errors.Is(err, ErrCapacityMismatch) {
		t.Fatalf("want ErrCapacityMismatch, got %v", err)
	}
	// 同一声明不受影响：other 仍按 n=2 进入空槽（而非容量不符）。
	o, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("other"))
	if err != nil {
		t.Fatalf("same declaration must still work, got %v", err)
	}
	if err := o.Release(ctx); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := l.AcquireSemaphore(ctx2, "pool", 3, WithOwner("other")); !errors.Is(err, ErrCapacityMismatch) {
		t.Fatalf("blocking mismatch want ErrCapacityMismatch, got %v", err)
	}
	if err := s.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// 全部槽位过期清空后：容量可重新声明。
	s2, err := l.TryAcquireSemaphore(ctx, "pool", 2, WithOwner("me"), WithLease(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	s3, err := l.TryAcquireSemaphore(ctx, "pool", 5, WithOwner("me"))
	if err != nil {
		t.Fatalf("re-declare after pool emptied: %v", err)
	}
	if err := s3.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s2.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("expired slot release want ErrLost, got %v", err)
	}
}

func TestSemaphoreLeaseExpiryAutoRelease(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	s, err := l.TryAcquireSemaphore(ctx, "pool", 1, WithOwner("me"), WithLease(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	// 槽位随租约过期自动腾出（容量 1：他人可接管）。
	w, err := l.TryAcquireSemaphore(ctx, "pool", 1, WithOwner("other"))
	if err != nil {
		t.Fatalf("expired slot should auto-release: %v", err)
	}
	// 过期句柄的释放被拒绝（防误删）。
	if err := s.Release(ctx); !errors.Is(err, ErrLost) || !errors.Is(err, ErrNotOwner) {
		t.Fatalf("stale release want ErrLost+ErrNotOwner, got %v", err)
	}
	if err := w.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSemaphoreBlockingWakesOnReleaseSignal(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h, err := l.TryAcquireSemaphore(ctx, "pool", 1, WithOwner("holder"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan struct{})
	go func() {
		// 轮询间隔 10s：在时限内醒来只能靠槽位腾出的信号。
		s, err := l.AcquireSemaphore(ctx, "pool", 1, WithOwner("waiter"), WithRetryInterval(10*time.Second))
		if err != nil {
			t.Error(err)
			return
		}
		close(acquired)
		if err := s.Release(ctx); err != nil {
			t.Error(err)
		}
	}()
	waitForParked(t, drv)
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-acquired:
	case <-time.After(3 * time.Second):
		t.Fatal("blocking semaphore acquire did not wake on release signal")
	}
}

func TestSemaphoreCtxDoneWrapsErrHeld(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.TryAcquireSemaphore(ctx, "pool", 1, WithOwner("holder"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_, err := l.AcquireSemaphore(ctx2, "pool", 1, WithOwner("waiter"), WithRetryInterval(30*time.Millisecond))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrHeld+DeadlineExceeded, got %v", err)
	}
}

func TestSemaphoreWatchdogKeepsSlot(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	s, err := l.AcquireSemaphore(ctx, "pool", 1, WithOwner("me"), WithLease(150*time.Millisecond), WithWatchdog())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond) // 4 个租约周期
	if _, err := l.TryAcquireSemaphore(ctx, "pool", 1, WithOwner("other")); !errors.Is(err, ErrHeld) {
		t.Fatalf("watchdog should keep slot, got %v", err)
	}
	// 记录消失 → 槽位丢失。
	drv.drop("t:pool")
	select {
	case <-s.Lost():
	case <-time.After(2 * time.Second):
		t.Fatal("Lost not closed after watchdog refresh failed")
	}
	if err := s.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("release after lost want ErrLost, got %v", err)
	}
	if _, err := l.TryAcquireSemaphore(ctx, "pool", 1, WithOwner("other")); err != nil {
		t.Fatalf("after loss other owner should acquire freely: %v", err)
	}
}

func TestSemaphoreInvalidCapacity(t *testing.T) {
	l := newTestLocker(newFakeDriver())
	for _, n := range []int{0, -1} {
		if _, err := l.TryAcquireSemaphore(context.Background(), "pool", n); err == nil || errors.Is(err, ErrUnsupported) {
			t.Fatalf("n=%d want validation error, got %v", n, err)
		}
	}
}

func TestSemaphoreUnsupportedDriver(t *testing.T) {
	l := NewBasicLocker(coreOnlyDriver{drv: newFakeDriver()}, WithNamespace("t:"))
	if _, err := l.TryAcquireSemaphore(context.Background(), "pool", 1); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("want ErrUnsupported, got %v", err)
	}
	if _, err := l.AcquireSemaphore(context.Background(), "pool", 1); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("want ErrUnsupported, got %v", err)
	}
}
