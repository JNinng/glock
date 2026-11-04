package glock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLockerDefaultLease(t *testing.T) {
	drv := newFakeDriver()
	l := NewBasicLocker(drv, WithNamespace("t:"), WithDefaultLease(150*time.Millisecond))
	ctx := context.Background()

	// 未传 WithLease：按 Locker 默认 150ms 过期。
	if _, err := l.Acquire(ctx, "k", WithOwner("me")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	if _, err := l.TryAcquire(ctx, "k", WithOwner("other")); err != nil {
		t.Fatalf("acquire without WithLease should use locker default (150ms): %v", err)
	}

	// 单次 WithLease 覆盖 Locker 默认。
	h, err := l.Acquire(ctx, "k2", WithOwner("me"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	if _, err := l.TryAcquire(ctx, "k2", WithOwner("other")); !errors.Is(err, ErrHeld) {
		t.Fatalf("WithLease should override locker default: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestLockerDefaultWatchdog(t *testing.T) {
	drv := newFakeDriver()
	l := NewBasicLocker(drv, WithNamespace("t:"),
		WithDefaultLease(150*time.Millisecond), WithDefaultWatchdog())
	ctx := context.Background()

	h, err := l.Acquire(ctx, "k", WithOwner("me"))
	if err != nil {
		t.Fatal(err)
	}
	// watchdog 保活：远超租约后仍被持有。
	time.Sleep(500 * time.Millisecond)
	if _, err := l.TryAcquire(ctx, "k", WithOwner("other")); !errors.Is(err, ErrHeld) {
		t.Fatalf("default watchdog should keep hold alive: %v", err)
	}
	drv.drop("t:k")
	select {
	case <-h.Lost():
	case <-time.After(2 * time.Second):
		t.Fatal("Lost not closed after watchdog refresh failed")
	}
}

func TestLockerDefaultWatchdogMultiLock(t *testing.T) {
	drv := newFakeDriver()
	l := NewBasicLocker(drv, WithNamespace("t:"),
		WithDefaultLease(150*time.Millisecond), WithDefaultWatchdog())
	ctx := context.Background()

	ml, err := l.AcquireAll(ctx, []string{"a", "b"}, WithOwner("me"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := l.TryAcquire(ctx, "a", WithOwner("other")); !errors.Is(err, ErrHeld) {
		t.Fatalf("default watchdog should keep multi hold alive: %v", err)
	}
	drv.drop("t:b")
	select {
	case <-ml.Lost():
	case <-time.After(2 * time.Second):
		t.Fatal("MultiLock.Lost not closed after one member vanished")
	}
}

func TestLockerDefaultLeaseInvalid(t *testing.T) {
	l := NewBasicLocker(newFakeDriver(), WithDefaultLease(0))
	// 非法默认值的校验时机同 WithLease：在获取时暴露。
	_, err := l.TryAcquire(context.Background(), "k")
	if err == nil || !strings.Contains(err.Error(), "lease must be positive") {
		t.Fatalf("want lease validation error, got %v", err)
	}
}
