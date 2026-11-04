package glock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithLockRunsAndReleases(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	var got *Lock
	err := l.WithLock(ctx, "k", func(_ context.Context, lk *Lock) error {
		got = lk
		return nil
	}, WithOwner("me"))
	if err != nil {
		t.Fatalf("WithLock: %v", err)
	}
	if got == nil || got.FencingToken() == 0 || got.OwnerToken() != "me" {
		t.Fatalf("fn did not receive a live handle: %+v", got)
	}
	if drv.holdsKey("t:k") {
		t.Fatal("record should be released after fn returns")
	}
	if _, err := l.TryAcquire(ctx, "k", WithOwner("other")); err != nil {
		t.Fatalf("acquire after WithLock: %v", err)
	}
}

func TestWithLockAcquireFailureSkipsFn(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.Acquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	lockCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	called := false
	err := l.WithLock(lockCtx, "k", func(context.Context, *Lock) error {
		called = true
		return nil
	}, WithRetryInterval(20*time.Millisecond))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrHeld+DeadlineExceeded, got %v", err)
	}
	if called {
		t.Fatal("fn must not run when acquire fails")
	}
}

func TestWithLockReleasesOnFnError(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	sentinel := errors.New("boom")
	err := l.WithLock(ctx, "k", func(context.Context, *Lock) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("want fn error, got %v", err)
	}
	if drv.holdsKey("t:k") {
		t.Fatal("record should be released after fn error")
	}
}

// releaseCtxDriver 记录 Release 是否收到已取消的 ctx：fakeDriver 忽略 ctx，
// 无法验证 WithLock 的释放不依赖调用方取消。
type releaseCtxDriver struct {
	*fakeDriver
	sawCanceledCtx bool
}

func (d *releaseCtxDriver) Release(ctx context.Context, key string, mode Mode, owner string, fence uint64) error {
	if ctx.Err() != nil {
		d.sawCanceledCtx = true
		return ctx.Err()
	}
	return d.fakeDriver.Release(ctx, key, mode, owner, fence)
}

func TestWithLockReleasesAfterCallerCancel(t *testing.T) {
	drv := &releaseCtxDriver{fakeDriver: newFakeDriver()}
	l := NewBasicLocker(drv, WithNamespace("t:"))
	ctx, cancel := context.WithCancel(context.Background())

	var cause error
	err := l.WithLock(ctx, "k", func(fnCtx context.Context, _ *Lock) error {
		cancel()
		<-fnCtx.Done()
		cause = context.Cause(fnCtx)
		return fnCtx.Err()
	})
	if !errors.Is(cause, context.Canceled) {
		t.Fatalf("caller cancel should keep its cause, got %v", cause)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if errors.Is(err, ErrLost) {
		t.Fatalf("caller cancel must not be reported as lost: %v", err)
	}
	if drv.sawCanceledCtx {
		t.Fatal("Release must not be called with a canceled context")
	}
	if drv.holdsKey("t:k") {
		t.Fatal("release must not depend on the caller ctx")
	}
}

func TestWithLockReportsLossEvenIfReleaseSucceeds(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	err := l.WithLock(ctx, "k", func(_ context.Context, lk *Lock) error {
		// 模拟本地已判定丢失、后端记录实际仍在（如续期传输错误后的保守
		// 丢失判定）：释放会成功，但 fn 期间已失去互斥保证，必须上报。
		lk.h.markLost(ctx, errors.New("simulated transport failure"))
		return nil
	})
	if !errors.Is(err, ErrLost) {
		t.Fatalf("want ErrLost, got %v", err)
	}
	if drv.holdsKey("t:k") {
		t.Fatal("release should still run and free the record")
	}
}

func TestWithLockLossCancelsFnCtx(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	var cause error
	err := l.WithLock(ctx, "k", func(fnCtx context.Context, _ *Lock) error {
		drv.drop("t:k") // watchdog 下一次续约发现记录消失
		select {
		case <-fnCtx.Done():
			cause = context.Cause(fnCtx)
			return cause
		case <-time.After(2 * time.Second):
			return errors.New("fn ctx not canceled after hold lost")
		}
	}, WithWatchdog(), WithLease(150*time.Millisecond))
	if !errors.Is(cause, ErrLost) {
		t.Fatalf("loss cancel cause want ErrLost, got %v", cause)
	}
	if !errors.Is(err, ErrLost) {
		t.Fatalf("want ErrLost, got %v", err)
	}
}

func TestWithLockReleasesOnPanic(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic should propagate")
			}
		}()
		_ = l.WithLock(ctx, "k", func(context.Context, *Lock) error { panic("boom") })
	}()
	if drv.holdsKey("t:k") {
		t.Fatal("panic must not leak the hold")
	}
}
