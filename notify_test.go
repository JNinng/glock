package glock

import (
	"context"
	"errors"
	"testing"
	"time"
)

var _ NotifyingDriver = (*fakeDriver)(nil)

func TestAcquireWakesOnReleaseSignal(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h0, err := l.TryAcquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan struct{})
	go func() {
		// 轮询间隔 10s：在时限内醒来只能靠释放信号。
		h, err := l.Acquire(ctx, "k", WithOwner("waiter"), WithRetryInterval(10*time.Second))
		if err != nil {
			t.Error(err)
			return
		}
		close(acquired)
		if err := h.Release(ctx); err != nil {
			t.Error(err)
		}
	}()
	// 等待者已停靠在 WaitForChange 再释放，消除注册时序竞争。
	waitForParked(t, drv)
	if err := h0.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-acquired:
	case <-time.After(3 * time.Second):
		t.Fatal("blocking acquire did not wake on release signal (poll interval 10s)")
	}
}

func TestAcquireNotifyHonorsCtx(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.TryAcquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	// 轮询间隔 10s：ctx 结束必须穿透等待预算立即返回。
	_, err := l.Acquire(ctx2, "k", WithOwner("waiter"), WithRetryInterval(10*time.Second))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrHeld+DeadlineExceeded, got %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("ctx done must return promptly, took %s", time.Since(start))
	}
}

func TestAcquireNotifySpuriousWakesAreHarmless(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h0, err := l.TryAcquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	// 持续制造虚假唤醒：等待者反复被叫醒但拿不到锁，直到 holder 释放。
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			drv.mu.Lock()
			drv.broadcast("t:k")
			drv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()
	defer close(stop)
	acquired := make(chan struct{})
	go func() {
		h, err := l.Acquire(ctx, "k", WithOwner("waiter"), WithRetryInterval(10*time.Second))
		if err != nil {
			t.Error(err)
			return
		}
		close(acquired)
		if err := h.Release(ctx); err != nil {
			t.Error(err)
		}
	}()
	waitForParked(t, drv)
	if err := h0.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-acquired:
	case <-time.After(3 * time.Second):
		t.Fatal("acquire should still succeed under spurious wakes")
	}
}

// waitForParked 阻塞到至少一个等待者停靠进 WaitForChange（fakeDriver 的
// 停靠计数），超时 2s 判失败。测试用它消除"释放先于订阅"的时序竞争。
func waitForParked(t *testing.T, drv *fakeDriver) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for drv.waiting.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no waiter parked in WaitForChange")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
