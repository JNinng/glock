package glock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAcquireMaxRetriesExhausts(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.TryAcquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	// 尝试计数以 holder 到手后的基线起算（tryTimes 含全部调用方）。
	base := len(drv.tryTimes("t:k"))
	start := time.Now()
	_, err := l.Acquire(ctx, "k", WithOwner("waiter"),
		WithRetryInterval(20*time.Millisecond), WithMaxRetries(2))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, ErrExhausted) {
		t.Fatalf("want ErrHeld+ErrExhausted, got %v", err)
	}
	// 初次 + 2 次重试 = 3 次尝试，随后立即放弃（不再等待）。
	if n := len(drv.tryTimes("t:k")) - base; n != 3 {
		t.Fatalf("want exactly 3 attempts, got %d", n)
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("exhaustion must not wait past the last failure, took %s", elapsed)
	}
}

func TestAcquireMaxRetriesZeroIsSingleAttempt(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.TryAcquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	base := len(drv.tryTimes("t:k"))
	start := time.Now()
	_, err := l.Acquire(ctx, "k", WithOwner("waiter"), WithMaxRetries(0))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, ErrExhausted) {
		t.Fatalf("want ErrHeld+ErrExhausted, got %v", err)
	}
	if n := len(drv.tryTimes("t:k")) - base; n != 1 {
		t.Fatalf("n=0 means single attempt, got %d", n)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("n=0 must not enter the wait path, took %s", elapsed)
	}
}

func TestAcquireMaxRetriesNegativeIsUnlimited(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.TryAcquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	base := len(drv.tryTimes("t:k"))
	ctx2, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_, err := l.Acquire(ctx2, "k", WithOwner("waiter"),
		WithRetryInterval(20*time.Millisecond), WithMaxRetries(-3))
	// 负数不限：放弃只随 ctx，仍是 ErrHeld+ctx 错误路径，非 ErrExhausted。
	if !errors.Is(err, ErrHeld) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrExhausted) {
		t.Fatalf("negative limit must be unlimited (ctx path), got %v", err)
	}
	if n := len(drv.tryTimes("t:k")) - base; n < 2 {
		t.Fatalf("unlimited must keep retrying, got %d attempts", n)
	}
}

func TestAcquireMaxRetriesSucceedsAfterRelease(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h0, err := l.TryAcquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	base := len(drv.tryTimes("t:k"))
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := h0.Release(ctx); err != nil {
			t.Error(err)
		}
	}()
	h, err := l.Acquire(ctx, "k", WithOwner("waiter"),
		WithRetryInterval(20*time.Millisecond), WithMaxRetries(10))
	if err != nil {
		t.Fatalf("acquire within retry limit: %v", err)
	}
	if n := len(drv.tryTimes("t:k")) - base; n < 2 {
		t.Fatalf("want at least 2 attempts (initial + retry), got %d", n)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.TryAcquire(ctx, "k", WithOwner("holder"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	base := len(drv.tryTimes("t:k"))
	// 20ms→160ms：间隔 20/40/80/160/160，±20% 抖动下递增段不可能被逆转。
	start := time.Now()
	_, err := l.Acquire(ctx, "k", WithOwner("waiter"),
		WithBackoff(20*time.Millisecond, 160*time.Millisecond), WithMaxRetries(5))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, ErrExhausted) {
		t.Fatalf("want ErrHeld+ErrExhausted, got %v", err)
	}
	times := drv.tryTimes("t:k")[base:]
	if len(times) != 6 {
		t.Fatalf("want exactly 6 attempts, got %d", len(times))
	}
	var gaps [5]time.Duration
	for i := range gaps {
		gaps[i] = times[i+1].Sub(times[i])
	}
	// 递增段：相邻间隔标称值差 2 倍，±20% 抖动不可能逆转。
	if !(gaps[1] > gaps[0] && gaps[2] > gaps[1] && gaps[3] > gaps[2]) {
		t.Fatalf("backoff must grow: gaps %v", gaps)
	}
	// 封顶段：两轮都落在 160ms±20%（放宽到 100–300ms 容忍调度毛刺）。
	for i, g := range gaps[3:5] {
		if g < 100*time.Millisecond || g > 300*time.Millisecond {
			t.Fatalf("capped gap %d = %s, want ~160ms±20%%", i+3, g)
		}
	}
	// 总时长下界：等满 5 轮（≥20+40+80+128+128），上界防失控。
	if elapsed := time.Since(start); elapsed < 350*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("total wait %s outside expected backoff envelope", elapsed)
	}
}

func TestBackoffConfigValidation(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	// 封顶低于初始间隔：获取时校验报错，不触后端。
	_, err := l.TryAcquire(ctx, "k", WithBackoff(50*time.Millisecond, 20*time.Millisecond))
	if err == nil || !strings.Contains(err.Error(), "backoff cap") {
		t.Fatalf("want backoff cap validation error, got %v", err)
	}
	if n := len(drv.tryTimes("t:k")); n != 0 {
		t.Fatalf("validation must reject before any backend call, got %d attempts", n)
	}
	// initial 非正沿用间隔校验。
	if _, err := l.TryAcquire(ctx, "k", WithBackoff(0, time.Second)); err == nil ||
		!strings.Contains(err.Error(), "retry interval must be positive") {
		t.Fatalf("want interval validation error, got %v", err)
	}
	// 合法边界（cap == initial）可用；与 WithRetryInterval 后声明者生效。
	h, err := l.TryAcquire(ctx, "k2", WithBackoff(50*time.Millisecond, 50*time.Millisecond))
	if err != nil {
		t.Fatalf("cap == initial must be accepted: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := l.TryAcquire(ctx, "k3", WithBackoff(10*time.Millisecond, time.Second),
		WithRetryInterval(20*time.Millisecond)); err != nil {
		t.Fatalf("later WithRetryInterval must override backoff: %v", err)
	}
}

func TestSemaphoreMaxRetriesExhausts(t *testing.T) {
	l := newTestLocker(newFakeDriver())
	ctx := context.Background()

	if _, err := l.TryAcquireSemaphore(ctx, "pool", 1, WithOwner("holder"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	_, err := l.AcquireSemaphore(ctx, "pool", 1, WithOwner("waiter"),
		WithRetryInterval(20*time.Millisecond), WithMaxRetries(0))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, ErrExhausted) {
		t.Fatalf("want ErrHeld+ErrExhausted, got %v", err)
	}
}

func TestAcquireAllMaxRetriesExhaustsAndRollsBack(t *testing.T) {
	l := newTestLocker(newFakeDriver())
	ctx := context.Background()

	// k2 按字典序排在 a 之后：a 先到手，k2 一次尝试即放弃，整组回滚。
	if _, err := l.Acquire(ctx, "k2", WithOwner("other"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	_, err := l.AcquireAll(ctx, []string{"a", "k2"}, WithOwner("me"), WithMaxRetries(0))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, ErrExhausted) {
		t.Fatalf("want ErrHeld+ErrExhausted, got %v", err)
	}
	// 回滚后 a 不残留 me 的持有。
	if _, err := l.TryAcquire(ctx, "a", WithOwner("third")); err != nil {
		t.Fatalf("key %q not rolled back: %v", "a", err)
	}
}
