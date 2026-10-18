package glock

import (
	"context"
	"errors"
	"testing"
	"time"
)

var _ ProbingDriver = (*fakeDriver)(nil)

func TestProbeReportsWriteHold(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if got, err := l.Probe(ctx, "k"); err != nil || len(got) != 0 {
		t.Fatalf("fresh key = %v, %v; want empty list", got, err)
	}
	h, err := l.Acquire(ctx, "k", WithOwner("me"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	got, err := l.Probe(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 hold, got %+v", got)
	}
	if info := got[0]; info.Owner != "me" || info.Mode != Write ||
		info.Fence != h.FencingToken() || info.Remaining <= 0 {
		t.Fatalf("bad hold info %+v (want owner me, write, fence %d, positive remaining)",
			info, h.FencingToken())
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := l.Probe(ctx, "k"); err != nil || len(got) != 0 {
		t.Fatalf("after release = %v, %v; want empty list", got, err)
	}
}

func TestProbeListsReadersSorted(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	fences := map[string]uint64{}
	for _, o := range []string{"r2", "r1"} {
		h, err := l.AcquireRead(ctx, "k", WithOwner(o), WithLease(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		fences[o] = h.FencingToken()
	}
	got, err := l.Probe(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Owner != "r1" || got[1].Owner != "r2" {
		t.Fatalf("want [r1 r2], got %+v", got)
	}
	for _, info := range got {
		if info.Mode != Read || info.Fence != fences[info.Owner] || info.Remaining <= 0 {
			t.Fatalf("bad reader info %+v (want read, fence %d, positive remaining)",
				info, fences[info.Owner])
		}
	}
}

func TestProbeExcludesExpiredAndDoesNotExtend(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	h, err := l.Acquire(ctx, "k", WithOwner("me"), WithLease(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Probe(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	got, err := l.Probe(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expired hold must not be listed, got %+v", got)
	}
	// Probe 不得替持有续期：租约确实到期了。
	if _, err := l.TryAcquire(ctx, "k", WithOwner("other")); err != nil {
		t.Fatalf("probe must not extend lease: %v", err)
	}
	if err := h.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("stale release want ErrLost, got %v", err)
	}
}

func TestProbeAppliesNamespace(t *testing.T) {
	// newTestLocker 的命名空间是 "t:"：Probe("k") 必须命中的是 t:k 的记录，
	// 而不是裸键 k（fakeDriver 里两者互不可见）。
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.Acquire(ctx, "k", WithOwner("me"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	drv.mu.Lock()
	_, bare := drv.recs["k"]
	_, namespaced := drv.recs["t:k"]
	drv.mu.Unlock()
	if bare || !namespaced {
		t.Fatalf("driver records: bare=%v namespaced=%v", bare, namespaced)
	}
	got, err := l.Probe(ctx, "k")
	if err != nil || len(got) != 1 || got[0].Owner != "me" {
		t.Fatalf("probe via namespaced locker = %v, %v", got, err)
	}
}

// coreOnlyDriver 只实现核心四方法（不实现 ProbingDriver），验证能力缺失路径。
type coreOnlyDriver struct{ drv Driver }

func (d coreOnlyDriver) TryAcquire(ctx context.Context, key string, mode Mode, owner string, lease time.Duration) (uint64, error) {
	return d.drv.TryAcquire(ctx, key, mode, owner, lease)
}

func (d coreOnlyDriver) Refresh(ctx context.Context, key string, mode Mode, owner string, fence uint64, lease time.Duration) error {
	return d.drv.Refresh(ctx, key, mode, owner, fence, lease)
}

func (d coreOnlyDriver) Release(ctx context.Context, key string, mode Mode, owner string, fence uint64) error {
	return d.drv.Release(ctx, key, mode, owner, fence)
}

func (d coreOnlyDriver) Downgrade(ctx context.Context, key, owner string, fence uint64) error {
	return d.drv.Downgrade(ctx, key, owner, fence)
}

func TestProbeUnsupportedDriver(t *testing.T) {
	l := NewBasicLocker(coreOnlyDriver{drv: newFakeDriver()}, WithNamespace("t:"))
	if _, err := l.Probe(context.Background(), "k"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("want ErrUnsupported, got %v", err)
	}
}
