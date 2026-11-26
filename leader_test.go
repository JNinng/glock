package glock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 选主编排测试。时序参数：租约 90ms → watchdog 间隔 ~30ms；断言窗口放宽到
// 秒级，只对"必然发生/必然不发生"的契约敏感，不赌竞争胜负。

// drainLostReason 等待失位回调触发并返回丢失原因。
func drainLostReason(t *testing.T, lostC <-chan error, timeout time.Duration) error {
	t.Helper()
	select {
	case reason := <-lostC:
		return reason
	case <-time.After(timeout):
		t.Fatal("OnLost not fired")
		return nil
	}
}

// TestLeadershipResignHandsOver：退位是干净终结——继任者拿到全新栅栏，退位者
// 不触发 OnLost、句柄终结（重复退位返回 ErrLost）。
func TestLeadershipResignHandsOver(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	la, err := l.Campaign(ctx, "ld", WithOwner("a"), WithLease(time.Minute))
	if err != nil {
		t.Fatalf("campaign a: %v", err)
	}
	lostA := make(chan error, 4)
	regainedA := make(chan uint64, 4)
	la.OnLost(func(reason error) { lostA <- reason })
	la.OnRegained(func(fence uint64) { regainedA <- fence })
	fenceA := la.FencingToken()
	if fenceA == 0 || !la.Leading() {
		t.Fatalf("campaign must lead with fence, got fence=%d leading=%v", fenceA, la.Leading())
	}
	if len(regainedA) != 0 {
		t.Fatal("OnRegained must not fire on first Campaign return")
	}

	// b 竞选被 a 挡住，阻塞在 Campaign。
	lbC := make(chan *Leadership, 1)
	go func() {
		lb, err := l.Campaign(context.Background(), "ld", WithOwner("b"), WithLease(time.Minute), WithRetryInterval(15*time.Millisecond))
		if err != nil {
			t.Error(err)
			close(lbC)
			return
		}
		lbC <- lb
	}()
	time.Sleep(150 * time.Millisecond)
	select {
	case lb := <-lbC:
		t.Fatalf("standby must block while leader holds: %v", lb)
	default:
	}

	// a 退位：b 立即上位，拿到全新栅栏（a 释放后记录消失，b 是全新获取）。
	if err := la.Resign(ctx); err != nil {
		t.Fatalf("resign a: %v", err)
	}
	var lb *Leadership
	select {
	case lb = <-lbC:
	case <-time.After(5 * time.Second):
		t.Fatal("standby did not take over after resign")
	}
	if !lb.Leading() || lb.FencingToken() <= fenceA {
		t.Fatalf("new leader fence %d must exceed old %d", lb.FencingToken(), fenceA)
	}

	// a 的终结是干净路径：无 OnLost/OnRegained，句柄终结。
	<-la.Done()
	if la.Leading() || la.FencingToken() != 0 {
		t.Fatalf("after resign leading=%v fence=%d, want false/0", la.Leading(), la.FencingToken())
	}
	time.Sleep(100 * time.Millisecond)
	if len(lostA) != 0 || len(regainedA) != 0 {
		t.Fatal("resign must not fire OnLost/OnRegained")
	}
	if err := la.Resign(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("double resign want ErrLost, got %v", err)
	}

	// b 退位后锁可用。
	if err := lb.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	<-lb.Done()
	if _, err := l.TryAcquire(ctx, "ld", WithOwner("check")); err != nil {
		t.Fatalf("key must be free after all resigned: %v", err)
	}
}

// TestLeadershipMisjudgedLossReentrantRegain：续期传输故障按失位保守报告，但
// 记录仍在 → 同 Owner 再竞选走重入恢复，栅栏不变，领导权无缝续任。
func TestLeadershipMisjudgedLossReentrantRegain(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	la, err := l.Campaign(ctx, "ld", WithOwner("a"), WithLease(90*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	fence0 := la.FencingToken()
	lostC := make(chan error, 4)
	regainedC := make(chan uint64, 4)
	la.OnLost(func(reason error) { lostC <- reason })
	la.OnRegained(func(fence uint64) { regainedC <- fence })

	// 注入一次续期故障：watchdog 下一跳（租约/3 ≈ 30ms，远早于租约过期）失败。
	sentinel := errors.New("fake: transport down")
	drv.failNextRefresh(sentinel)
	reason := drainLostReason(t, lostC, 3*time.Second)
	if !errors.Is(reason, sentinel) {
		t.Fatalf("OnLost reason want injected error, got %v", reason)
	}
	if la.Leading() {
		t.Fatal("must not be leading between loss and regain")
	}

	// 记录仍在（租约未过期）：重入恢复，栅栏沿用，OnLost 先于 OnRegained。
	select {
	case fence := <-regainedC:
		if fence != fence0 {
			t.Fatalf("reentrant regain fence %d must equal previous %d", fence, fence0)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnRegained not fired after misjudged loss")
	}
	if !la.Leading() {
		t.Fatal("must be leading after reentrant regain")
	}

	// 恢复后的任期由重启的 watchdog 保活：越过原始租约仍在任。
	time.Sleep(300 * time.Millisecond)
	if !la.Leading() {
		t.Fatal("watchdog must keep leadership alive after regain")
	}
	if err := la.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	<-la.Done()
}

// TestLeadershipFailoverOnLossDrop：记录被删 → 在任者 OnLost(ErrNotOwner) 并
// 自动再竞选；任何时刻至多一个领导者；换届的栅栏严格递增（删记录后的再获取
// 都是全新获取事件）；退位后另一位当选。不赌 a/b 谁赢得删后空位。
func TestLeadershipFailoverOnLossDrop(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	la, err := l.Campaign(ctx, "ld", WithOwner("a"), WithLease(90*time.Millisecond), WithRetryInterval(300*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	fence0 := la.FencingToken()

	var mu sync.Mutex
	var events []string // a 侧回调顺序：每次 regained 之前必须有本任期的 lost
	lostA := make(chan error, 8)
	regainedA := make(chan uint64, 8)
	la.OnLost(func(reason error) {
		mu.Lock()
		events = append(events, "lost")
		mu.Unlock()
		lostA <- reason
	})
	la.OnRegained(func(fence uint64) {
		mu.Lock()
		events = append(events, "regained")
		mu.Unlock()
		regainedA <- fence
	})

	// b 竞选被 a 挡住；其句柄经信号区转交（b 可能先于 a 重新当选）。
	var lbObserved atomic.Pointer[Leadership]
	lbC := make(chan *Leadership, 1)
	go func() {
		lb, err := l.Campaign(context.Background(), "ld", WithOwner("b"), WithLease(90*time.Millisecond), WithRetryInterval(15*time.Millisecond))
		if err != nil {
			t.Error(err)
			close(lbC)
			return
		}
		lbObserved.Store(lb)
		lbC <- lb
	}()

	// 视图收敛断言：外部删除记录后，失位探测有至多一个 watchdog 间隔的滞后
	//（租约式选主固有的双主窗口，正确性由栅栏防护兜底）；滞后过后本地
	// Leading 视图必须收敛到至多一个为 true。
	converge := func() {
		t.Helper()
		time.Sleep(120 * time.Millisecond) // ≥ 2×watchdog 间隔（90ms 租约 → ~30ms）
		if lb := lbObserved.Load(); la.Leading() && lb != nil && lb.Leading() {
			t.Fatal("leadership views did not converge after detection lag")
		}
	}

	// 在任者失位：Saboteur 删记录 → watchdog 续期失败（ErrNotOwner）。
	drv.drop("t:ld")
	reason := drainLostReason(t, lostA, 3*time.Second)
	if !errors.Is(reason, ErrNotOwner) {
		t.Fatalf("OnLost reason want ErrNotOwner, got %v", reason)
	}

	// 空位必被填补（a 再竞选与 b 竞选谁赢都行），且新栅栏严格递增。
	var leader *Leadership
	select {
	case <-regainedA:
		leader = la
	case lb := <-lbC:
		leader = lb
	case <-time.After(5 * time.Second):
		t.Fatal("no leader emerged after loss")
	}
	fence1 := leader.FencingToken()
	if fence1 <= fence0 {
		t.Fatalf("post-loss fence %d must exceed pre-loss %d (fresh acquisition)", fence1, fence0)
	}
	converge()

	// 领导者退位：另一位当选，栅栏继续递增。
	if err := leader.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	var next *Leadership
	if leader == la {
		select {
		case next = <-lbC:
		case <-time.After(5 * time.Second):
			t.Fatal("standby did not take over after resign")
		}
	} else {
		select {
		case <-regainedA:
			next = la
		case <-time.After(5 * time.Second):
			t.Fatal("deposed leader did not regain after resign")
		}
	}
	fence2 := next.FencingToken()
	if fence2 <= fence1 {
		t.Fatalf("handover fence %d must exceed previous %d", fence2, fence1)
	}
	converge()

	// 回调顺序契约：a 的每次 regained 前必有 lost。
	mu.Lock()
	prevLost := false
	for _, e := range events {
		if e == "lost" {
			prevLost = true
		} else {
			if !prevLost {
				mu.Unlock()
				t.Fatalf("regained without preceding lost: %v", events)
			}
			prevLost = false
		}
	}
	mu.Unlock()

	if err := next.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	<-la.Done()
}

// TestLeadershipResignFromCallback：回调内退位合法——当前回调返回后不再竞选、
// 不再派发，领导权终结且不死锁。
func TestLeadershipResignFromCallback(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	la, err := l.Campaign(ctx, "ld", WithOwner("a"), WithLease(90*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	regainedC := make(chan uint64, 4)
	resigned := make(chan error, 1)
	lostC := make(chan error, 4)
	la.OnLost(func(reason error) {
		lostC <- reason
		resigned <- la.Resign(context.Background())
	})
	la.OnRegained(func(fence uint64) { regainedC <- fence })

	drv.drop("t:ld")
	drainLostReason(t, lostC, 3*time.Second)
	select {
	case err := <-resigned:
		if err != nil {
			t.Fatalf("resign from callback: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("callback did not finish (deadlock?)")
	}
	// 不再竞选：Done 关闭，无 OnRegained，不在任。
	select {
	case <-la.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("leadership did not end after resign from callback")
	}
	time.Sleep(100 * time.Millisecond)
	if len(regainedC) != 0 || la.Leading() {
		t.Fatal("must not recampaign after resign from callback")
	}
	if err := la.Resign(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("double resign want ErrLost, got %v", err)
	}
}

// TestLeadershipCtxEndReleases：Campaign ctx 结束是干净终结——残余持有被独立
// 预算释放（记录消失，他人可立即获取），不触发 OnLost。
func TestLeadershipCtxEndReleases(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx, cancel := context.WithCancel(context.Background())

	la, err := l.Campaign(ctx, "ld", WithOwner("a"), WithLease(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	lostC := make(chan error, 4)
	la.OnLost(func(reason error) { lostC <- reason })

	cancel()
	select {
	case <-la.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Done not closed after ctx cancel")
	}
	time.Sleep(100 * time.Millisecond)
	if len(lostC) != 0 {
		t.Fatal("ctx end must not fire OnLost")
	}
	if _, err := l.TryAcquire(ctx, "ld", WithOwner("check")); err != nil {
		t.Fatalf("hold must be released on ctx end: %v", err)
	}
}

// TestLeadershipCallbackPanicSwallowed：回调 panic 被捕获记录，不中断派发与
// 再竞选。
func TestLeadershipCallbackPanicSwallowed(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	la, err := l.Campaign(ctx, "ld", WithOwner("a"), WithLease(90*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	fence0 := la.FencingToken()
	lostC := make(chan error, 4)
	regainedC := make(chan uint64, 4)
	la.OnLost(func(reason error) {
		lostC <- reason
		panic("callback boom")
	})
	la.OnRegained(func(fence uint64) { regainedC <- fence })

	drv.failNextRefresh(errors.New("fake: transport down"))
	drainLostReason(t, lostC, 3*time.Second)
	// panic 未杀死监督 goroutine：重入恢复照常发生。
	select {
	case <-regainedC:
	case <-time.After(5 * time.Second):
		t.Fatal("supervision died after callback panic")
	}
	if !la.Leading() || la.FencingToken() != fence0 {
		t.Fatalf("regain after panic: leading=%v fence=%d", la.Leading(), la.FencingToken())
	}
	if err := la.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	<-la.Done()
}

// TestLeadershipCampaignAbandonContract：首次当选的放弃契约与 Acquire 同构
// ——重试上限用尽返回 ErrHeld+ErrExhausted，ctx 结束返回 ErrHeld+ctx 错误。
func TestLeadershipCampaignAbandonContract(t *testing.T) {
	drv := newFakeDriver()
	l := newTestLocker(drv)
	ctx := context.Background()

	if _, err := l.TryAcquire(ctx, "ld", WithOwner("holder"), WithLease(time.Minute)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := l.Campaign(ctx, "ld", WithOwner("a"), WithRetryInterval(20*time.Millisecond), WithMaxRetries(2))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, ErrExhausted) {
		t.Fatalf("want ErrHeld+ErrExhausted, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("exhaustion must give up promptly, took %s", elapsed)
	}

	ctx2, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_, err = l.Campaign(ctx2, "ld", WithOwner("a"), WithRetryInterval(20*time.Millisecond))
	if !errors.Is(err, ErrHeld) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrHeld+DeadlineExceeded, got %v", err)
	}
}
