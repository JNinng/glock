package glock

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// jitter 给时长加 ±jitterRatio 的随机抖动，防同频惊群。
func jitter(d time.Duration) time.Duration {
	f := 1 + (rand.Float64()*2-1)*jitterRatio
	return time.Duration(float64(d) * f)
}

// watchdog 周期性为一组持有续约：单锁一组一个成员，多锁整组一个。
// 任一成员续约失败即整组标记丢失并停止。
type watchdog struct {
	holds    []*hold
	interval time.Duration // 基准间隔 = 租约/3，每轮独立抖动
	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
}

func watchdogInterval(lease time.Duration) time.Duration {
	return lease / watchdogIntervalDiv
}

func startWatchdog(holds []*hold) {
	wd := &watchdog{holds: holds, interval: watchdogInterval(holds[0].lease), stop: make(chan struct{})}
	for _, h := range holds {
		h.wd = wd
	}
	wd.wg.Add(1)
	go wd.run()
}

func (wd *watchdog) run() {
	defer wd.wg.Done()
	for {
		iv := jitter(wd.interval)
		select {
		case <-wd.stop:
			return
		case <-time.After(iv):
		}
		if wd.refreshAll(iv) {
			return
		}
	}
}

// refreshAll 逐个续约；返回 true 表示已终结（被停止或整组丢失）。
func (wd *watchdog) refreshAll(timeout time.Duration) bool {
	for _, h := range wd.holds {
		if h.dead.Load() {
			return true // 持有已被释放/转移，watchdog 随之终结
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := h.drv.Refresh(ctx, h.fullKey, h.mode, h.owner, h.fence, h.lease)
		cancel()
		if err == nil {
			continue
		}
		if errors.Is(err, ErrNotOwner) {
			// 锁已被他人接管或记录消失：整组丢失。
			for _, h := range wd.holds {
				h.markLost(err)
			}
			return true
		}
		// 续期失败原因未知（网络/后端故障）。按整组丢失处理：
		// 宁可让业务提前停下，不可让持有在不知情下过期。
		for _, h := range wd.holds {
			h.markLost(err)
		}
		return true
	}
	return false
}

// stopAndWait 幂等停止并等待 goroutine 退出。
func (wd *watchdog) stopAndWait() {
	wd.stopOnce.Do(func() { close(wd.stop) })
	wd.wg.Wait()
}
