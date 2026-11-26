// Package locktest 提供 glock 的一致性测试套件：接口的语义就是这套测试，
// 后端通过全部子测试才算实现 glock（v1 契约要求 RWLocker 与 MultiLocker）。
//
// 用法：
//
//	locktest.RunSuite(t, func(t *testing.T, ns string) locktest.Env {
//	    locker := mybackend.New(client, glock.WithNamespace(ns))
//	    return locktest.Env{Locker: locker, RW: locker, Multi: locker,
//	        Saboteur: mySaboteur{ns: ns}, Prober: locker}
//	})
package locktest

import (
	"fmt"
	"testing"
	"time"

	glock "github.com/jninng/glock"
)

// Saboteur 模拟后端数据消失（watchdog 丢失路径的测试钩子）。
// key 为使用者传入的原始 Key（未加命名空间前缀）。
type Saboteur interface {
	Drop(key string) error
}

// Env 是一套被测环境：Locker 加上按能力拆开的接口与破坏钩子。
// Locker/RW/Multi/Saboteur 必填，Prober/Notifier/Sem 可选。
type Env struct {
	Locker   glock.Locker
	RW       glock.RWLocker    // v1 契约必填
	Multi    glock.MultiLocker // v1 契约必填
	Saboteur Saboteur          // v1 契约必填
	// Prober 承载状态内省能力（可选）：Driver 实现 glock.ProbingDriver 时
	// 填入；未填时 Probe 契约测试跳过。
	Prober glock.ProbingLocker
	// Notifier 承载释放唤醒能力（可选）：Driver 实现 glock.NotifyingDriver
	// 时填入；未填时唤醒契约测试跳过。
	Notifier glock.NotifyingDriver
	// Sem 承载信号量能力（可选）：Driver 实现 glock.SemaphoreDriver 时填入；
	// 未填时信号量契约测试跳过。
	Sem glock.SemaphoreLocker
}

// Factory 为每个子测试构造隔离环境（ns 为该子测试唯一的命名空间）。
type Factory func(t *testing.T, ns string) Env

// RunSuite 对 Env 运行全部契约子测试。
func RunSuite(t *testing.T, f Factory) {
	t.Helper()
	run := func(name string, test func(t *testing.T, env Env)) {
		t.Run(name, func(t *testing.T) {
			// ns 每次运行唯一：子测试可能留下未释放的短租约持有，
			// 固定 ns 会在快速重跑时撞上残留记录。
			ns := fmt.Sprintf("%s/%d", t.Name(), time.Now().UnixNano())
			env := f(t, ns)
			if env.Locker == nil || env.RW == nil || env.Multi == nil || env.Saboteur == nil {
				t.Fatal("locktest: Env 的 Locker/RW/Multi/Saboteur 均为必填（v1 契约）")
			}
			test(t, env)
		})
	}
	// 排在最前：本用例必须是不预热 schema 的后端（见 backend/local_test.go）
	// 的首次后端操作，并发首用才会与惰性初始化相撞；排后会被前面的子测试预热。
	run("ColdStart_ConcurrentFirstUse_AllSucceed", testColdStartConcurrent)
	run("Mutex", testMutex)
	run("AcquireBlockingRetries", testAcquireBlockingRetries)
	run("AcquireCtxDoneWrapsErrHeld", testAcquireCtxDone)
	run("AcquireMaxRetriesExhausts", testAcquireMaxRetries)
	run("AcquireBackoff", testAcquireBackoff)
	run("LeaseAutoRelease", testLeaseAutoRelease)
	run("FencingMonotonic", testFencingMonotonic)
	run("SafeReleaseStaleFence", testSafeReleaseStaleFence)
	run("Reentrancy", testReentrancy)
	run("WALRecoveryReentrant", testWALRecovery)
	run("ReadShared", testReadShared)
	run("MixedLeaseReadHold", testMixedLeaseReadHold)
	run("WriteThenReadConflict", testWriteThenReadConflict)
	run("ReadThenWriteConflict", testReadThenWriteConflict)
	run("Downgrade", testDowngrade)
	run("DowngradeRequiresSingleCount", testDowngradeCountGuard)
	run("DowngradeAfterExpiry", testDowngradeAfterExpiry)
	run("MultiAllOrNothing", testMultiAllOrNothing)
	run("MultiSharedWatchdog", testMultiSharedWatchdog)
	run("MultiNoCrossDeadlock", testMultiNoCrossDeadlock)
	run("SemaphoreCapacity", testSemCapacity)
	run("SemaphoreReentrantSlots", testSemReentrantSlots)
	run("SemaphoreCapacityMismatch", testSemCapacityMismatch)
	run("SemaphoreLeaseAutoRelease", testSemLeaseAutoRelease)
	run("SemaphoreFencing", testSemFencing)
	run("SemaphoreBlockingAndCtx", testSemBlockingAndCtx)
	run("SemaphoreWatchdogLost", testSemWatchdogLost)
	run("WatchdogLostSingle", testWatchdogLostSingle)
	run("LeaderResignHandover", testLeaderResignHandover)
	run("LeaderFailoverOnLoss", testLeaderFailoverOnLoss)
	run("LeaderCampaignAbandon", testLeaderCampaignAbandon)
	run("Probe", testProbe)
	run("NotifyRelease", testNotifyRelease)
}
