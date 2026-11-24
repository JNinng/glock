# 信号量槽位即持有（Mode.Semaphore 复用持有编排）

信号量（N 上限资源池）不复刻一套新原语编排，而是把"一个槽位"建模为一次持有：
有界租约、可重入计数、防误删、Fencing Token 全部沿用锁的既有契约——释放即腾出
槽位，过期即自动腾出（获取路径惰性清理），watchdog 保活槽位。Driver 侧只新增
一个原子操作 `SemaphoreDriver.TryAcquireSemaphore(key, n, owner, lease)`，
Refresh/Release 以新增的 `Mode.Semaphore` 路由到信号量记录；BasicLocker 的
获取循环、唤醒、watchdog、句柄生命周期零改动复用。

关键决策：

- 容量在池首个获取时固定。尚有存活槽位时容量声明不符返回专用错误
  ErrCapacityMismatch（不可通过等待自愈，阻塞获取也立即失败）；全部槽位过期
  清空后允许重新声明——记录消失即容量消失，与锁的过期接管同构。槽满复用
  ErrHeld（可等待自愈），阻塞获取与唤醒编排因此零新增。
- 重入多占槽位而非沿用：同 Owner 再取递增计数（多占一个槽位）、沿用既有栅栏；
  只有全新持有者是获取事件、递增栅栏。同一 Owner 的全部槽位共享一份租约（与
  读模式的共享记录一致）。Owner 的全部槽位释放（计数归零）后持有条目消失，
  再取是全新获取事件。
- 信号量记录与锁键空间隔离（redisc 独立 Hash `<prefix>{<key>}:s`、postgrec
  独立表 glock_sems）：同名的锁与信号量互不相干，Probe 只回答锁的问题。
  栅栏计数与锁共用同一计数器（同一 Key 名一个），跨资源仍单调，不影响
  迟到写入拒绝的正确性。
- 槽位腾出属于放行性变更：NotifyingDriver 的释放唤醒对信号量同样生效
  （postgrec 触发器在计数递减/删行时通知，redisc 在释放脚本后发布）。

替代方案：给 SemaphoreDriver 配独立的 RefreshSemaphore/ReleaseSemaphore
方法——hold 与 watchdog 需要按模式分派两套调用，编排复用被打折；
`Mode.Semaphore` 路由让持有生命周期代码零分支。共享 Mode 空间的代价是
Driver 实现者需理解"Refresh/Release 可能以 Semaphore 模式到达"，该义务写进
SemaphoreDriver 接口契约；不实现此能力的 Driver 永远收不到该模式。

## Consequences

- 实现 SemaphoreDriver 即必须让 Refresh/Release 按 Mode 路由。
- Probe 不覆盖信号量；`Mode.Semaphore` 目前只出现在 Refresh/Release 的路由
  参数与句柄日志中，HoldInfo 不会出现信号量持有。
- locktest 以 `Env.Sem` 承载该能力（可选，未填跳过信号量子测试）。
