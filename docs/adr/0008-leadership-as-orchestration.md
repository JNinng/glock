# 选主即编排（Leadership 是跨任期句柄，不是新 Driver 能力）

选主（一个 Key 上只有一个活跃主，失位自动补位）不复刻一套新原语编排，而是把
"竞选 → 在任保活 → 失位再竞选"建模为既有持有编排之上的监督循环：竞选与再
竞选就是阻塞 Acquire（退避、抖动、释放唤醒、放弃契约全部复用），在任保活就是
watchdog（强制开启），失位感知就是既有 Lost 信号（watchdog 续期失败标记），
换届正确性就是 Fencing Token。核心只新增一个跨任期句柄 Leadership 与它的监督
goroutine：守望当前任期，失位后派发 OnLost 并以同一 Owner Token 自动再竞选，
重新当选派发 OnRegained，直到 Resign 或 Campaign 的 ctx 结束。Driver 零改动，
所有后端经 BasicLocker 免费获得（与 WithLock 同层）。

关键决策：

- 命名 Campaign/Resign 而非路线图草案的 WaitAndAcquire：后者读作一次性阻塞
  获取（正是现有 Acquire），丢失了"跨任期监督"这一独有语义。Try*/Acquire*
  家族保留给单次获取语义。
- OnLost 必须有对称的 OnRegained：Leadership 是跨多个持有期的对象，使用者
  收到失位信号后停止当家，需要等价的"重新当选"信号恢复当家。回调在监督
  goroutine 上同步有序派发（第 n 次 OnLost 先于第 n+1 次 OnRegained），回调
  返回前不开始再竞选，派发时 Leading() 已翻转到位；回调 panic 被捕获记录，
  不中断派发（回调跑在监督 goroutine 上，任其传播会带崩进程）。
- 失位后以同一 Owner Token 再竞选：续期误判（传输故障按失位保守报告、记录
  实际仍在）时同 token 重取是重入恢复、栅栏不变、无缝续任；真实接管时同
  token 重取是全新获取、新栅栏。契约明确"回调不得假设栅栏递增"，下游正确性
  不依赖该差异（栅栏防护接受 ≥，相等放行）。
- watchdog 强制开启：无续期的固定租约 + 自动再竞选 = 每个租约周期掉一次当家
  的抖动循环。再竞选不受 WithMaxRetries 约束——领导权只终结于 Resign 与
  Campaign 的 ctx 结束，其余一切（失位、后端抖动）在监督循环内部消化并经
  回调报告；首次当选保留 Acquire 的完整放弃契约（ErrHeld+ErrExhausted 等）。
- Resign 与 ctx 结束都是干净终结：不触发 OnLost。Resign 用调用方 ctx 释放
  （与 Release 同契约）；ctx 结束路径的残余持有用独立 5 秒预算释放（WithLock
  同款理由：终结时调用方 ctx 可能已取消）。终结后 Done 关闭，提供监督
  goroutine 的 join 点（以同一 token 重竞选同一 Key 前应等它）。
- 诚实的双主声明：租约式选主在时钟漂移、进程暂停与失位探测滞后（至多一个
  watchdog 间隔）下存在双主窗口，Leadership 是便利编排而非更强一致性原语，
  下游操作的正确性仍由 Fencing Token 防护兜底（ADR-0004/0007）。

替代方案：任期句柄风格（OnAcquired(fn(ctx, *Lock))，ctx 在任期结束时取消）
——与 WithLock 对称、完全复用 Lock，但使用者要按任期重建回调注册，且能力
路线图明确点名 OnLost 回调形态；回调对形态配 Leading()/FencingToken() 只读
查询即可覆盖任期状态。etcd 式 watch 选主（等待即订阅）留给 etcd 后端：
NotifyingDriver 已让轮询竞选获得接近唤醒的换届时延，专用 watch API 不必提前
引入。

## Consequences

- LeaderElector 是 BasicLocker 恒定实现的核心编排接口，不走 Driver 能力探测；
  locktest 以 env.Locker 类型断言承载，非 BasicLocker 的第三方包装自动跳过。
- Leadership 不暴露当前任期的 *Lock（避免使用者绕过监督释放/降级任期持有）；
  任期信息经 Leading/FencingToken/OwnerToken 只读查询，FencingToken 不在任
  时返回 0。
- locktest 新增 LeaderResignHandover / LeaderFailoverOnLoss /
  LeaderCampaignAbandon 子测试；失位自动再竞选路径的续期误判变体（重入恢复、
  栅栏不变）需要续期故障注入，由核心模块的 fake Driver 单测覆盖，契约套件
  覆盖删记录变体（Saboteur.Drop）。
