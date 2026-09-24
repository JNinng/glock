// Package glock 定义后端无关的分布式锁契约：能力接口、句柄与后端缝合点 Driver。
//
// 语义要点（术语见仓库 CONTEXT.md，决策见 docs/adr/）：
//
//   - 所有持有必须有界租约，到期由后端自动释放；不存在无限持有。
//   - 防误删：释放只删除自己 Owner Token 持有的锁，且所有变更操作携带栅栏代际，
//     迟到的旧操作必然被拒绝（ADR-0003）。
//   - Fencing Token 是强制契约：每次获取事件单调递增，可交给下游存储拒绝
//     旧持有者的迟到写入（ADR-0004）。
//   - 可重入计数保存在后端，只认存活租约；崩溃恢复以 Acquire + WithOwner
//     （既有 token）重走获取路径。
//   - 阻塞获取不保证公平，也不保证获得顺序。
//   - 读→写升级不提供；持写再取读返回 ErrModeConflict，写持有者应使用 Downgrade。
//
// 后端实现 Driver（单次原子操作），用 NewBasicLocker 获得全部编排
// （阻塞重试、watchdog、句柄生命周期、多锁回滚）。
package glock
