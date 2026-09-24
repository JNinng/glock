# postgrec 采用租约表而非 advisory lock

Postgres 后端用一张租约表（key、owner token、mode、count、fencing、expires_at）加 CAS 更新实现锁，而不是 pg_advisory_lock。

advisory lock 只能满足需求的一半：会话级自动释放和阻塞获取天然支持，但没有租约、无法自动续期、没有 Owner Token
概念（防误删无从谈起）、读写降级非原子，且与 PgBouncer transaction 池化不兼容。租约表虽要处理过期行清理且吞吐低于
advisory，但能完整实现 glock 的全部语义契约。

## Considered Options

- **pg_advisory_lock**: 语义覆盖不足，拒绝。若未来需要轻量场景可另立后端 module，不改本决策。
- **事务级 advisory（pg_advisory_xact_lock）作为锁本体**: 锁生命周期被事务绑死，与长任务持有模型冲突，拒绝（事务内作串行化互斥量使用另见下）。

## Consequences

- 获取、降级、计数归零删除等跨行判定在事务内用 pg_advisory_xact_lock(hashtextextended(key, 0)) 按 Key
  串行化——仅作事务内互斥量，随提交自动释放，兼容 PgBouncer 事务池化；锁的语义本体仍是租约表，不依赖会话级 advisory。
- 过期行在获取路径惰性删除，可选 RunReaper 后台清扫，无必须运行的外部组件。
- 表、索引与栅栏序列在首个锁操作前自动创建（幂等 DDL，成功一次后跳过）；EnsureSchema 仅供显式预热。
