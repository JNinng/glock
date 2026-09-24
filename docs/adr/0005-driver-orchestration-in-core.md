# 所有后端共享 core 的编排（Driver 缝合点）

契约模块不只定义接口：`glock.Driver` 是单次原子操作的后端缝合点（获取一次、
续约一次、释放一次、降级一次），`glock.BasicLocker` 在其上实现全部编排——
阻塞重试与抖动、watchdog、丢失标记与 Lost 通道、多锁的字典序获取与回滚、
降级的持有转移。后端模块（redisc 的 Lua、postgrec 的租约表事务）只对
Driver 负责，编排逻辑只写一遍。

替代方案是每个后端自带完整编排：两份重复的 watchdog/重试/回滚逻辑会立刻
漂移，且第三方后端无法通过同一套 locktest 契约测试。

## Consequences

- Driver 是导出的公开缝合点：实现 Driver + 通过 locktest.RunSuite 即成为一个 glock 后端。
- Driver 的原子性边界就是后端脚本的原子性边界（Lua / 事务内 advisory 串行化）。
- 编排在 core 侧需要日志与指标，因此 core 依赖观测契约 observ（其自身零第三方依赖，见 ADR-0001）。
