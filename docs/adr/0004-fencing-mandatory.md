# 栅栏令牌是强制契约，不是可选能力

v1 契约要求所有后端必须签发 Fencing Token：句柄上 `FencingToken() uint64` 无错误返回。

最初设计为可选能力接口（后端按支持度实现）。放弃的原因：ADR-0003 的防迟到释放机制在库内部依赖代际计数，而它对使用者暴露的形态正是
Fencing Token——同一个计数器。两个 v1 后端（Redis INCR、Postgres sequence）实现都平凡。若留为可选，句柄方法必须返回
`(uint64, error)` 并挖一个运行时 ErrUnsupported 的洞，与"能力缺失在类型系统暴露"的原则矛盾。

## Consequences

- 不满足栅栏签发的后端不满足 glock v1 契约（如未来的纯 advisory lock 后端）。
- 读写降级与可重入的计数递增不触发代际递增；只有获取事件递增。
