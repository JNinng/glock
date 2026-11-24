# 下游栅栏防护跟随后端 module，以条件写入 helper 交付

Fencing Token（ADR-0004）只在下游存储配合检查时才有效：下游存住曾写入的
最大栅栏，仅接受不小于它的写入。这个检查必须做成开箱即用的 helper（PG
条件更新、Redis 栅栏 CAS），否则每个使用者手写一遍，最常写错的一处是把
相等栅栏也拒掉——可重入沿用同一栅栏（ADR-0003 只在获取事件递增），同代
持有者的再次写入不是迟到写入，相等必须放行。

关键决策：

- helper 放在 redisc/postgrec 两个后端 module（`redisc.FencedSet/FencedGet`、
  `postgrec.FencedUpsertSQL`），核心 module 只在文档层陈述语义。防护的对象
  是使用者的业务数据，依赖 pgx/go-redis 正是后端 module 既有的依赖；放核心
  违反 ADR-0001 的"核心不携带数据库驱动"。它们不是 Driver 能力：不定义
  接口、不进 locktest 契约套件——Driver 契约回答"后端是否配合 glock"，
  helper 回答"下游如何配合 glock"，测试随各后端 module 走（Redis CAS 对
  miniredis，PG 对本机实例）与 example 黑盒检查。
- 拒绝是正常结果不是错误：CAS 返回 false、条件更新影响 0 行。迟到写入的
  被拒是栅栏机制在工作，调用方据此决定放弃或重试；不占用错误通道。
- fence=0 显式报错（Redis 侧）：真实栅栏从 1 起（INCR/nextval），0 只能
  来自未初始化变量，静默接受会伪造一道从不拒绝的防线。PG 构造器不接触
  值，无法做此检查，由文档承担。
- Redis 布局：守护键是 Hash（f=栅栏、v=业务值），键名原样使用，不加
  glock 前缀与 hash tag——操作全部单键，无跨 slot 问题，守护键与锁记录
  键空间互不相干。栅栏与业务值同键同命：TTL 由调用方选择（>0 每写续期，
  <=0 不改动），守护键过期防护即随状态消失。
- PG 形态：语句构造器而非执行器——pgx v5 没有 pool/conn/tx 共用的执行
  接口，构造器对三者通用，事务边界留给使用者（检查与业务写入同事务是
  使用者的编排责任）；标识符白名单校验（防注入），不加引号——引号会把
  大小写变成敏感语义，白名单内的名称按 PG 常规折叠处理。
- 两个后端语义对齐：接受 >=、拒绝 <、0 行/false 表达拒绝；防护是惰性的
  ——迟到的写入在真实持有者（更新代）首次写入后才必然被拒，这是栅栏
  模式的固有边界，此前的窗口由锁互斥本身覆盖，文档如实陈述而不掩饰。

替代方案：下游守卫表 + 触发器（库级强制所有写入路径过检查）——被否：
把 glock 管理的表耦合进业务 schema，异常式控制流（raise exception）比
0 行/false 难处理，且多行写入场景下触发器仍只能逐行比对，惰性窗口并不
因此收窄。Executor 包装执行（helper 直接收 pool/tx 代执行）——被否：见
上，构造器保持执行面无关。

## Consequences

- 使用者以一行业务写入的代价获得正确栅栏检查：Redis 一调 FencedSet，PG
  一次构造 + 一次 Exec 并检查 RowsAffected。
- PG 业务表须有：keyCol 上的唯一约束（ON CONFLICT 依赖）与 fence 列
  NOT NULL DEFAULT 0（NULL 参与 <= 恒为假，会让该行被永久拒绝）。
- locktest 契约套件不覆盖 helper；新后端若提供同形 helper，语义须与本
  ADR 对齐（>= 放行、拒绝为正常结果、0 栅栏报错）。
