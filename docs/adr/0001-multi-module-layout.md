# 多 module 仓库布局：core 与各后端独立 go.mod

仓库按 `github.com/jninng/glock`（核心契约与编排，仅依赖零依赖的观测契约 observ）、`glock/redisc`、`glock/postgrec`、
`glock/locktest`（一致性测试套件）拆分独立 Go module，使用方 `go get github.com/jninng/glock/redisc` 时只把该后端的依赖写进自己的
go.mod/go.sum。

core 引入 observ 是明确决策：编排层（watchdog、多锁回滚）需要日志与指标，
observ 自身零第三方依赖，不违反"不携带数据库驱动"的初衷。

选择多 module 而非单 module 多包，是为了严格的依赖隔离：核心模块不携带任何数据库驱动。代价是嵌套 tag（如 `redisc/v1.2.3`
）的版本管理成本，接受。

## Consequences

- 本地开发用 go.work 把各 module 串起来；子 module 的 replace 指向本地路径，发布打嵌套 tag 后移除。
- 每个后端 module 的 tag 必须带目录前缀，发版脚本需覆盖。
- 一致性测试套件（locktest）依赖 testcontainers，独立成 module，不污染 core。
