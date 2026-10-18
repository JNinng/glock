# glock

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

后端无关的分布式锁库，要求 Go 1.26.2 及以上。核心模块定义契约，各后端独立 module 按需引用：

```bash
go get github.com/jninng/glock/redisc     # Redis 后端（go-redis v9）
go get github.com/jninng/glock/postgrec   # Postgres 后端（pgx v5，租约表）
```

语义契约（互斥、防误删、有界租约、可重入、自动续期、阻塞/非阻塞、读写锁、
多锁、写降读、栅栏令牌）见 [CONTEXT.md](CONTEXT.md) 与 [docs/adr/](docs/adr/)；
一致性测试套件见 [locktest](locktest)——接口的语义就是这套测试。

## 快速开始

```go
import (
    goredis "github.com/redis/go-redis/v9"
    "github.com/jninng/glock"
    "github.com/jninng/glock/redisc"
)

locker := redisc.New(goredis.NewClient(&goredis.Options{Addr: ":6379"}))
// Redis 键布局默认 glock:{<key>} 与 glock:{<key>}:f（前缀在 hash tag 外，
// SCAN glock:* 可枚举全部）；自定义用
// redisc.NewDriver(rdb, redisc.WithKeyPrefix("app:")) + glock.NewBasicLocker。

// watchdog 模式：默认租约 30s，后台按租约/3 自动续约，进程死亡后自然到期。
lk, err := locker.Acquire(ctx, "job:42", glock.WithWatchdog())
if err != nil { /* ErrHeld：被他人持有 */ }
defer lk.Release(context.Background())

// 交给下游存储拒绝旧持有者的迟到写入（自动释放场景的正确性补救）。
writeWithFence(lk.FencingToken())
```

Postgres：

```go
pool, _ := pgxpool.New(ctx, "postgres://...")
locker := postgrec.New(pool)
// 表与栅栏序列在首个锁操作前自动创建（幂等 DDL）；
// 可选显式预热：locker.EnsureSchema(ctx)。

// 可选：后台清扫过期行（获取路径本身已惰性清理）。
go locker.RunReaper(ctx, time.Minute)
```

## 能力与用法一览

| 需求     | API                                                                                                                         |
|--------|-----------------------------------------------------------------------------------------------------------------------------|
| 非阻塞获取  | `TryAcquire / TryAcquireRead / TryAcquireAll`（竞争失败返回 `ErrHeld`）                                                             |
| 阻塞获取   | `Acquire / AcquireRead / AcquireAll`（ctx 结束返回包装 `ErrHeld`+ctx 错误）；Driver 实现 `NotifyingDriver` 时释放信号优先唤醒，轮询间隔为等待上限与兜底 |
| 固定租约   | `WithLease(d)`：到期自动释放，不续期                                                                                                   |
| 自动续期   | `WithWatchdog()`：间隔=租约/3 ±20% 抖动；续期失败→句柄 `Lost()` 关闭、操作返回 `ErrLost`                                                         |
| Locker 级默认 | `WithDefaultLease / WithDefaultWatchdog`：构造级默认，单次获取可用 `WithLease / WithWatchdog` 覆盖                                                    |
| 手动续约   | `Lock.Refresh(ctx)`（watchdog 与固定租约下均可用）                                                                                                |
| 可重入    | 同 `WithOwner(token)` 再次获取，计数递增、栅栏不变                                                                                         |
| WAL 恢复 | 先 `glock.NewOwnerToken()` 持久化，恢复后 `Acquire(key, WithOwner(token))`                                                          |
| 读写锁    | `AcquireRead`；持写再取读返回 `ErrModeConflict`（读→写升级不提供）                                                                           |
| 写降读    | `Lock.Downgrade(ctx)`（原子、栅栏不变、需写计数=1、旧句柄终结）                                                                                 |
| 多锁     | `AcquireAll`（仅互斥、字典序防死锁、all-or-nothing 回滚）                                                                                  |
| 执行辅助   | `WithLock(ctx, key, fn)`：阻塞获取→fn→自动释放；watchdog 发现丢失时取消 fn 的 ctx（cause=ErrLost），丢失/释放失败并入返回值 |
| 状态内省   | `Probe(ctx, key)`：只读列出存活持有（Owner、模式、剩余租约、Fence），按 Owner 排序；Driver 实现 `ProbingDriver` 即具备，否则返回 `ErrUnsupported` |
| 观测     | 日志 `*slog.Logger`（`glock.WithLogger`，默认静默）；指标 otel `metric.Meter`（`glock.WithMeter`，默认跟随 `otel.SetMeterProvider`，未配置即 noop） |

## 后端实现 Driver

后端只需实现 `glock.Driver`（单次原子操作），用 `glock.NewBasicLocker(drv)`
获得全部编排（重试、watchdog、多锁回滚、句柄生命周期）。可选实现
`glock.ProbingDriver`（只读内省）与 `glock.NotifyingDriver`（释放唤醒）
分别为 Locker 增加 `Probe` 与信号唤醒能力。参考实现：
[locktest.InmemDriver](locktest/inmem.go)。新后端须通过 `locktest.RunSuite`
全部契约测试（Docker 环境下 Redis/Postgres 后端也会跑同一套，
见 [locktest/backend](locktest/backend)）。

## 仓库布局

独立 Go module（ADR-0001）：`.` 契约与编排（不携带数据库驱动，观测接缝为
slog 与 otel API——`otel` 根包加 `otel/metric`）、`redisc`、`postgrec`、
`locktest`（一致性测试套件，依赖 testcontainers）。
本地开发用 go.work 串联；子 module 的 replace 指向本地路径，发布打嵌套 tag
（如 `redisc/v1.0.0`）后移除。

真实后端的契约测试：`locktest/backend` 优先用本机常驻服务
（`GLOCK_TEST_REDIS_ADDR`，默认 127.0.0.1:6379；
`GLOCK_TEST_PG_DSN`，默认 postgres://postgresql:root@127.0.0.1:5432/postgres?sslmode=disable），
其次 testcontainers 拉起容器；两者都不可用时自动 skip。
无 Docker 环境另有 miniredis（内嵌 Redis 含 Lua）套件兜底。
