// Package postgrec 提供基于 Postgres 租约表（ADR-0002）的 glock 后端。
//
// 数据布局：
//
//	CREATE TABLE glock_locks (
//	  key text, owner text, mode char(1) CHECK (mode IN ('w','r')),
//	  count bigint, fence bigint, expires_at timestamptz,
//	  PRIMARY KEY (key, owner))
//
// 写持有是一行（key, owner, 'w'）；读持有每个读者一行（key, owner, 'r'）。
// 栅栏取全局 SEQUENCE glock_fence，对每个 Key 依然严格递增（ADR-0004）。
//
// 原子性：单行 CAS 用条件 UPDATE；跨行判定（获取、降级、计数归零删除）在
// 事务内先用 pg_advisory_xact_lock(hashtextextended(key,0)) 按 Key 串行化——
// 仅作事务内互斥量，随提交/回滚自动释放，兼容 PgBouncer 事务池化。
// 所有时间比较用数据库 now()（单一时钟源）。
//
// 首个锁操作前自动尝试建表（幂等 DDL，成功一次后跳过；显式调用
// EnsureSchema 可预热）；过期行在获取路径惰性删除，
// 可选启动 RunReaper 做后台清扫（无必须运行的外部组件）。
package postgrec

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	glock "github.com/jninng/glock"
)

// Locker 是 Postgres 后端，实现 glock 全部能力接口。
type Locker struct {
	*glock.BasicLocker
	drv  *Driver
	pool *pgxpool.Pool
}

// New 基于 pgx 连接池构造 Locker。连接池由调用方管理生命周期。
func New(pool *pgxpool.Pool, opts ...glock.Option) *Locker {
	d := NewDriver(pool)
	return &Locker{BasicLocker: glock.NewBasicLocker(d, opts...), drv: d, pool: pool}
}

// Pool 返回底层连接池（reaper/诊断用）。
func (l *Locker) Pool() *pgxpool.Pool { return l.pool }

// EnsureSchema 幂等地创建表、索引与栅栏序列。可选：首个锁操作前会自动尝试；
// 显式调用可预热并提前暴露权限问题。
func (l *Locker) EnsureSchema(ctx context.Context) error {
	return l.drv.EnsureSchema(ctx)
}

// RunReaper 阻塞地周期清扫过期行（惰性删除的补充，低竞争高频 Key 场景可选）。
// 随 ctx 取消退出。
func (l *Locker) RunReaper(ctx context.Context, every time.Duration) error {
	if every <= 0 {
		return fmt.Errorf("postgrec: reaper interval must be positive, got %s", every)
	}
	if err := l.drv.ensureSchema(ctx); err != nil {
		return fmt.Errorf("postgrec: reap ensure schema: %w", err)
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if _, err := l.pool.Exec(ctx, `DELETE FROM glock_locks WHERE expires_at <= now()`); err != nil {
				return fmt.Errorf("postgrec: reap: %w", err)
			}
		}
	}
}
