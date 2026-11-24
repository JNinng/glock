package postgrec

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	glock "github.com/jninng/glock"
)

// Driver 是 Postgres 租约表后端的 glock.Driver 实现。
type Driver struct {
	pool        *pgxpool.Pool
	schemaReady atomic.Bool
}

var (
	_ glock.Driver          = (*Driver)(nil)
	_ glock.SemaphoreDriver = (*Driver)(nil)
	_ glock.ProbingDriver   = (*Driver)(nil)
	_ glock.NotifyingDriver = (*Driver)(nil)
)

// NewDriver 构造 Driver。通常直接用 New 获得完整 Locker。
func NewDriver(pool *pgxpool.Pool) *Driver { return &Driver{pool: pool} }

const schemaDDL = `
CREATE SEQUENCE IF NOT EXISTS glock_fence;
CREATE TABLE IF NOT EXISTS glock_locks (
  key        text        NOT NULL,
  owner      text        NOT NULL,
  mode       char(1)     NOT NULL CHECK (mode IN ('w', 'r')),
  count      bigint      NOT NULL,
  fence      bigint      NOT NULL,
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (key, owner)
);
CREATE INDEX IF NOT EXISTS glock_locks_expires_idx ON glock_locks (expires_at);
CREATE TABLE IF NOT EXISTS glock_sems (
  key        text        NOT NULL,
  owner      text        NOT NULL,
  count      bigint      NOT NULL,
  fence      bigint      NOT NULL,
  cap        bigint      NOT NULL,
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (key, owner)
);
CREATE INDEX IF NOT EXISTS glock_sems_expires_idx ON glock_sems (expires_at);
CREATE OR REPLACE FUNCTION glock_notify_change() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND OLD.mode = 'w' AND NEW.mode = 'r') THEN
    PERFORM pg_notify('glock_change', OLD.key);
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION glock_notify_sem_change() RETURNS trigger AS $$
BEGIN
  -- 槽位腾出（计数递减）即放行等待者；续约与重入不通知。
  IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND NEW.count < OLD.count) THEN
    PERFORM pg_notify('glock_change', OLD.key);
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'glock_locks_notify'
      AND tgrelid = 'glock_locks'::regclass AND NOT tgisinternal
  ) THEN
    BEGIN
      -- CREATE TRIGGER 取表级排他锁：只在缺失时执行，避免与并发的锁操作
      -- 事务（advisory + 行锁）互相等待。duplicate_object 只兜底 glock
      -- 之外路径（如手工建触发器）的并发创建。
      CREATE TRIGGER glock_locks_notify
      AFTER DELETE OR UPDATE ON glock_locks
      FOR EACH ROW EXECUTE FUNCTION glock_notify_change();
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'glock_sems_notify'
      AND tgrelid = 'glock_sems'::regclass AND NOT tgisinternal
  ) THEN
    BEGIN
      CREATE TRIGGER glock_sems_notify
      AFTER DELETE OR UPDATE ON glock_sems
      FOR EACH ROW EXECUTE FUNCTION glock_notify_sem_change();
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
  END IF;
END $$;
`

// notifyChannel 是释放唤醒的 LISTEN/NOTIFY 通道，payload 为记录的 key。
const notifyChannel = "glock_change"

// schemaLockKey 是 EnsureSchema 串行化用的固定键：DDL 会更新 pg_proc 等
// 系统目录，多会话并发执行同样的“幂等 DDL”仍会撞出 XX000/23505/42P07
// （幂等只消除对象已存在，不消除目录行竞争）。键不带命名空间前缀：建表
// 是库级一次性事件，全部 Driver 实例须会合到同一把锁上。
const schemaLockKey = "glock:schema"

// EnsureSchema 幂等建表，经事务级 advisory 锁在库级串行执行——并发冷启动
// （多实例同时首调）排队而非撞车。首个锁操作前会自动尝试（成功一次后跳过）；
// 显式调用可预热并提前暴露权限问题。
func (d *Driver) EnsureSchema(ctx context.Context) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgrec: ensure schema: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, schemaLockKey); err != nil {
		return fmt.Errorf("postgrec: ensure schema: %w", err)
	}
	if _, err := tx.Exec(ctx, schemaDDL); err != nil {
		return fmt.Errorf("postgrec: ensure schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgrec: ensure schema: %w", err)
	}
	d.schemaReady.Store(true)
	return nil
}

// ensureSchema 首个操作前自动建表；失败不置位，下个操作重试。
func (d *Driver) ensureSchema(ctx context.Context) error {
	if d.schemaReady.Load() {
		return nil
	}
	return d.EnsureSchema(ctx)
}

// rowRec 是 glock_locks 一行持有记录的内存快照。
type rowRec struct {
	owner string
	mode  string
	count int64
	fence int64
}

// semRow 是 glock_sems 一行槽位记录的内存快照（cap 全行一致，任取一行比对）。
type semRow struct {
	owner string
	count int64
	fence int64
	cap   int64
}

// TryAcquire 实现 glock.Driver。事务内按 Key 串行化并惰性清理过期行后判定：
// 全新获取经全局序列签发新栅栏；同 Owner 同模式且租约存活的重入沿用既有
// 栅栏；竞争失败返回 ErrHeld，同 Owner 反模式（持读取写、持写取读）返回
// ErrModeConflict。
func (d *Driver) TryAcquire(ctx context.Context, key string, mode glock.Mode, owner string, lease time.Duration) (uint64, error) {
	fail := func(op string, err error) (uint64, error) {
		return 0, fmt.Errorf("postgrec: %s %q: %w", op, key, err)
	}
	if err := d.ensureSchema(ctx); err != nil {
		return fail("acquire ensure schema", err)
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fail("acquire begin", err)
	}
	defer tx.Rollback(ctx)

	// 按 Key 串行化跨行判定；事务级 advisory 随提交自动释放。
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return fail("acquire serialize", err)
	}
	// 惰性清理本 Key 的过期行。
	if _, err := tx.Exec(ctx, `DELETE FROM glock_locks WHERE key = $1 AND expires_at <= now()`, key); err != nil {
		return fail("acquire purge", err)
	}
	rows, err := d.rowsFor(ctx, tx, key)
	if err != nil {
		return fail("acquire load", err)
	}
	var writer *rowRec
	readers := map[string]rowRec{}
	for i := range rows {
		if rows[i].mode == "w" {
			writer = &rows[i]
		} else {
			readers[rows[i].owner] = rows[i]
		}
	}

	switch mode {
	case glock.Write:
		if writer != nil {
			if writer.owner == owner {
				if _, err := tx.Exec(ctx,
					`UPDATE glock_locks SET count = count + 1, expires_at = now() + make_interval(secs => $3)
					 WHERE key = $1 AND owner = $2`, key, owner, leaseSecs(lease)); err != nil {
					return fail("acquire reentrant", err)
				}
				if err := tx.Commit(ctx); err != nil {
					return fail("acquire commit", err)
				}
				return uint64(writer.fence), nil // 重入沿用代际
			}
			return 0, fmt.Errorf("postgrec: acquire %q: %w", key, glock.ErrHeld)
		}
		if len(readers) > 0 {
			if _, mine := readers[owner]; mine {
				return 0, fmt.Errorf("postgrec: acquire %q: %w", key, glock.ErrModeConflict) // 持读取写
			}
			return 0, fmt.Errorf("postgrec: acquire %q: %w", key, glock.ErrHeld)
		}
		fence, err := d.insertHold(ctx, tx, key, owner, "w", lease)
		if err != nil {
			return fail("acquire insert", err)
		}
		return fence, nil

	default: // Read
		if writer != nil {
			if writer.owner == owner {
				return 0, fmt.Errorf("postgrec: acquire %q: %w", key, glock.ErrModeConflict) // 持写取读
			}
			return 0, fmt.Errorf("postgrec: acquire %q: %w", key, glock.ErrHeld)
		}
		if r, mine := readers[owner]; mine {
			if _, err := tx.Exec(ctx,
				`UPDATE glock_locks SET count = count + 1, expires_at = now() + make_interval(secs => $3)
				 WHERE key = $1 AND owner = $2 AND mode = 'r'`, key, owner, leaseSecs(lease)); err != nil {
				return fail("acquire reentrant read", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return fail("acquire commit", err)
			}
			return uint64(r.fence), nil
		}
		fence, err := d.insertHold(ctx, tx, key, owner, "r", lease)
		if err != nil {
			return fail("acquire insert read", err)
		}
		return fence, nil
	}
}

// TryAcquireSemaphore 实现 glock.SemaphoreDriver。与 TryAcquire 同构：事务内
// advisory 锁按 Key 串行化并惰性清理过期行后判定——存活槽位已满返回 ErrHeld
// （重入同样受容量约束），容量声明不符返回 ErrCapacityMismatch；重入多占一个
// 槽位、沿用既有栅栏，全新持有者经全局序列签发新栅栏。池清空（全部槽位过期）
// 后容量可重新声明。信号量记录在独立的 glock_sems 表，与锁键空间隔离。
func (d *Driver) TryAcquireSemaphore(ctx context.Context, key string, n int, owner string, lease time.Duration) (uint64, error) {
	fail := func(op string, err error) (uint64, error) {
		return 0, fmt.Errorf("postgrec: %s %q: %w", op, key, err)
	}
	if err := d.ensureSchema(ctx); err != nil {
		return fail("sem acquire ensure schema", err)
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fail("sem acquire begin", err)
	}
	defer tx.Rollback(ctx)

	// 与锁操作共用同一把 Key advisory 锁：跨键空间只是多一层无害串行化。
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return fail("sem acquire serialize", err)
	}
	// 惰性清理本 Key 的过期槽位行。
	if _, err := tx.Exec(ctx, `DELETE FROM glock_sems WHERE key = $1 AND expires_at <= now()`, key); err != nil {
		return fail("sem acquire purge", err)
	}
	rows, err := tx.Query(ctx,
		`SELECT owner, count, fence, cap FROM glock_sems WHERE key = $1`, key)
	if err != nil {
		return fail("sem acquire load", err)
	}
	var recs []semRow
	for rows.Next() {
		var r semRow
		if err := rows.Scan(&r.owner, &r.count, &r.fence, &r.cap); err != nil {
			rows.Close()
			return fail("sem acquire scan", err)
		}
		recs = append(recs, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fail("sem acquire load", err)
	}
	rows.Close()

	if len(recs) > 0 {
		if recs[0].cap != int64(n) {
			return 0, fmt.Errorf("postgrec: sem acquire %q: %w", key, glock.ErrCapacityMismatch)
		}
		var inUse int64
		for _, r := range recs {
			inUse += r.count
		}
		if inUse >= int64(n) {
			return 0, fmt.Errorf("postgrec: sem acquire %q: %w", key, glock.ErrHeld)
		}
		for _, r := range recs {
			if r.owner != owner {
				continue
			}
			// 重入：多占一个槽位，沿用既有栅栏。
			if _, err := tx.Exec(ctx,
				`UPDATE glock_sems SET count = count + 1, expires_at = now() + make_interval(secs => $3)
				 WHERE key = $1 AND owner = $2`, key, owner, leaseSecs(lease)); err != nil {
				return fail("sem acquire reentrant", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return fail("sem acquire commit", err)
			}
			return uint64(r.fence), nil
		}
	}
	// 全新持有者（含池清空后的重建，cap 随新行固定）。
	var fence int64
	err = tx.QueryRow(ctx,
		`INSERT INTO glock_sems (key, owner, count, fence, cap, expires_at)
		 VALUES ($1, $2, 1, nextval('glock_fence'), $3, now() + make_interval(secs => $4))
		 RETURNING fence`,
		key, owner, n, leaseSecs(lease)).Scan(&fence)
	if err != nil {
		return fail("sem acquire insert", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fail("sem acquire commit", err)
	}
	return uint64(fence), nil
}

// Refresh 实现 glock.Driver：单行 CAS，无需 advisory。信号量槽位
// （Mode.Semaphore）路由到 glock_sems。
func (d *Driver) Refresh(ctx context.Context, key string, mode glock.Mode, owner string, fence uint64, lease time.Duration) error {
	if err := d.ensureSchema(ctx); err != nil {
		return fmt.Errorf("postgrec: refresh %q: %w", key, err)
	}
	if mode == glock.Semaphore {
		tag, err := d.pool.Exec(ctx,
			`UPDATE glock_sems SET expires_at = now() + make_interval(secs => $4)
			 WHERE key = $1 AND owner = $2 AND fence = $3 AND expires_at > now()`,
			key, owner, int64(fence), leaseSecs(lease))
		if err != nil {
			return fmt.Errorf("postgrec: refresh %q: %w", key, err)
		}
		return rowsAffected(key, "refresh", tag)
	}
	tag, err := d.pool.Exec(ctx,
		`UPDATE glock_locks SET expires_at = now() + make_interval(secs => $5)
		 WHERE key = $1 AND owner = $2 AND mode = $3 AND fence = $4 AND expires_at > now()`,
		key, owner, modeChar(mode), int64(fence), leaseSecs(lease))
	if err != nil {
		return fmt.Errorf("postgrec: refresh %q: %w", key, err)
	}
	return rowsAffected(key, "refresh", tag)
}

// Release 实现 glock.Driver：计数归零的删除与并发重入竞争，需按 Key 串行化。
// 信号量槽位路由到 glock_sems（释放即腾出槽位，触发器通知等待者）。
func (d *Driver) Release(ctx context.Context, key string, mode glock.Mode, owner string, fence uint64) error {
	fail := func(op string, err error) error {
		return fmt.Errorf("postgrec: %s %q: %w", op, key, err)
	}
	if err := d.ensureSchema(ctx); err != nil {
		return fail("release ensure schema", err)
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fail("release begin", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return fail("release serialize", err)
	}
	var count int64
	if mode == glock.Semaphore {
		tag, err := tx.Exec(ctx,
			`UPDATE glock_sems SET count = count - 1
			 WHERE key = $1 AND owner = $2 AND fence = $3 AND expires_at > now()`,
			key, owner, int64(fence))
		if err != nil {
			return fail("release", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("postgrec: release %q: %w", key, glock.ErrNotOwner)
		}
		if err := tx.QueryRow(ctx,
			`SELECT count FROM glock_sems WHERE key = $1 AND owner = $2`, key, owner).Scan(&count); err != nil {
			return fail("release inspect", err)
		}
		if count <= 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM glock_sems WHERE key = $1 AND owner = $2`, key, owner); err != nil {
				return fail("release delete", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return fail("release commit", err)
		}
		return nil
	}
	tag, err := tx.Exec(ctx,
		`UPDATE glock_locks SET count = count - 1
		 WHERE key = $1 AND owner = $2 AND mode = $3 AND fence = $4 AND expires_at > now()`,
		key, owner, modeChar(mode), int64(fence))
	if err != nil {
		return fail("release", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgrec: release %q: %w", key, glock.ErrNotOwner)
	}
	if err := tx.QueryRow(ctx,
		`SELECT count FROM glock_locks WHERE key = $1 AND owner = $2`, key, owner).Scan(&count); err != nil {
		return fail("release inspect", err)
	}
	if count <= 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM glock_locks WHERE key = $1 AND owner = $2`, key, owner); err != nil {
			return fail("release delete", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fail("release commit", err)
	}
	return nil
}

// Downgrade 实现 glock.Driver：写行翻转为读行（PK 不变，单行原子）。
func (d *Driver) Downgrade(ctx context.Context, key, owner string, fence uint64) error {
	fail := func(op string, err error) error {
		return fmt.Errorf("postgrec: %s %q: %w", op, key, err)
	}
	if err := d.ensureSchema(ctx); err != nil {
		return fail("downgrade ensure schema", err)
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fail("downgrade begin", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return fail("downgrade serialize", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE glock_locks SET mode = 'r', count = 1
		 WHERE key = $1 AND owner = $2 AND mode = 'w' AND fence = $3 AND expires_at > now()
		   AND count = 1`,
		key, owner, int64(fence))
	if err != nil {
		return fail("downgrade", err)
	}
	if tag.RowsAffected() == 0 {
		// 区分拒绝原因：存活记录上计数>1 是模式冲突；身份/代际不符或
		// 已过期都是防误删拒绝。过期行不得报模式冲突——持有已丢失。
		var c int64
		err := tx.QueryRow(ctx,
			`SELECT count FROM glock_locks WHERE key = $1 AND owner = $2 AND mode = 'w' AND fence = $3
			   AND expires_at > now()`,
			key, owner, int64(fence)).Scan(&c)
		if err == nil && c > 1 {
			return fmt.Errorf("postgrec: downgrade %q: %w", key, glock.ErrModeConflict)
		}
		return fmt.Errorf("postgrec: downgrade %q: %w", key, glock.ErrNotOwner)
	}
	if err := tx.Commit(ctx); err != nil {
		return fail("downgrade commit", err)
	}
	return nil
}

// Probe 实现 glock.ProbingDriver：只读 SELECT，不改动任何行，剩余租约用
// 数据库 now() 计算（与存活判定同源）。过期残余不算持有，清理仍属获取路径。
func (d *Driver) Probe(ctx context.Context, key string) ([]glock.HoldInfo, error) {
	if err := d.ensureSchema(ctx); err != nil {
		return nil, fmt.Errorf("postgrec: probe %q: %w", key, err)
	}
	rows, err := d.pool.Query(ctx,
		`SELECT owner, mode, fence, (EXTRACT(EPOCH FROM expires_at - now()) * 1000)::bigint
		 FROM glock_locks WHERE key = $1 AND expires_at > now()
		 ORDER BY owner`, key)
	if err != nil {
		return nil, fmt.Errorf("postgrec: probe %q: %w", key, err)
	}
	defer rows.Close()
	var out []glock.HoldInfo
	for rows.Next() {
		var owner, mode string
		var fence, ttlMs int64
		if err := rows.Scan(&owner, &mode, &fence, &ttlMs); err != nil {
			return nil, fmt.Errorf("postgrec: probe %q: %w", key, err)
		}
		out = append(out, glock.HoldInfo{
			Owner:     owner,
			Mode:      modeFromChar(mode),
			Fence:     uint64(fence),
			Remaining: time.Duration(ttlMs) * time.Millisecond,
		})
	}
	return out, rows.Err()
}

// WaitForChange 实现 glock.NotifyingDriver：独立连接 LISTEN，收到本 Key
// 的通知即提前返回。通知由 glock_locks 上的触发器在行删除（释放、过期
// 清理）与写降读时发出，随事务提交送达——信号与状态可见性有序。尽力而为：
// LISTEN 生效前发生的通知会漏收，等待落回 maxWait 由调用方轮询兜底；
// 拿不到连接或 LISTEN 失败（如 PgBouncer 事务池化）按剩余预算等满。
func (d *Driver) WaitForChange(ctx context.Context, key string, maxWait time.Duration) error {
	deadline := time.Now().Add(maxWait)
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return waitOut(ctx, deadline)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `LISTEN `+notifyChannel); err != nil {
		return waitOut(ctx, deadline)
	}
	for {
		wctx, cancel := context.WithDeadline(ctx, deadline)
		n, err := conn.Conn().WaitForNotification(wctx)
		cancel()
		if err == nil {
			if n.Channel == notifyChannel && n.Payload == key {
				return nil
			}
			continue // 其他 Key 的通知：继续等自己的
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return waitOut(ctx, deadline) // maxWait 用尽或连接故障：等满剩余预算
	}
}

// waitOut 按剩余预算阻塞至 deadline：通知机制故障时的兜底等待，保住调用方
// 的轮询节律。
func waitOut(ctx context.Context, deadline time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Until(deadline)):
		return nil
	}
}

// Drop 删除 Key 的全部行（锁与信号量两张表），仅供测试模拟后端数据消失。
func (d *Driver) Drop(ctx context.Context, key string) error {
	if _, err := d.pool.Exec(ctx, `DELETE FROM glock_locks WHERE key = $1`, key); err != nil {
		return fmt.Errorf("postgrec: drop %q: %w", key, err)
	}
	if _, err := d.pool.Exec(ctx, `DELETE FROM glock_sems WHERE key = $1`, key); err != nil {
		return fmt.Errorf("postgrec: drop %q: %w", key, err)
	}
	return nil
}

func (d *Driver) rowsFor(ctx context.Context, tx pgx.Tx, key string) ([]rowRec, error) {
	rows, err := tx.Query(ctx,
		`SELECT owner, mode, count, fence FROM glock_locks WHERE key = $1`, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []rowRec
	for rows.Next() {
		var r rowRec
		if err := rows.Scan(&r.owner, &r.mode, &r.count, &r.fence); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// insertHold 插入新持有行（栅栏取自全局序列）并提交事务，返回签发的栅栏。
func (d *Driver) insertHold(ctx context.Context, tx pgx.Tx, key, owner, mode string, lease time.Duration) (uint64, error) {
	var fence int64
	err := tx.QueryRow(ctx,
		`INSERT INTO glock_locks (key, owner, mode, count, fence, expires_at)
		 VALUES ($1, $2, $3, 1, nextval('glock_fence'), now() + make_interval(secs => $4))
		 RETURNING fence`,
		key, owner, mode, leaseSecs(lease)).Scan(&fence)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return uint64(fence), nil
}

// rowsAffected 将 0 行更新映射为 ErrNotOwner（防误删拒绝）。
func rowsAffected(key, op string, tag pgconn.CommandTag) error {
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgrec: %s %q: %w", op, key, glock.ErrNotOwner)
	}
	return nil
}

func modeChar(mode glock.Mode) string {
	if mode == glock.Read {
		return "r"
	}
	return "w"
}

func modeFromChar(s string) glock.Mode {
	if s == "r" {
		return glock.Read
	}
	return glock.Write
}

func leaseSecs(d time.Duration) float64 { return float64(d.Microseconds()) / 1e6 }
