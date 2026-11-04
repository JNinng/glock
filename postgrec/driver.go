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
CREATE OR REPLACE FUNCTION glock_notify_change() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND OLD.mode = 'w' AND NEW.mode = 'r') THEN
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
      -- 事务（advisory + 行锁）互相等待。并发冷启动撞车按已创建处理。
      CREATE TRIGGER glock_locks_notify
      AFTER DELETE OR UPDATE ON glock_locks
      FOR EACH ROW EXECUTE FUNCTION glock_notify_change();
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
  END IF;
END $$;
`

// notifyChannel 是释放唤醒的 LISTEN/NOTIFY 通道，payload 为记录的 key。
const notifyChannel = "glock_change"

// EnsureSchema 幂等建表。首个锁操作前会自动尝试（成功一次后跳过）；
// 显式调用可预热并提前暴露权限问题。
func (d *Driver) EnsureSchema(ctx context.Context) error {
	if _, err := d.pool.Exec(ctx, schemaDDL); err != nil {
		return fmt.Errorf("postgrec: ensure schema: %w", err)
	}
	d.schemaReady.Store(true)
	return nil
}

// ensureSchema 首个操作前自动建表；失败不置位，下个操作重试。
// DDL 幂等（IF NOT EXISTS），并发首调最坏情况是一次可重试错误。
func (d *Driver) ensureSchema(ctx context.Context) error {
	if d.schemaReady.Load() {
		return nil
	}
	return d.EnsureSchema(ctx)
}

type rowRec struct {
	owner string
	mode  string
	count int64
	fence int64
}

// TryAcquire 实现 glock.Driver。
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

// Refresh 实现 glock.Driver：单行 CAS，无需 advisory。
func (d *Driver) Refresh(ctx context.Context, key string, mode glock.Mode, owner string, fence uint64, lease time.Duration) error {
	if err := d.ensureSchema(ctx); err != nil {
		return fmt.Errorf("postgrec: refresh %q: %w", key, err)
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

// Drop 删除 Key 的全部行，仅供测试模拟后端数据消失。
func (d *Driver) Drop(ctx context.Context, key string) error {
	if _, err := d.pool.Exec(ctx, `DELETE FROM glock_locks WHERE key = $1`, key); err != nil {
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
