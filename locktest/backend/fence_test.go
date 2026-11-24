package backend

// 下游栅栏防护 helper 的行为测试：Redis CAS 走 miniredis（内嵌 Lua，无需
// 真实服务），PG 条件更新走本机常驻 Postgres（不可达即 skip，同契约测试
// 的门控约定）。语义断言两后端对齐：接受 >=、拒绝 <、相等放行（可重入）。

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jninng/glock/postgrec"
	"github.com/jninng/glock/redisc"
	goredis "github.com/redis/go-redis/v9"
)

// 首写_栅栏CAS_接受并可读回。
func TestFencedSetFirstWriteAcceptedAndReadable(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()

	accepted, err := redisc.FencedSet(ctx, rdb, "state:job:1", 5, "v1", 0)
	if err != nil {
		t.Fatalf("FencedSet: %v", err)
	}
	if !accepted {
		t.Fatal("首写被拒，want accepted")
	}
	value, fence, ok, err := redisc.FencedGet(ctx, rdb, "state:job:1")
	if err != nil || !ok {
		t.Fatalf("FencedGet: ok=%v err=%v", ok, err)
	}
	if value != "v1" || fence != 5 {
		t.Fatalf("read back (%q, %d), want (v1, 5)", value, fence)
	}
}

// 更低栅栏_写入_拒绝且原值不变（迟到的旧持有者）。
func TestFencedSetLowerFenceRejected(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()

	if accepted, err := redisc.FencedSet(ctx, rdb, "state:job:2", 6, "v2", 0); err != nil || !accepted {
		t.Fatalf("initial write: accepted=%v err=%v", accepted, err)
	}
	accepted, err := redisc.FencedSet(ctx, rdb, "state:job:2", 5, "zombie", 0)
	if err != nil {
		t.Fatalf("stale write: %v", err)
	}
	if accepted {
		t.Fatal("更低栅栏被接受，want rejected")
	}
	value, fence, ok, _ := redisc.FencedGet(ctx, rdb, "state:job:2")
	if !ok || value != "v2" || fence != 6 {
		t.Fatalf("after rejection read back (%q, %d, %v), want (v2, 6, true)", value, fence, ok)
	}
}

// 相等栅栏_再次写入_放行（可重入沿用同一栅栏，同代持有者的再次写入）。
func TestFencedSetEqualFenceAccepted(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()

	if accepted, err := redisc.FencedSet(ctx, rdb, "state:job:3", 7, "a", 0); err != nil || !accepted {
		t.Fatalf("first write: accepted=%v err=%v", accepted, err)
	}
	accepted, err := redisc.FencedSet(ctx, rdb, "state:job:3", 7, "b", 0)
	if err != nil {
		t.Fatalf("equal write: %v", err)
	}
	if !accepted {
		t.Fatal("相等栅栏被拒，want accepted（可重入语义）")
	}
}

// 零栅栏_写入_报错（真实后端签发的栅栏从 1 起，0 只能来自未初始化变量）。
func TestFencedSetZeroFenceRejected(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	if _, err := redisc.FencedSet(context.Background(), rdb, "state:job:4", 0, "x", 0); err == nil {
		t.Fatal("fence=0 被接受，want error")
	}
}

// 不存在的键_读取_ok=false 且无错误（首写前的常规读取路径）。
func TestFencedGetMissingKeyNotOk(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	_, _, ok, err := redisc.FencedGet(context.Background(), rdb, "state:absent")
	if err != nil {
		t.Fatalf("FencedGet: %v", err)
	}
	if ok {
		t.Fatal("缺失键 ok=true, want false")
	}
}

// 正 TTL_写入_设置存活窗口；零 TTL_再写_不改动既有窗口。
func TestFencedSetTTLSemantics(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	const key = "state:job:5"

	if _, err := redisc.FencedSet(ctx, rdb, key, 1, "v", time.Hour); err != nil {
		t.Fatalf("FencedSet: %v", err)
	}
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > time.Hour {
		t.Fatalf("TTL = %v, want (0, 1h]", ttl)
	}

	if _, err := redisc.FencedSet(ctx, rdb, key, 1, "v2", 0); err != nil {
		t.Fatalf("FencedSet without ttl: %v", err)
	}
	ttl, err = rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 {
		t.Fatalf("零 TTL 写入清掉了既有存活窗口：TTL = %v", ttl)
	}
}

// PG 条件更新：首写插入、相等栅栏放行、更高栅栏接管、更低栅栏拒绝且原值
// 不变。一次性表随测试创建，Cleanup 删除。
func TestFencedUpsertSQLGuardSequence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, localPGDSN())
	if err != nil {
		t.Skipf("local postgres DSN invalid: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("local postgres unavailable: %v", err)
	}

	table := fmt.Sprintf("glock_fence_ut_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE TABLE "+table+
		" (id text PRIMARY KEY, val text, fence bigint NOT NULL DEFAULT 0)"); err != nil {
		t.Skipf("cannot create scratch table (permission?): %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE "+table)
	})

	q, err := execFencedUpsertBuild(table)
	if err != nil {
		t.Fatal(err)
	}

	// 首写插入。
	tag, err := pool.Exec(ctx, q, "row", "v1", uint64(5))
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("first write affected %d rows, want 1", tag.RowsAffected())
	}

	// 相等栅栏放行（可重入语义）。
	tag, err = pool.Exec(ctx, q, "row", "v1b", uint64(5))
	if err != nil {
		t.Fatalf("equal rewrite: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("equal rewrite affected %d rows, want 1（相等必须放行）", tag.RowsAffected())
	}

	// 更高栅栏接管。
	tag, err = pool.Exec(ctx, q, "row", "v2", uint64(6))
	if err != nil {
		t.Fatalf("newer write: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("newer write affected %d rows, want 1", tag.RowsAffected())
	}

	// 更低栅栏拒绝：0 行受影响，原值不变。
	tag, err = pool.Exec(ctx, q, "row", "zombie", uint64(5))
	if err != nil {
		t.Fatalf("stale write: %v", err)
	}
	if tag.RowsAffected() != 0 {
		t.Fatalf("stale write affected %d rows, want 0（迟到写入必须被拒）", tag.RowsAffected())
	}
	var val string
	if err := pool.QueryRow(ctx, "SELECT val FROM "+table+" WHERE id = 'row'").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != "v2" {
		t.Fatalf("row value = %q after rejection, want v2", val)
	}
}

func execFencedUpsertBuild(table string) (string, error) {
	return postgrec.FencedUpsertSQL(table, "id", []string{"val"}, "fence")
}
