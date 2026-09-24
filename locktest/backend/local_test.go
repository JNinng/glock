package backend

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	glock "github.com/jninng/glock"
	"github.com/jninng/glock/locktest"
	"github.com/jninng/glock/postgrec"
	"github.com/jninng/glock/redisc"
	goredis "github.com/redis/go-redis/v9"
)

// 本机常驻服务的契约测试（区别于 testcontainers 拉起的一次性容器）。
// 环境变量可覆盖；服务不可达时 skip。DSN/地址不需要的地方保持零配置。

func localRedisAddr() string {
	if v := os.Getenv("GLOCK_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}

func localPGDSN() string {
	if v := os.Getenv("GLOCK_TEST_PG_DSN"); v != "" {
		return v
	}
	return "postgres://postgresql:root@127.0.0.1:5432/postgres?sslmode=disable"
}

func TestLocalRedisConformance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rdb := goredis.NewClient(&goredis.Options{Addr: localRedisAddr()})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("local redis unavailable at %s: %v", localRedisAddr(), err)
		return
	}

	drv := redisc.NewDriver(rdb)
	locktest.RunSuite(t, func(t *testing.T, ns string) locktest.Env {
		locker := redisc.New(rdb, glock.WithNamespace(ns))
		return locktest.Env{
			Locker:   locker,
			RW:       locker,
			Multi:    locker,
			Saboteur: &redisSaboteur{drv: drv, ns: ns},
		}
	})
}

func TestLocalPostgresConformance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, localPGDSN())
	if err != nil {
		t.Skipf("local postgres DSN invalid: %v", err)
		return
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("local postgres unavailable: %v", err)
		return
	}

	// 故意不调用 EnsureSchema：首个锁操作必须自动建表。
	drv := postgrec.NewDriver(pool)
	locktest.RunSuite(t, func(t *testing.T, ns string) locktest.Env {
		l := postgrec.New(pool, glock.WithNamespace(ns))
		return locktest.Env{
			Locker:   l,
			RW:       l,
			Multi:    l,
			Saboteur: &pgSaboteur{drv: drv, ns: ns},
		}
	})
}
