package backend

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	glock "github.com/jninng/glock"
	"github.com/jninng/glock/locktest"
	"github.com/jninng/glock/postgrec"
	"github.com/jninng/glock/redisc"
	redisclient "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// redis/Postgres 容器与真实客户端跑同一套契约：接口的语义就是这套测试。

func TestRedisConformance(t *testing.T) {
	ctx := context.Background()
	c, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Skipf("docker unavailable, skip redis conformance: %v", err)
		return
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	conn, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	addr := strings.TrimPrefix(conn, "redis://")
	rdb := redisclient.NewClient(&redisclient.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

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

type redisSaboteur struct {
	drv *redisc.Driver
	ns  string
}

func (s *redisSaboteur) Drop(key string) error {
	return s.drv.Drop(context.Background(), s.ns+key)
}

func TestPostgresConformance(t *testing.T) {
	ctx := context.Background()
	c, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("glock"),
		postgres.WithUsername("glock"),
		postgres.WithPassword("glock"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("docker unavailable, skip postgres conformance: %v", err)
		return
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	conn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	locker := postgrec.New(pool)
	if err := locker.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
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

type pgSaboteur struct {
	drv *postgrec.Driver
	ns  string
}

func (s *pgSaboteur) Drop(key string) error {
	return s.drv.Drop(context.Background(), s.ns+key)
}
