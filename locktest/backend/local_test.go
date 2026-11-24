package backend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
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
// 地址可用环境变量覆盖，缺省用本地默认值；服务不可达时 skip。

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
			Prober:   locker,
			Notifier: drv,
			Sem:      locker,
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
			Prober:   l,
			Notifier: drv,
			Sem:      l,
		}
	})
}

// 全新 schema（对象尚不存在）下的并发首用：EnsureSchema 的 DDL 含 CREATE
// OR REPLACE FUNCTION、CREATE SEQUENCE 等系统目录更新，多会话并发执行
// 会撞出 23505/42P07/XX000——幂等不消除目录行竞争，必须串行化（DDL 专用
// advisory 锁）。显式 EnsureSchema 与惰性首调建表两条路径各验一遍：前者
// 面对空 schema（首次部署），后者面对前者已建好的对象（进程重启/新实例），
// 恰好覆盖两种真实冷启动形态。一次性 schema 随测试创建，Cleanup 级联删除。
func TestLocalPostgresEmptySchema_ConcurrentFirstUse_AllSucceed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, localPGDSN())
	if err != nil {
		t.Skipf("local postgres DSN invalid: %v", err)
	}
	defer admin.Close()
	if err := admin.Ping(ctx); err != nil {
		t.Skipf("local postgres unavailable: %v", err)
	}
	schema := fmt.Sprintf("glock_cold_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Skipf("cannot create scratch schema %s (permission?): %v", schema, err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	})

	const n = 16
	cfg, err := pgxpool.ParseConfig(localPGDSN() + "&search_path=" + schema)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = n // 保证 n 路真正并发，不因连接池排队掩盖撞车
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	for _, direct := range []bool{true, false} {
		drv := postgrec.NewDriver(pool)
		start := make(chan struct{})
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				if direct {
					errs[i] = drv.EnsureSchema(ctx)
				} else {
					_, errs[i] = drv.TryAcquire(ctx, "cold", glock.Write,
						fmt.Sprintf("o%d", i), time.Minute)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			// 惰性路径竞争同一把写锁：ErrHeld 是正常竞争结果，其余一律是失败。
			if err != nil && !errors.Is(err, glock.ErrHeld) {
				t.Errorf("direct=%v concurrent first use %d: %v", direct, i, err)
			}
		}
	}
}
