package backend

import (
	"context"
	"slices"
	"sort"
	"testing"

	"github.com/alicebob/miniredis/v2"
	glock "github.com/jninng/glock"
	"github.com/jninng/glock/locktest"
	"github.com/jninng/glock/redisc"
	goredis "github.com/redis/go-redis/v9"
)

// miniredis 内嵌 Redis（含 Lua）在无 Docker 环境也能验证 Lua 脚本语义；
// 真实 Redis 的契约测试见同目录 TestRedisConformance（Docker 门控）。
func TestMiniredisConformance(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	drv := redisc.NewDriver(rdb)
	locktest.RunSuite(t, func(t *testing.T, ns string) locktest.Env {
		locker := redisc.New(rdb, glock.WithNamespace(ns))
		return locktest.Env{
			Locker:   locker,
			RW:       locker,
			Multi:    locker,
			Saboteur: &mrSaboteur{drv: drv, ns: ns},
		}
	})
}

type mrSaboteur struct {
	drv *redisc.Driver
	ns  string
}

func (s *mrSaboteur) Drop(key string) error {
	return s.drv.Drop(context.Background(), s.ns+key)
}

// 默认物理键布局：glock:{<key>} 与 glock:{<key>}:f（前缀在 hash tag 外）。
func TestRedisKeyLayout(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	if _, err := redisc.New(rdb).TryAcquire(ctx, "job:1"); err != nil {
		t.Fatal(err)
	}
	assertKeys(t, rdb, "glock:{job:1}*", []string{"glock:{job:1}", "glock:{job:1}:f"})

	// 自定义前缀；空字符串 = 无前缀。
	drv := redisc.NewDriver(rdb, redisc.WithKeyPrefix("app:"))
	if _, err := glock.NewBasicLocker(drv).TryAcquire(ctx, "job:2"); err != nil {
		t.Fatal(err)
	}
	assertKeys(t, rdb, "app:{job:2}*", []string{"app:{job:2}", "app:{job:2}:f"})

	drv0 := redisc.NewDriver(rdb, redisc.WithKeyPrefix(""))
	if _, err := glock.NewBasicLocker(drv0).TryAcquire(ctx, "job:3"); err != nil {
		t.Fatal(err)
	}
	assertKeys(t, rdb, "{job:3}*", []string{"{job:3}", "{job:3}:f"})
}

func assertKeys(t *testing.T, rdb *goredis.Client, pattern string, want []string) {
	t.Helper()
	got, err := rdb.Keys(context.Background(), pattern).Result()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	if !slices.Equal(got, want) {
		t.Fatalf("keys(%q) = %v, want %v", pattern, got, want)
	}
}
