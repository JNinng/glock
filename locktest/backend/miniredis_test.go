package backend

import (
	"context"
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
