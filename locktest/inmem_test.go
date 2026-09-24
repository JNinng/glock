package locktest_test

import (
	"context"
	"testing"

	glock "github.com/jninng/glock"
	"github.com/jninng/glock/locktest"
)

// 内存参考实现必须通过全部契约测试：无 Docker 环境的语义底线。
func TestInmemConformance(t *testing.T) {
	locktest.RunSuite(t, func(t *testing.T, ns string) locktest.Env {
		drv := locktest.NewInmemDriver()
		locker := glock.NewBasicLocker(drv, glock.WithNamespace(ns))
		return locktest.Env{
			Locker:   locker,
			RW:       locker,
			Multi:    locker,
			Saboteur: &inmemSaboteur{drv: drv, ns: ns},
		}
	})
}

type inmemSaboteur struct {
	drv *locktest.InmemDriver
	ns  string
}

func (s *inmemSaboteur) Drop(key string) error {
	return s.drv.Drop(context.Background(), s.ns+key)
}
