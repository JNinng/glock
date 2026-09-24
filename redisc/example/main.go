// 可运行示例：go run ./example （在 redisc module 目录下）。
// 演示 watchdog：租约 2s、任务 6s，跨 3 个租约周期仍持有；
// 每 2s 用"竞争者"试探一次互斥（应得 ErrHeld）。
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	glock "github.com/jninng/glock"
	"github.com/jninng/glock/redisc"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	addr := getenv("GLOCK_EXAMPLE_REDIS_ADDR", "127.0.0.1:6379")
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	defer rdb.Close()

	ctx := context.Background()
	locker := redisc.New(rdb)

	lk, err := locker.Acquire(ctx, "job:example",
		glock.WithLease(2*time.Second), // 任务远长于租约：交给 watchdog
		glock.WithWatchdog())
	if err != nil {
		fmt.Fprintf(os.Stderr, "acquire: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("acquired job:example  fence=%d  lease=2s  watchdog=on\n", lk.FencingToken())

	start := time.Now()
	for i := 2; i <= 6; i += 2 {
		time.Sleep(2 * time.Second)
		// 竞争者试探：watchdog 续约中的持有应拒绝竞争者。
		if _, err := locker.TryAcquire(ctx, "job:example", glock.WithOwner("challenger")); errors.Is(err, glock.ErrHeld) {
			fmt.Printf("task %ds: hold alive, challenger rejected (ErrHeld)\n", i)
		} else {
			fmt.Fprintf(os.Stderr, "task %ds: hold unexpectedly lost: %v\n", i, err)
			os.Exit(1)
		}
	}
	fmt.Printf("task done in %s\n", time.Since(start).Round(time.Millisecond))

	if err := lk.Release(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "release: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("released")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
