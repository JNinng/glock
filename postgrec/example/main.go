// 可运行示例：go run ./example （在 postgrec module 目录下）。
// 演示 watchdog：租约 2s、任务 6s，跨 3 个租约周期仍持有；
// 每 2s 用"竞争者"试探一次互斥（应得 ErrHeld）。表自动创建。
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	glock "github.com/jninng/glock"
	"github.com/jninng/glock/postgrec"
)

func main() {
	dsn := getenv("GLOCK_EXAMPLE_PG_DSN", "postgres://postgresql:root@127.0.0.1:5432/postgres?sslmode=disable")
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	locker := postgrec.New(pool) // 表与栅栏序列在首个操作前自动创建

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
