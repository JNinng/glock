// 黑盒组件自检：只走公开 API，把 glock 当作使用者拿到的库，逐项验证
// README/CONTEXT.md 承诺的能力。对每个可达的后端（本机 Redis、本机
// Postgres）跑同一组检查，按项打印 PASS/FAIL；全部通过退出码 0，
// 任一失败打印明细并退出码 1。
//
// 运行（仓库根目录）：go run ./example
// 后端地址可用环境变量覆盖：GLOCK_EXAMPLE_REDIS_ADDR、GLOCK_EXAMPLE_PG_DSN。
//
// 每次运行使用独立命名空间（WithNamespace），可与线上数据共存，重复运行
// 互不干扰。
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	glock "github.com/jninng/glock"
	"github.com/jninng/glock/postgrec"
	"github.com/jninng/glock/redisc"
	goredis "github.com/redis/go-redis/v9"
)

// backend 是一个可达后端：newLocker 按需构造同后端的 Locker 实例（供
// 构造级选项的检查另建），close 释放底层资源。
type backend struct {
	name      string
	newLocker func(opts ...glock.Option) glock.Locker
	close     func()
}

func main() {
	backends := discover(context.Background())
	if len(backends) == 0 {
		fmt.Fprintln(os.Stderr, "没有可达的后端：需要本机 Redis（127.0.0.1:6379）或本机 Postgres（127.0.0.1:5432）")
		os.Exit(1)
	}

	failed := 0
	for _, b := range backends {
		failed += runSuite(b)
	}
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "共 %d 项检查未通过\n", failed)
		os.Exit(1)
	}
	fmt.Println("全部检查通过")
}

// discover 探测本机后端，不可达的跳过（打印原因），全部可达则覆盖两种后端。
func discover(ctx context.Context) []backend {
	var bs []backend
	if b, ok := discoverRedis(ctx); ok {
		bs = append(bs, b)
	}
	if b, ok := discoverPostgres(ctx); ok {
		bs = append(bs, b)
	}
	return bs
}

func discoverRedis(ctx context.Context) (backend, bool) {
	addr := getenv("GLOCK_EXAMPLE_REDIS_ADDR", "127.0.0.1:6379")
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err := rdb.Ping(pingCtx).Err()
	cancel()
	if err != nil {
		_ = rdb.Close()
		fmt.Printf("跳过 Redis（%s 不可达）\n", addr)
		return backend{}, false
	}
	return backend{
		name:      "Redis " + addr,
		newLocker: func(opts ...glock.Option) glock.Locker { return redisc.New(rdb, opts...) },
		close:     func() { _ = rdb.Close() },
	}, true
}

func discoverPostgres(ctx context.Context) (backend, bool) {
	dsn := getenv("GLOCK_EXAMPLE_PG_DSN", "postgres://postgresql:root@127.0.0.1:5432/postgres?sslmode=disable")
	fail := func(err error) (backend, bool) {
		fmt.Printf("跳过 Postgres（%v）\n", err)
		return backend{}, false
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fail(err)
	}
	// 探测与预热同样限时（对齐 Redis 侧的 2s）：异常网络下不长时间挂住启动。
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err = pool.Ping(pingCtx); err != nil {
		pool.Close()
		return fail(err)
	}
	// 建表预热提前暴露权限问题；后续 Locker 复用同一 schema。
	ddlCtx, ddlCancel := context.WithTimeout(ctx, 5*time.Second)
	defer ddlCancel()
	if err = postgrec.New(pool).EnsureSchema(ddlCtx); err != nil {
		pool.Close()
		return fail(err)
	}
	return backend{
		name:      "Postgres",
		newLocker: func(opts ...glock.Option) glock.Locker { return postgrec.New(pool, opts...) },
		close:     pool.Close,
	}, true
}

// runSuite 对一个后端跑全部演示：每项独立超时（防止能力缺陷挂死整个
// 运行），逐项打印结果，返回失败数。
func runSuite(b backend) int {
	fmt.Printf("== %s ==\n", b.name)
	defer b.close()

	ns := fmt.Sprintf("example/%d/", time.Now().UnixNano())
	lg := b.newLocker(glock.WithNamespace(ns))
	e := env{newLocker: b.newLocker, ns: ns}

	failed := 0
	for _, d := range demos {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		start := time.Now()
		err := d.run(ctx, e, lg)
		cancel()
		if err == nil {
			fmt.Printf("PASS\t%s\t(%s)\n", d.name, time.Since(start).Round(time.Millisecond))
			continue
		}
		fmt.Printf("FAIL\t%s\t(%s)\n\t%v\n", d.name, time.Since(start).Round(time.Millisecond), err)
		failed++
	}
	fmt.Println()
	return failed
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
