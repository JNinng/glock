package redisc

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// 下游栅栏防护（ADR-0004）：Fencing Token 只有下游存储配合检查才有效——
// 业务键存住曾写入的最大栅栏，仅接受不小于它的写入，迟到的旧持有者写入
// 即被拒绝。本文件把检查与写入做成单个 Lua 脚本（原子 CAS），开箱即用。
//
// 布局：守护键是一个 Hash，字段 f（栅栏）与 v（业务值），由本组 helper
// 独占——键名按使用者传入原样使用（不加 glock 前缀、不加 hash tag：全部
// 操作单键，无跨 slot 问题）。
//
// 语义（与 postgrec 的条件更新一致）：
//
//   - 接受 incoming >= stored，拒绝 incoming < stored。相等必须放行：
//     可重入沿用同一栅栏（ADR-0003 只在获取事件递增），同代持有者的
//     再次写入不是迟到写入。手写检查最常见的错误正是把相等也拒掉。
//   - 拒绝以 false 表达，是正常结果而非错误：调用方自行决定放弃或重试。
//   - fence=0 直接报错：任何 glock 后端签发的栅栏都从 1 起（INCR/nextval），
//     0 只能来自未初始化的变量，静默接受会伪造一道从不拒绝的防线。
//   - 防护是惰性的：迟到的写入在真实持有者（更新代）首次写入后才必然
//     被拒——这是栅栏模式的固有边界，此前的窗口由锁互斥本身覆盖。
//
// TTL：ttl > 0 时每次写入（重）设 PEXPIRE，<= 0 时不改动既有存活窗口。
// 栅栏与业务值同键同命：守护键过期后防护随状态一起消失，重建后的首写
// 从新栅栏起算——TTL 应不短于最坏迟到窗口，否则防护形同虚设。

// fencedSetScript：栅栏 CAS。stored 不存在视为 0（首写必过）；incoming <
// stored 返回 0（拒绝），否则写入 f/v 并按需续期，返回 1。栅栏以十进制
// 字符串存储，与 glock 自身记录的栅栏字段同构。
var fencedSetScript = redis.NewScript(`
local cur = tonumber(redis.call('HGET', KEYS[1], 'f'))
if cur and tonumber(ARGV[1]) < cur then return 0 end
redis.call('HSET', KEYS[1], 'f', ARGV[1])
redis.call('HSET', KEYS[1], 'v', ARGV[2])
local ttl = tonumber(ARGV[3])
if ttl > 0 then redis.call('PEXPIRE', KEYS[1], ttl) end
return 1
`)

// FencedSet 以栅栏 CAS 写入受锁保护的业务状态：fence 不小于守护键已存栅栏
// 才写入（相等放行——可重入沿用同一栅栏的再次写入），小于则拒绝并返回
// false。value 二进制安全，编码（JSON/protobuf 等）由调用方决定。
//
// 客户端就是调用方自建并持有生命周期的那个 redis.UniversalClient（与
// NewDriver 所用可以是同一个）；守护键与 glock 的锁记录键空间互不相干，
// 无需共享前缀约定。
func FencedSet(ctx context.Context, client redis.UniversalClient, key string, fence uint64, value string, ttl time.Duration) (bool, error) {
	if fence == 0 {
		return false, fmt.Errorf("redisc: fenced set %q: fence must be positive, got 0", key)
	}
	n, err := fencedSetScript.Run(ctx, client, []string{key},
		strconv.FormatUint(fence, 10), value, ttl.Milliseconds(),
	).Int()
	if err != nil {
		return false, fmt.Errorf("redisc: fenced set %q: %w", key, err)
	}
	return n == 1, nil
}

// FencedGet 读回守护键的业务值与已存栅栏。键不存在（或字段缺失）返回
// ok=false——首写前的常规读取路径，不是错误。
func FencedGet(ctx context.Context, client redis.UniversalClient, key string) (value string, fence uint64, ok bool, err error) {
	vals, err := client.HMGet(ctx, key, "f", "v").Result()
	if err != nil {
		return "", 0, false, fmt.Errorf("redisc: fenced get %q: %w", key, err)
	}
	if len(vals) != 2 || vals[0] == nil || vals[1] == nil {
		return "", 0, false, nil
	}
	fenceS, _ := vals[0].(string)
	value, _ = vals[1].(string)
	fence, err = strconv.ParseUint(fenceS, 10, 64)
	if err != nil {
		return "", 0, false, fmt.Errorf("redisc: fenced get %q: bad fence %q", key, fenceS)
	}
	return value, fence, true, nil
}
