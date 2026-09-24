// Package redisc 提供基于 Redis（go-redis v9）的 glock 后端。
//
// 数据布局（BasicLocker 已加命名空间前缀，此处直接使用完整 Key）：
//
//   - 锁记录：一个 Hash，Key 为 "{<key>}"（hash tag 保证与栅栏计数同 slot，
//     兼容 Redis Cluster）。
//   - 写模式字段：owner、count、mode="0"、fence、expires（绝对毫秒时间戳）。
//   - 读模式字段：mode="1"，每个读者三件套 o:<owner>（计数）、e:<owner>（到期）、
//     f:<owner>（栅栏）。
//   - 栅栏计数：独立 Key "{<key>}:f" 的 INCR，永不过期（保单调）。
//
// 所有操作走 Lua 保证原子。过期比较用客户端绝对时间：竞争者间的时钟偏差
// 进入互斥窗口，要求运行环境 NTP 对时（Redisson 同款假设）。
// 记录 Key 额外设置 PEXPIRE = 2×租约，让无人接管的过期记录由服务端回收。
package redisc

import (
	"context"
	"fmt"
	"time"

	glock "github.com/jninng/glock"
	"github.com/redis/go-redis/v9"
)

// New 返回基于 go-redis 的 Locker，实现 glock 全部能力接口
// （Locker、RWLocker、MultiLocker）。client 由调用方管理生命周期。
func New(client redis.UniversalClient, opts ...glock.Option) *glock.BasicLocker {
	return glock.NewBasicLocker(NewDriver(client), opts...)
}

// Driver 是 Redis 后端的 glock.Driver 实现。
type Driver struct {
	client redis.UniversalClient
}

// NewDriver 构造 Driver。通常直接用 New 获得完整 Locker。
func NewDriver(client redis.UniversalClient) *Driver { return &Driver{client: client} }

// 返回码约定（Lua 数组首元素）：1 成功，0 被他人持有，-1 防误删拒绝，-2 模式冲突。
const (
	retOK       = 1
	retHeld     = 0
	retNotOwner = -1
	retConflict = -2
)

var acquireScript = redis.NewScript(`
local lock, fkey = KEYS[1], KEYS[2]
local owner, now, lease, mode = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4]

local m = redis.call('HGET', lock, 'mode')
if m == mode then
  if mode == '0' then
    if redis.call('HGET', lock, 'owner') == owner and tonumber(redis.call('HGET', lock, 'expires')) > now then
      redis.call('HINCRBY', lock, 'count', 1)
      redis.call('HSET', lock, 'expires', now + lease)
      redis.call('PEXPIRE', lock, lease * 2)
      return {1, tonumber(redis.call('HGET', lock, 'fence'))}
    end
  else
    if redis.call('HEXISTS', lock, 'o:' .. owner) == 1 and tonumber(redis.call('HGET', lock, 'e:' .. owner)) > now then
      redis.call('HINCRBY', lock, 'o:' .. owner, 1)
      redis.call('HSET', lock, 'e:' .. owner, now + lease)
      redis.call('PEXPIRE', lock, lease * 2)
      return {1, tonumber(redis.call('HGET', lock, 'f:' .. owner))}
    end
  end
end

-- 记录存在但（对本次请求）不可重入：清理过期残余，判断是否仍有存活持有。
if m then
  if m == '0' then
    local wlive = tonumber(redis.call('HGET', lock, 'expires')) > now
    if wlive then
      if redis.call('HGET', lock, 'owner') == owner then
        return {-2} -- 同 owner 持写取读：应降级（mode 参数是反方向）
      end
      return {0}
    end
    redis.call('DEL', lock)
  else
    local alive, mine = false, false
    local fields = redis.call('HGETALL', lock)
    local del = {}
    for i = 1, #fields, 2 do
      local f = fields[i]
      if string.sub(f, 1, 2) == 'e:' then
        local o = string.sub(f, 3)
        if tonumber(fields[i + 1]) > now then
          alive = true
          if o == owner then mine = true end
        else
          del[#del + 1] = 'o:' .. o
          del[#del + 1] = 'e:' .. o
          del[#del + 1] = 'f:' .. o
        end
      end
    end
    if #del > 0 then redis.call('HDEL', lock, unpack(del)) end
    if alive then
      if mine then return {-2} end -- 同 owner 持读取写：升级，明确不做
      if mode == '0' then return {0} end
      -- 新读者加入（既有读者存活）：落到下方全新获取分支，INCR 栅栏
    else
      redis.call('DEL', lock)
    end
  end
end

-- 全新获取（写，或读模式下无存活读者）。单字段 HSET：兼容旧版 Redis
--（4.0 起 Lua 才允许多字段 HSET）。
local f = redis.call('INCR', fkey)
if mode == '0' then
  redis.call('HSET', lock, 'owner', owner)
  redis.call('HSET', lock, 'count', 1)
  redis.call('HSET', lock, 'mode', '0')
  redis.call('HSET', lock, 'fence', f)
  redis.call('HSET', lock, 'expires', now + lease)
else
  redis.call('HSET', lock, 'mode', '1')
  redis.call('HSET', lock, 'o:' .. owner, 1)
  redis.call('HSET', lock, 'e:' .. owner, now + lease)
  redis.call('HSET', lock, 'f:' .. owner, f)
end
redis.call('PEXPIRE', lock, lease * 2)
return {1, f}
`)

var refreshScript = redis.NewScript(`
local lock = KEYS[1]
local owner, fence, now, lease, mode = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), ARGV[5]
if redis.call('HGET', lock, 'mode') == mode then
  if mode == '0' then
    if redis.call('HGET', lock, 'owner') == owner and tonumber(redis.call('HGET', lock, 'fence')) == fence
        and tonumber(redis.call('HGET', lock, 'expires')) > now then
      redis.call('HSET', lock, 'expires', now + lease)
      redis.call('PEXPIRE', lock, lease * 2)
      return 1
    end
  else
    if tonumber(redis.call('HGET', lock, 'f:' .. owner)) == fence
        and tonumber(redis.call('HGET', lock, 'e:' .. owner)) > now then
      redis.call('HSET', lock, 'e:' .. owner, now + lease)
      redis.call('PEXPIRE', lock, lease * 2)
      return 1
    end
  end
end
return -1
`)

var releaseScript = redis.NewScript(`
local lock = KEYS[1]
local owner, fence, now, mode = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4]
if redis.call('HGET', lock, 'mode') == mode then
  if mode == '0' then
    if redis.call('HGET', lock, 'owner') == owner and tonumber(redis.call('HGET', lock, 'fence')) == fence
        and tonumber(redis.call('HGET', lock, 'expires')) > now then
      local c = redis.call('HINCRBY', lock, 'count', -1)
      if c <= 0 then redis.call('DEL', lock) end
      return 1
    end
  else
    if tonumber(redis.call('HGET', lock, 'f:' .. owner)) == fence
        and tonumber(redis.call('HGET', lock, 'e:' .. owner)) > now then
      local c = redis.call('HINCRBY', lock, 'o:' .. owner, -1)
      if c <= 0 then
        redis.call('HDEL', lock, 'o:' .. owner, 'e:' .. owner, 'f:' .. owner)
        local fields = redis.call('HKEYS', lock)
        local readers = 0
        for i = 1, #fields do
          if string.sub(fields[i], 1, 2) == 'o:' then readers = readers + 1 end
        end
        if readers == 0 then redis.call('DEL', lock) end
      end
      return 1
    end
  end
end
return -1
`)

var downgradeScript = redis.NewScript(`
local lock = KEYS[1]
local owner, fence, now = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
if redis.call('HGET', lock, 'mode') == '0' and redis.call('HGET', lock, 'owner') == owner
    and tonumber(redis.call('HGET', lock, 'fence')) == fence
    and tonumber(redis.call('HGET', lock, 'expires')) > now then
  if tonumber(redis.call('HGET', lock, 'count')) > 1 then return -2 end
  local exp, f = redis.call('HGET', lock, 'expires'), redis.call('HGET', lock, 'fence')
  redis.call('HDEL', lock, 'owner', 'count', 'fence', 'expires')
  redis.call('HSET', lock, 'mode', '1')
  redis.call('HSET', lock, 'o:' .. owner, 1)
  redis.call('HSET', lock, 'e:' .. owner, exp)
  redis.call('HSET', lock, 'f:' .. owner, f)
  return 1
end
return -1
`)

func lockKey(key string) string  { return "{" + key + "}" }
func fenceKey(key string) string { return "{" + key + "}:f" }

// TryAcquire 实现 glock.Driver。
func (d *Driver) TryAcquire(ctx context.Context, key string, mode glock.Mode, owner string, lease time.Duration) (uint64, error) {
	res, err := acquireScript.Run(ctx, d.client,
		[]string{lockKey(key), fenceKey(key)},
		owner, time.Now().UnixMilli(), lease.Milliseconds(), modeArg(mode),
	).Slice()
	if err != nil {
		return 0, fmt.Errorf("redisc: acquire %q: %w", key, err)
	}
	return acquireResult(key, res)
}

// Refresh 实现 glock.Driver。
func (d *Driver) Refresh(ctx context.Context, key string, mode glock.Mode, owner string, fence uint64, lease time.Duration) error {
	n, err := refreshScript.Run(ctx, d.client,
		[]string{lockKey(key)},
		owner, fence, time.Now().UnixMilli(), lease.Milliseconds(), modeArg(mode),
	).Int()
	if err != nil {
		return fmt.Errorf("redisc: refresh %q: %w", key, err)
	}
	return intResult(key, n)
}

// Release 实现 glock.Driver。
func (d *Driver) Release(ctx context.Context, key string, mode glock.Mode, owner string, fence uint64) error {
	n, err := releaseScript.Run(ctx, d.client,
		[]string{lockKey(key)},
		owner, fence, time.Now().UnixMilli(), modeArg(mode),
	).Int()
	if err != nil {
		return fmt.Errorf("redisc: release %q: %w", key, err)
	}
	return intResult(key, n)
}

// Downgrade 实现 glock.Driver。
func (d *Driver) Downgrade(ctx context.Context, key, owner string, fence uint64) error {
	n, err := downgradeScript.Run(ctx, d.client,
		[]string{lockKey(key)},
		owner, fence, time.Now().UnixMilli(),
	).Int()
	if err != nil {
		return fmt.Errorf("redisc: downgrade %q: %w", key, err)
	}
	return intResult(key, n)
}

// Drop 删除锁记录（不删栅栏计数），仅供测试模拟后端数据消失。
func (d *Driver) Drop(ctx context.Context, key string) error {
	if err := d.client.Del(ctx, lockKey(key)).Err(); err != nil {
		return fmt.Errorf("redisc: drop %q: %w", key, err)
	}
	return nil
}

func modeArg(mode glock.Mode) string {
	if mode == glock.Read {
		return "1"
	}
	return "0"
}

// acquireResult 解析 {code} 或 {code, fence}。
func acquireResult(key string, res []interface{}) (uint64, error) {
	if len(res) == 0 {
		return 0, fmt.Errorf("redisc: acquire %q: empty script reply", key)
	}
	code, ok := res[0].(int64)
	if !ok {
		return 0, fmt.Errorf("redisc: acquire %q: unexpected reply %v", key, res)
	}
	switch code {
	case retOK:
		if len(res) < 2 {
			return 0, fmt.Errorf("redisc: acquire %q: missing fence in reply %v", key, res)
		}
		fence, ok := res[1].(int64)
		if !ok {
			return 0, fmt.Errorf("redisc: acquire %q: bad fence %v", key, res[1])
		}
		return uint64(fence), nil
	case retHeld:
		return 0, fmt.Errorf("redisc: acquire %q: %w", key, glock.ErrHeld)
	case retConflict:
		return 0, fmt.Errorf("redisc: acquire %q: %w", key, glock.ErrModeConflict)
	default:
		return 0, fmt.Errorf("redisc: acquire %q: unexpected code %d", key, code)
	}
}

// intResult 解析单整数返回码（refresh/release/downgrade 共用）。
func intResult(key string, code int) error {
	switch code {
	case retOK:
		return nil
	case retNotOwner:
		return fmt.Errorf("redisc: %w", glock.ErrNotOwner)
	case retConflict:
		return fmt.Errorf("redisc: %w", glock.ErrModeConflict)
	default:
		return fmt.Errorf("redisc: unexpected code %d for %q", code, key)
	}
}
