package redis

import (
	"context"

	redigo "github.com/gomodule/redigo/redis"
	"trpc.group/trpc-go/trpc-go/log"
)

// MultiLock redis 互斥批量锁
type MultiLock struct {
	Key        string      // 需要锁定的主key
	MemberList redigo.Args // 需要批量锁定的子key列表
	Lease      int64       // 过期时间, 单位秒
	CurTs      int64       // 加锁当前时间
}

var mlockScript = NewScript(1, `
local key = KEYS[1]
local cur_ts = tonumber(ARGV[1])
local expire_ts = tonumber(ARGV[2])
local zadd_param = {}
for i = 3, #(ARGV) do
	local score = redis.call("zscore", key, ARGV[i])
	if score ~= false then
		if tonumber(score) >= cur_ts then
			return 1
		end
	end
	zadd_param[(i-3)*2+1] = expire_ts
	zadd_param[(i-3)*2+2] = ARGV[i]
end
redis.call("zadd", key, unpack(zadd_param))
return 0
`)

var mUnlockScript = NewScript(1, `
local key = KEYS[1]
local cur_ts = tonumber(ARGV[1])
local expire_ts = tonumber(ARGV[2])
local need_zrem = 1
for i = 3, #(ARGV) do
	local score = redis.call("zscore", key, ARGV[i])
	if score ~= false then
		if tonumber(score) ~= expire_ts then
			need_zrem = 0
			break
		end
	end
end
if need_zrem == 1 then
	redis.call("zrem", key, select(3, unpack(ARGV)))
end
redis.call("zremrangebyscore", key, "-inf", "(" .. cur_ts)
return 0
`)

// MLock 加锁
func (c *Redis) MLock(ctx context.Context, mlock *MultiLock) (bool, error) {
	args := redigo.Args{}.Add(mlock.Key, mlock.CurTs, mlock.CurTs+mlock.Lease).AddFlat(mlock.MemberList)
	ret, err := redigo.Int(c.RunScript(ctx, mlockScript, args...))
	if err != nil {
		log.ErrorContextf(ctx, "MLock|redis RunScript failed, err: %v", err)
		return false, err
	}
	if ret == 1 { // 锁定失败, 已经被锁了
		return false, nil
	}
	return true, nil
}

// MUnlock 解锁
func (c *Redis) MUnlock(ctx context.Context, mlock *MultiLock) (bool, error) {
	args := redigo.Args{}.Add(mlock.Key, mlock.CurTs, mlock.CurTs+mlock.Lease).AddFlat(mlock.MemberList)
	ret, err := redigo.Int(c.RunScript(ctx, mUnlockScript, args...))
	if err != nil {
		log.ErrorContextf(ctx, "MUnlock|redis RunScript failed, err: %v", err)
		return false, err
	}
	if ret == 1 { // 解锁失败, 已经过期了
		return false, nil
	}
	return true, nil
}
