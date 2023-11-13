package redis

import (
	"context"

	redigo "github.com/gomodule/redigo/redis"
	"trpc.group/trpc-go/trpc-go/log"
)

// Lock Lock
type Lock struct {
	Key   string
	Value string
	Lease int
}

// NewLock 创建一个新的锁
func NewLock(ctx context.Context, key string, value string, lease int) (lock *Lock) {
	lock = &Lock{
		Key:   key,
		Value: value,
		Lease: lease,
	}
	return
}

// UnLockScript UnLockScript
var UnLockScript = NewScript(1, `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end`)

// RefleshLockScript RefleshLockScript
var RefleshLockScript = NewScript(1, `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
else
	return "FAIL"
end`)

// TryLockOrRefleshLockScript TryLockOrRefleshLockScript
var TryLockOrRefleshLockScript = NewScript(1, `
local data = redis.call("get", KEYS[1])
if data == false then
	return redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
else
	if data == ARGV[1] then
		return redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
	else
		return "FAIL"
	end
end`)

// TryLock 加锁
func (c *Redis) TryLock(ctx context.Context, l *Lock) (bool, error) {
	ok, err := redigo.String(c.core.Do(ctx, "SET", l.Key, l.Value, "NX", "EX", l.Lease))
	if err != nil || ok != retOk {
		if err == redigo.ErrNil {
			log.InfoContextf(ctx, "setnx nil, key:%s, value:%s", l.Key, l.Value)
			return false, nil
		}
		log.ErrorContextf(ctx, "redisCli.Set(\"%s\", \"%s\", nx, ex %d), err:%v or not ok", l.Key, l.Value, l.Lease, err)
		return false, err
	}
	return true, nil
}

// Unlock 解锁
func (c *Redis) Unlock(ctx context.Context, l *Lock) (bool, error) {
	ret, err := redigo.Int64(c.scriptDo(ctx, UnLockScript, l.Key, l.Value))
	if err != nil {
		log.ErrorContextf(ctx, "UnLockScript err:%s, key:%s, value:%s", err.Error(), l.Key, l.Value)
		return false, err
	}

	if ret == 1 {
		return true, nil
	}
	return false, nil
}

// Reflesh 续约
func (c *Redis) Reflesh(ctx context.Context, l *Lock) (bool, error) {
	res, err := redigo.String(c.scriptDo(ctx, RefleshLockScript, l.Key, l.Value, l.Lease))
	if err != nil {
		log.ErrorContextf(ctx, "RefleshLockScript err:%s, key:%s, value:%s, lease:%v", err.Error(), l.Key, l.Value, l.Lease)
		return false, err
	}

	if res == retOk {
		return true, nil
	}
	return false, nil
}

// TryLockOrRefleshLock 锁或者刷新
func (c *Redis) TryLockOrRefleshLock(ctx context.Context, l *Lock) (success bool, err error) {
	res, errTmp := redigo.String(c.scriptDo(ctx, TryLockOrRefleshLockScript, l.Key, l.Value, l.Lease))
	if errTmp != nil {
		log.ErrorContextf(ctx, "TryLockOrRefleshLockScript err:%v, key:%v, value:%v, lease:%v", err, l.Key, l.Value, l.Lease)
		err = errTmp
		return
	}

	if res == retOk {
		success = true
		return
	}
	return
}
