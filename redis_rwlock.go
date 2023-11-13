package redis

import (
	"context"

	redigo "github.com/gomodule/redigo/redis"
	"trpc.group/trpc-go/trpc-go/log"
)

// RWLock redis读写锁 读写互斥, 读读、写写不互斥
type RWLock struct {
	Key   string
	Value string
	Lease int
}

// NewRWLock 创建一个新的读写锁
func NewRWLock(ctx context.Context, key string, value string, lease int) (lock *RWLock) {
	lock = &RWLock{
		Key:   key,
		Value: value,
		Lease: lease,
	}
	return
}

// Lock 加写锁
func (l *RWLock) Lock(ctx context.Context, red *Redis) (bool, error) {
	return l.lock(ctx, red, "w")
}

// RLock 加读锁
func (l *RWLock) RLock(ctx context.Context, red *Redis) (bool, error) {
	return l.lock(ctx, red, "r")
}

func (l *RWLock) lock(ctx context.Context, red *Redis, rw string) (bool, error) {

	keyAndArgs := make([]interface{}, 0)
	//主key, 读写类型, 超时时间, 副key
	keyAndArgs = append(keyAndArgs, l.Key, rw, l.Lease, l.Value)
	reply, err := redigo.String(red.RunScript(ctx, RWLockScript, keyAndArgs...))
	if err != nil {
		log.ErrorContextf(ctx, "RunScript err:%v, rwlock:%+v", err, l)
		return false, err
	}

	if reply != retOk {
		return false, nil
	}

	return true, nil
}

// Unlock 解锁
func (l *RWLock) Unlock(ctx context.Context, red *Redis) (bool, error) {

	keyAndArgs := make([]interface{}, 0)
	//主key, 副key
	keyAndArgs = append(keyAndArgs, l.Key, l.Value)
	reply, err := redigo.String(red.RunScript(ctx, RWUnLockScript, keyAndArgs...))
	if err != nil {
		log.ErrorContextf(ctx, "RunScript err:%v, rwlock:%+v", err, l)
		return false, err
	}

	if reply != retOk {
		return false, nil
	}

	return true, nil
}

// RWLockScript 读写锁加锁
var RWLockScript = NewScript(1, `
local rwkey = "_rw"
local key = KEYS[1]
local rwval = ARGV[1]
local seconds = tonumber(ARGV[2])
local subkey = ARGV[3]
local rwnow = redis.call("HGET", key, rwkey)
if rwnow ~= false then
	if rwnow ~= rwval then
		return "FAIL"
	end
end
redis.call("HMSET", key, rwkey, rwval, subkey, "")
redis.call("EXPIRE", key, seconds)
return "OK"
`)

// RWUnLockScript 读写锁解锁
var RWUnLockScript = NewScript(1, `
local key = KEYS[1]
local subkey = ARGV[1]
redis.call("HDEL", key, subkey)
local hlen = redis.call("HLEN", key)
if hlen <= 1 then
	redis.call("DEL", key)
end
return "OK"
`)
