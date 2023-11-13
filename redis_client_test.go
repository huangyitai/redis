package redis

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	redigo "github.com/gomodule/redigo/redis"
)

func newRedisClient(ctx context.Context, opts ...Option) (redisClient *Redis) {
	optsReal := []Option{
		WithAddress("ip://127.0.0.1:6379"),
	}
	optsReal = append(optsReal, opts...)
	redisClient = NewRedis(ctx, optsReal...)
	return redisClient
}

func TestNewRedis(t *testing.T) {
	ctx := context.Background()
	testOnBorrow := func(c redis.Conn, t time.Time) error {
		_, err := c.Do("PING")
		return err
	}
	opts := []Option{
		WithAddress("ip://127.0.0.1:6379"),
		WithConnectTimeout(100 * time.Millisecond),
		WithWriteTimeout(200 * time.Millisecond),
		WithReadTimeout(300 * time.Millisecond),
		WithPassword("123456"),
		WithMaxIdle(10),
		WithMaxActive(100),
		WithIdleTimeout(5 * time.Minute),
		WithWait(false),
		WithTestOnBorrow(testOnBorrow),
		WithConnTryTimes(3),
	}
	_ = newRedisClient(ctx, opts...)
	/*
		if redisClient.redisOpts.Address != "ip://127.0.0.1:6379" {
			t.Errorf("Address set failed, real: %v, set: %v", redisClient.redisOpts.Address, "ip://127.0.0.1:6379")
		}
		if redisClient.redisOpts.ConnectTimeout != 100*time.Millisecond {
			t.Errorf("ConnectTimeout set failed, real: %v, set: %v", redisClient.redisOpts.ConnectTimeout, 100*time.Millisecond)
		}
		if redisClient.redisOpts.WriteTimeout != 200*time.Millisecond {
			t.Errorf("WriteTimeout set failed, real: %v, set: %v", redisClient.redisOpts.WriteTimeout, 200*time.Millisecond)
		}
		if redisClient.redisOpts.ReadTimeout != 300*time.Millisecond {
			t.Errorf("ReadTimeout set failed, real: %v, set: %v", redisClient.redisOpts.ReadTimeout, 300*time.Millisecond)
		}
		if redisClient.redisOpts.Password != "123456" {
			t.Errorf("Password set failed, real: %v, set: %v", redisClient.redisOpts.Password, "123456")
		}
		if redisClient.redisOpts.MaxIdle != 10 {
			t.Errorf("MaxIdle set failed, real: %v, set: %v", redisClient.redisOpts.MaxIdle, 10)
		}
		if redisClient.redisOpts.MaxActive != 100 {
			t.Errorf("MaxActive set failed, real: %v, set: %v", redisClient.redisOpts.MaxActive, 100)
		}
		if redisClient.redisOpts.IdleTimeout != 5*time.Minute {
			t.Errorf("IdleTimeout set failed, real: %v, set: %v", redisClient.redisOpts.IdleTimeout, 5*time.Minute)
		}
		if redisClient.redisOpts.Wait {
			t.Errorf("Wait set failed, real: %v, set: %v", redisClient.redisOpts.Wait, false)
		}
	*/
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:get"
	{
		fmt.Printf("redisClient.Set: %p\n", redisClient.Set)
		reply, err := redisClient.Set(ctx, key, "123")
		if reply != "OK" || err != nil {
			t.Errorf("Redis.Set failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.GetString(ctx, key)
		if reply != "123" || err != nil {
			t.Errorf("Redis.GetString failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.GetBytes(ctx, key)
		if len(reply) != 3 || reply[0] != '1' || reply[1] != '2' || reply[2] != '3' || err != nil {
			t.Errorf("Redis.GetBytes failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.GetString(ctx, key)
		if reply != "" || err != redigo.ErrNil {
			t.Errorf("Redis.Get failed, reply: %v, err: %v", reply, err)
		}
	}

	//单连接
	fmt.Printf("single conn\n")
	redisClientConn, err := redisClient.WithRedisConn(ctx)
	if err != nil {
		t.Errorf("Redis.WithRedisConn failed, err: %v", err)
	}
	{
		fmt.Printf("redisClientConn.Set: %p\n", redisClientConn.Set)
		reply, err := redisClientConn.Set(ctx, key, "123")
		if reply != "OK" || err != nil {
			t.Errorf("Redis.Set failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClientConn.GetString(ctx, key)
		if reply != "123" || err != nil {
			t.Errorf("Redis.GetString failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClientConn.GetBytes(ctx, key)
		if len(reply) != 3 || reply[0] != '1' || reply[1] != '2' || reply[2] != '3' || err != nil {
			t.Errorf("Redis.GetBytes failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClientConn.Del(ctx, key, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClientConn.GetString(ctx, key)
		if reply != "" || err != redigo.ErrNil {
			t.Errorf("Redis.Get failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestSet(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:set"
	{
		// 测试普通set
		reply, err := redisClient.Set(ctx, key, "123")
		if reply != "OK" || err != nil {
			t.Errorf("Redis.Set failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		// 测试带EX的set
		{
			reply, err := redisClient.Set(ctx, key, 123, WithSetEX(1), WithSetXX())
			if reply != "OK" || err != nil {
				t.Errorf("Redis.Set with EX failed, reply: %v, err: %v", reply, err)
			}
		}
		{
			reply, err := redisClient.GetString(ctx, key)
			if reply != "123" || err != nil {
				t.Errorf("redis.Set with EX failed, reply: %v, err: %v", reply, err)
			}
		}
		{
			time.Sleep(2 * time.Second)
			reply, err := redisClient.GetString(ctx, key)
			if reply != "" || err != redigo.ErrNil {
				t.Errorf("redis.Set with EX failed, reply: %v, err: %v", reply, err)
			}
		}
	}
	{
		// 测试带PX的set
		{
			reply, err := redisClient.Set(ctx, key, []byte("123"), WithSetPX(1000), WithSetNX())
			if reply != "OK" || err != nil {
				t.Errorf("Redis.Set with PX failed, reply: %v, err: %v", reply, err)
			}
		}
		{
			reply, err := redisClient.GetString(ctx, key)
			if reply != "123" || err != nil {
				t.Errorf("redis.Set with PX failed, reply: %v, err: %v", reply, err)
			}
		}
		{
			time.Sleep(2 * time.Second)
			reply, err := redisClient.GetString(ctx, key)
			if reply != "" || err != redigo.ErrNil {
				t.Errorf("redis.Set with PX failed, reply: %v, err: %v", reply, err)
			}
		}
	}
	{
		// 测试带NX的set
		reply, err := redisClient.Set(ctx, key, "123", WithSetPX(1), WithSetNX())
		if reply != "OK" || err != nil {
			t.Errorf("redisClient.Set with XX failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		// 测试带XX的set
		time.Sleep(2 * time.Millisecond)
		reply, err := redisClient.Set(ctx, key, "123", WithSetXX())
		if reply != "" || err != redigo.ErrNil {
			t.Errorf("redisClient.Set with XX failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestDel(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	keys := []string{"key:test:del1", "key:test:del2", "key:test:del3"}
	{
		for i := 0; i < len(keys); i++ {
			reply, err := redisClient.Set(ctx, keys[i], strconv.Itoa(i))
			if err != nil {
				t.Errorf("Redis.Set failed, key: %v, reply: %v, err: %v", keys[i], reply, err)
			}
		}
	}
	{
		reply, err := redisClient.Del(ctx, keys...)
		if reply != int64(len(keys)) || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v, keys: %v", reply, err, keys)
		}
	}
}

func TestZAdd(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zadd"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 2.0, 3.0}
	{
		// 普通zadd
		reply, err := redigo.Int64(redisClient.ZAdd(ctx, key, members, scores))
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		// incr
		reply, err := redisClient.ZAddIncr(ctx, key, "a", 4.0)
		if reply != 5.0 || err != nil {
			t.Errorf("Redis.ZAddIncr failed, reply: %v, err: %v", reply, err)
		}

		reply, err = redisClient.ZAddIncr(ctx, key, "a", 5.0, WithZAddNX())
		if reply != 0 || err != redigo.ErrNil {
			t.Errorf("Redis.ZAddIncr failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		// ch
		reply, err := redisClient.ZAdd(ctx, key, []string{"a"}, []float64{6.0}, WithZAddCH())
		if reply != 1 || err != nil {
			t.Errorf("Redis.ZAdd incr ch failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		// xx
		reply, err := redisClient.ZAdd(ctx, key, []string{"a"}, []float64{1.0}, WithZAddXX(), WithZAddCH())
		if reply != 1 || err != nil {
			t.Errorf("Redis.ZAdd xx failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		// nx
		reply, err := redisClient.ZAdd(ctx, key, []string{"d"}, []float64{4.0}, WithZAddNX())
		if reply != 1 || err != nil {
			t.Errorf("Redis.ZAdd nx failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZCount(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zcount"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 2.0, 3.0}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZCount(ctx, key, "(1", "(3")
		if reply != 1 || err != nil {
			t.Errorf("Redis.ZCount failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZCount(ctx, key, "-inf", "(3")
		if reply != 2 || err != nil {
			t.Errorf("Redis.ZCount failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZCount(ctx, key, "1", "3")
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZCount failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZRank(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zrank"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 2.0, 3.0}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZRank(ctx, key, "a")
		if reply != 0 || err != nil {
			t.Errorf("Redis.ZRank failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZRank(ctx, key, "b")
		if reply != 1 || err != nil {
			t.Errorf("Redis.ZRank failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZRank(ctx, key, "c")
		if reply != 2 || err != nil {
			t.Errorf("Redis.ZRank failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZRank(ctx, key, "d")
		if reply != 0 || err != redigo.ErrNil {
			t.Errorf("Redis.ZRank failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZRank(ctx, key+"2333", "a")
		if reply != 0 || err != redigo.ErrNil {
			t.Errorf("Redis.ZRank failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZCard(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zcard"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 2.0, 3.0}
	{
		reply, err := redisClient.ZCard(ctx, key)
		if reply != 0 || err != nil {
			t.Errorf("Redis.ZCard failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZCard(ctx, key)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZCard failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZRem(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zrem"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 2.0, 3.0}
	{
		reply, err := redisClient.ZRem(ctx, key, "a")
		if reply != 0 || err != nil {
			t.Errorf("Redis.ZRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZRem(ctx, key, "a", "c")
		if reply != 2 || err != nil {
			t.Errorf("Redis.ZRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		if len(members) != 1 || len(scores) != 1 || err != nil || members[0] != "b" || scores[0] != 2.0 {
			t.Errorf("Redis.ZRem failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZRange(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zrange"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 3.0, 2.0}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		if len(members) != 3 || len(scores) != 3 || err != nil || members[0] != "a" || members[1] != "c" ||
			members[2] != "b" || scores[0] != 1.0 || scores[1] != 2.0 || scores[2] != 3.0 {
			t.Errorf("Redis.ZRange withscores failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1)
		if len(members) != 3 || len(scores) != 0 || err != nil || members[0] != "a" || members[1] != "c" || members[2] != "b" {
			t.Errorf("Redis.ZRange failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZRevRange(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zrevrange"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 3.0, 2.0}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		members, scores, err := redisClient.ZRevRange(ctx, key, 0, -1, WithZRangeWithScores())
		if len(members) != 3 || len(scores) != 3 || err != nil || members[0] != "b" || members[1] != "c" ||
			members[2] != "a" || scores[0] != 3.0 || scores[1] != 2.0 || scores[2] != 1.0 {
			t.Errorf("Redis.ZRevRange withscores failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		members, scores, err := redisClient.ZRevRange(ctx, key, 0, 2)
		if len(members) != 3 || len(scores) != 0 || err != nil || members[0] != "b" || members[1] != "c" || members[2] != "a" {
			t.Errorf("Redis.ZRevRange failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZRangeByScore(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zrangebyscore"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 2.0, 3.0}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		members, scores, err := redisClient.ZRangeByScore(ctx, key, "(1", "3")
		if len(members) != 2 || len(scores) != 0 || err != nil || members[0] != "b" || members[1] != "c" {
			t.Errorf("Redis.ZRangeByScore failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		members, scores, err := redisClient.ZRangeByScore(ctx, key, "(1", "3", WithZRangeByScoreWithScores())
		if len(members) != 2 || len(scores) != 2 || err != nil || members[0] != "b" || members[1] != "c" || scores[0] != 2.0 || scores[1] != 3.0 {
			t.Errorf("Redis.ZRangeByScore failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		members, scores, err := redisClient.ZRangeByScore(ctx, key, "1", "(3", WithZRangeByScoreLimitOffset(1), WithZRangeByScoreLimitCount(2))
		if len(members) != 1 || len(scores) != 0 || err != nil || members[0] != "b" {
			t.Errorf("Redis.ZRangeByScore failed, members: %v, score: %v, err: %v", members, scores, err)
		}
	}
	{
		members, scores, err := redisClient.ZRangeByScore(ctx, key, "-inf", "+inf", WithZRangeByScoreLimitOffset(1), WithZRangeByScoreLimitCount(2), WithZRangeByScoreWithScores())
		if len(members) != 2 || len(scores) != 2 || err != nil || members[0] != "b" || members[1] != "c" || scores[0] != 2.0 || scores[1] != 3.0 {
			t.Errorf("Redis.ZRangeByScore failed, members: %v, score: %v, err: %v", members, scores, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZRevRangeByScore(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zrevrangebyscore"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 2.0, 3.0}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		members, scores, err := redisClient.ZRevRangeByScore(ctx, key, "(1", "3")
		if len(members) != 2 || len(scores) != 0 || err != nil || members[0] != "c" || members[1] != "b" {
			t.Errorf("Redis.ZRevRangeByScore failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		members, scores, err := redisClient.ZRevRangeByScore(ctx, key, "(1", "3", WithZRangeByScoreWithScores())
		if len(members) != 2 || len(scores) != 2 || err != nil || members[0] != "c" || members[1] != "b" || scores[0] != 3.0 || scores[1] != 2.0 {
			t.Errorf("Redis.ZRevRangeByScore failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		members, scores, err := redisClient.ZRevRangeByScore(ctx, key, "1", "(3", WithZRangeByScoreLimitOffset(1), WithZRangeByScoreLimitCount(2))
		if len(members) != 1 || len(scores) != 0 || err != nil || members[0] != "a" {
			t.Errorf("Redis.ZRevRangeByScore failed, members: %v, score: %v, err: %v", members, scores, err)
		}
	}
	{
		members, scores, err := redisClient.ZRevRangeByScore(ctx, key, "-inf", "+inf", WithZRangeByScoreLimitOffset(1), WithZRangeByScoreLimitCount(2), WithZRangeByScoreWithScores())
		if len(members) != 2 || len(scores) != 2 || err != nil || members[0] != "b" || members[1] != "a" || scores[0] != 2.0 || scores[1] != 1.0 {
			t.Errorf("Redis.ZRevRangeByScore failed, members: %v, score: %v, err: %v", members, scores, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZScore(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zscore"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 2.0, 3.0}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZScore(ctx, key, "a")
		if reply != 1.0 || err != nil {
			t.Errorf("Redis.ZScore failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZScore(ctx, key, "d")
		if reply != 0 || err != redigo.ErrNil {
			t.Errorf("Redis.ZScore failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZScore(ctx, key+"2333", "a")
		if reply != 0 || err != redigo.ErrNil {
			t.Errorf("Redis.ZScore failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZIncrBy(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zincrby"
	{
		reply, err := redisClient.ZIncrBy(ctx, key, "a", 3.0)
		if reply != 3.0 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZScore(ctx, key, "a")
		if reply != 3.0 || err != nil {
			t.Errorf("Redis.ZScore failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZIncrBy(ctx, key, "a", 3.0)
		if reply != 6.0 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZScore(ctx, key, "a")
		if reply != 6.0 || err != nil {
			t.Errorf("Redis.ZScore failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZRemRangeByRank(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zremrangebyrank"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 2.0, 3.0}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZRemRangeByRank(ctx, key, 1, 2)
		if reply != 2 || err != nil {
			t.Errorf("Redis.ZRemRangeByRank failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		if len(members) != 1 || len(scores) != 1 || err != nil || members[0] != "a" || scores[0] != 1.0 {
			t.Errorf("Redis.ZRemRangeByRank failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestZRemRangeByScore(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:zremrangebyscore"
	members := []string{"a", "b", "c"}
	scores := []float64{1.0, 2.0, 3.0}
	{
		reply, err := redisClient.ZAdd(ctx, key, members, scores)
		if reply != 3 || err != nil {
			t.Errorf("Redis.ZAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ZRemRangeByScore(ctx, key, "(1", "(3")
		if reply != 1 || err != nil {
			t.Errorf("Redis.ZRemRangeByScore failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		members, scores, err := redisClient.ZRevRangeByScore(ctx, key, "-inf", "inf", WithZRangeByScoreWithScores())
		if len(members) != 2 || len(scores) != 2 || err != nil || members[0] != "c" || members[1] != "a" || scores[0] != 3.0 || scores[1] != 1.0 {
			t.Errorf("Redis.ZRemRangeByScore failed, members: %v, scores: %v, err: %v", members, scores, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestMget(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	keys := []string{"key:test:mget1", "key:test:mget2", "key:test:mget3"}
	values := []string{"value:test:mget1", "value:test:mget2", "value:test:mget3"}
	{
		reply, err := redisClient.MgetString(ctx, keys...)
		if err != nil || len(reply) != 3 || reply[0] != "" || reply[1] != "" || reply[2] != "" {
			t.Errorf("Redis.Mget failed, reply: %+v, err: %v", reply, err)
		}
	}
	{
		for i := 0; i < len(keys); i++ {
			reply, err := redisClient.Set(ctx, keys[i], values[i])
			if reply != "OK" || err != nil {
				t.Errorf("Redis.Set failed, key: %v, value: %v, reply: %+v, err: %v", keys[i], values[i], reply, err)
			}
		}
	}
	{
		reply, err := redisClient.MgetBytes(ctx, keys...)
		if err != nil || len(reply) != 3 || string(reply[0]) != values[0] || string(reply[1]) != values[1] || string(reply[2]) != values[2] {
			t.Errorf("Redis.Mget failed, reply: %+v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, keys...)
		if err != nil || reply != 3 {
			t.Errorf("Redis.Del failed, reply: %+v, err: %v", reply, err)
		}
	}
}

func TestMset(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	keys := []string{"key:test:mset1", "key:test:mset2", "key:test:mset3"}
	values := [][]byte{[]byte("value:test:mset1"), []byte("value:test:mset2"), []byte("value:test:mset3")}
	{
		data := make([]interface{}, 0, len(values))
		for i := 0; i < len(values); i++ {
			data = append(data, values[i])
		}
		reply, err := redisClient.Mset(ctx, keys, data)
		if reply != "OK" || err != nil {
			t.Errorf("Redis.Mset failed, reply: %+v, err: %v, keys: %+v, values: %+v", reply, err, keys, data)
		}
	}
	{
		reply, err := redisClient.MgetString(ctx, keys...)
		if err != nil || len(reply) != 3 || reply[0] != "value:test:mset1" || reply[1] != "value:test:mset2" || reply[2] != "value:test:mset3" {
			t.Errorf("Redis.Mget failed, reply: %+v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, keys...)
		if err != nil || reply != 3 {
			t.Errorf("Redis.Del failed, reply: %+v, err: %v", reply, err)
		}
	}
}

func TestIncr(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:incr"
	{
		reply, err := redisClient.Incr(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Incr failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Incr(ctx, key)
		if reply != 2 || err != nil {
			t.Errorf("Redis.Incr failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestDecr(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:decr"
	{
		reply, err := redisClient.Decr(ctx, key)
		if reply != -1 || err != nil {
			t.Errorf("Redis.Decr failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Decr(ctx, key)
		if reply != -2 || err != nil {
			t.Errorf("Redis.Decr failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestIncrBy(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:incrby"
	{
		reply, err := redisClient.IncrBy(ctx, key, -2333)
		if reply != -2333 || err != nil {
			t.Errorf("Redis.IncrBy failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.IncrBy(ctx, key, 233)
		if reply != -2100 || err != nil {
			t.Errorf("Redis.IncrBy failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestDecrBy(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:decrby"
	{
		reply, err := redisClient.DecrBy(ctx, key, -666)
		if reply != 666 || err != nil {
			t.Errorf("Redis.DecrBy failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.DecrBy(ctx, key, 233)
		if reply != 433 || err != nil {
			t.Errorf("Redis.DecrBy failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestExists(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:exists"
	{
		reply, err := redisClient.Exists(ctx)
		if reply != 0 || err != ErrorParamInvalid {
			t.Errorf("Redis.Exists failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Exists(ctx, key)
		if reply != 0 || err != nil {
			t.Errorf("Redis.Exists failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Set(ctx, key, "123")
		if reply != "OK" || err != nil {
			t.Errorf("Redis.Set failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Exists(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Exists failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestExpire(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:expire"
	{
		reply, err := redisClient.Expire(ctx, key, 2333)
		if reply != 0 || err != nil {
			t.Errorf("Redis.Expire failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Set(ctx, key, "123")
		if reply != "OK" || err != nil {
			t.Errorf("Redis.Set failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Expire(ctx, key, 1)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Expire failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		time.Sleep(2 * time.Second)
		reply, err := redisClient.GetString(ctx, key)
		if reply != "" || err != redigo.ErrNil {
			t.Errorf("Redis.Get failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 0 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestExpireAt(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:expireat"
	{
		reply, err := redisClient.ExpireAt(ctx, key, 2333)
		if reply != 0 || err != nil {
			t.Errorf("Redis.ExpireAt failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Set(ctx, key, "123")
		if reply != "OK" || err != nil {
			t.Errorf("Redis.Set failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.ExpireAt(ctx, key, time.Now().Unix()+1)
		if reply != 1 || err != nil {
			t.Errorf("Redis.ExpireAt failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		time.Sleep(2 * time.Second)
		reply, err := redisClient.GetString(ctx, key)
		if reply != "" || err != redigo.ErrNil {
			t.Errorf("Redis.GetString failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 0 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestLPush(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:lpush"
	{
		reply, err := redisClient.LPush(ctx, key, 1, "2", 3.0, "4")
		if reply != 4 || err != nil {
			t.Errorf("Redis.LPush failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LRange(ctx, key, 0, -1)
		if len(reply) != 4 || err != nil || reply[0] != "4" || reply[1] != "3" || reply[2] != "2" || reply[3] != "1" {
			t.Errorf("Redis.LRange failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestRPush(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:rpush"
	{
		reply, err := redisClient.RPush(ctx, key, "1", 2, "3", 4.0)
		if reply != 4 || err != nil {
			t.Errorf("Redis.RPush failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LRange(ctx, key, 0, -1)
		if len(reply) != 4 || err != nil || reply[0] != "1" || reply[1] != "2" || reply[2] != "3" || reply[3] != "4" {
			t.Errorf("Redis.LRange failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestLPop(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:lpop"
	{
		reply, err := redisClient.LPop(ctx, key)
		if reply != "" || err != redigo.ErrNil {
			t.Errorf("Redis.LPop failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LPush(ctx, key, 1, 2, 3)
		if reply != 3 || err != nil {
			t.Errorf("Redis.LPush failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LPop(ctx, key)
		if reply != "3" || err != nil {
			t.Errorf("Redis.LPop failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestRPop(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:rpop"
	{
		reply, err := redisClient.RPop(ctx, key)
		if reply != "" || err != redigo.ErrNil {
			t.Errorf("Redis.LPop failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.RPush(ctx, key, 1, 2, 3)
		if reply != 3 || err != nil {
			t.Errorf("Redis.LPush failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.RPop(ctx, key)
		if reply != "3" || err != nil {
			t.Errorf("Redis.LPop failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestLRange(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:lrange"
	{
		reply, err := redisClient.LRange(ctx, key, 0, -1)
		if len(reply) != 0 || err != nil {
			t.Errorf("Redis.LRange failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LPush(ctx, key, 1, 2, 3)
		if reply != 3 || err != nil {
			t.Errorf("Redis.LPush failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LRange(ctx, key, 1, 1)
		if len(reply) != 1 || reply[0] != "2" || err != nil {
			t.Errorf("Redis.LRange failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestLRem(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:lrem"
	{
		reply, err := redisClient.LPush(ctx, key, "1", "2", "3", "1", "4", "5", "1", "6", "7", "1")
		if reply != 10 || err != nil {
			t.Errorf("Redis.LPush failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LRem(ctx, key, 2, 1)
		if reply != 2 || err != nil {
			t.Errorf("Redis.LRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LRange(ctx, key, 0, -1)
		if len(reply) != 8 || err != nil || reply[0] != "7" || reply[1] != "6" || reply[2] != "5" ||
			reply[3] != "4" || reply[4] != "1" || reply[5] != "3" || reply[6] != "2" || reply[7] != "1" {
			t.Errorf("Redis.LRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LRem(ctx, key, -1, 1)
		if reply != 1 || err != nil {
			t.Errorf("Redis.LRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LRange(ctx, key, 0, -1)
		if len(reply) != 7 || err != nil || reply[0] != "7" || reply[1] != "6" || reply[2] != "5" ||
			reply[3] != "4" || reply[4] != "1" || reply[5] != "3" || reply[6] != "2" {
			t.Errorf("Redis.LRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LRem(ctx, key, 0, 1)
		if reply != 1 || err != nil {
			t.Errorf("Redis.LRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LRange(ctx, key, 0, -1)
		if len(reply) != 6 || err != nil || reply[0] != "7" || reply[1] != "6" || reply[2] != "5" ||
			reply[3] != "4" || reply[4] != "3" || reply[5] != "2" {
			t.Errorf("Redis.LRange failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestLLen(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:llen"
	{
		reply, err := redisClient.LPush(ctx, key, 1, 2, 3, 4)
		if reply != 4 || err != nil {
			t.Errorf("Redis.LPush failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LLen(ctx, key)
		if reply != 4 || err != nil {
			t.Errorf("Redis.LLen failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestLIndex(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:lindex"
	{
		reply, err := redisClient.LIndex(ctx, key, 1)
		if reply != "" || err != redigo.ErrNil {
			t.Errorf("Redis.LIndex failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LPush(ctx, key, 1, "2", 3.0, 4)
		if reply != 4 || err != nil {
			t.Errorf("Redis.LPush failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LIndex(ctx, key, -2)
		if reply != "2" || err != nil {
			t.Errorf("Redis.LIndex failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.LIndex(ctx, key, 1)
		if reply != "3" || err != nil {
			t.Errorf("Redis.LIndex failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestSAdd(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:sadd"
	{
		reply, err := redisClient.SAdd(ctx, key, 1, "2", 3.0, 4)
		if reply != 4 || err != nil {
			t.Errorf("Redis.SAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SMembers(ctx, key)
		m := make(map[string]bool)
		for i := 0; i < len(reply); i++ {
			m[reply[i]] = true
		}
		if len(m) != 4 || err != nil || !m["1"] || !m["2"] || !m["3"] || !m["4"] {
			t.Errorf("Redis.SMembers failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestSCard(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:scard"
	{
		reply, err := redisClient.SCard(ctx, key)
		if reply != 0 || err != nil {
			t.Errorf("Redis.SCard failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SAdd(ctx, key, 1, 2, 3, 4)
		if reply != 4 || err != nil {
			t.Errorf("Redis.SAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SCard(ctx, key)
		if reply != 4 || err != nil {
			t.Errorf("Redis.SCard failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestSIsMember(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:sismember"
	{
		reply, err := redisClient.SIsMember(ctx, key, 1)
		if reply != 0 || err != nil {
			t.Errorf("Redis.SIsMember failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SAdd(ctx, key, 1)
		if reply != 1 || err != nil {
			t.Errorf("Redis.SAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SIsMember(ctx, key, 1)
		if reply != 1 || err != nil {
			t.Errorf("Redis.SIsMember failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestSMembers(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:smembers"
	{
		reply, err := redisClient.SMembers(ctx, key)
		if len(reply) != 0 || err != nil {
			t.Errorf("Redis.SMembers failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SAdd(ctx, key, 1, 2, 3, 4)
		if reply != 4 || err != nil {
			t.Errorf("Redis.SAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SMembers(ctx, key)
		m := make(map[string]bool)
		for i := 0; i < len(reply); i++ {
			m[reply[i]] = true
		}
		if len(m) != 4 || err != nil || !m["1"] || !m["2"] || !m["3"] || !m["4"] {
			t.Errorf("Redis.SMembers failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestSRem(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:srem"
	{
		reply, err := redisClient.SRem(ctx, key)
		if reply != 0 || err != ErrorParamInvalid {
			t.Errorf("Redis.SRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SRem(ctx, key, 1, 2, 3)
		if reply != 0 || err != nil {
			t.Errorf("Redis.SRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SAdd(ctx, key, 1, 2, 3)
		if reply != 3 || err != nil {
			t.Errorf("Redis.SAdd failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SRem(ctx, key, 1, 3)
		if reply != 2 || err != nil {
			t.Errorf("Redis.SRem failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.SMembers(ctx, key)
		if len(reply) != 1 || reply[0] != "2" || err != nil {
			t.Errorf("Redis.SMembers failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestHSet(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:hset"
	{
		reply, err := redisClient.HSet(ctx, key, []string{"a"}, []interface{}{1})
		if reply != 1 || err != nil {
			t.Errorf("Redis.HSet failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HSet(ctx, key, []string{"a"}, []interface{}{2})
		if reply != 0 || err != nil {
			t.Errorf("Redis.HSet failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HGet(ctx, key, "a")
		if reply != "2" || err != nil {
			t.Errorf("Redis.HGet failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestHGet(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:hget"
	{
		reply, err := redisClient.HGet(ctx, key, "1")
		if reply != "" || err != redigo.ErrNil {
			t.Errorf("Redis.HGet failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HSet(ctx, key, []string{"a"}, []interface{}{1})
		if reply != 1 || err != nil {
			t.Errorf("Redis.HSet failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HGet(ctx, key, "a")
		if reply != "1" || err != nil {
			t.Errorf("Redis.HGet failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestHGetAll(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:hgetall"
	{
		fields, values, err := redisClient.HGetAll(ctx, key)
		if len(fields) != 0 || len(values) != 0 || err != nil {
			t.Errorf("Redis.HGetAll failed, fields: %v, values: %v, err: %v", fields, values, err)
		}
	}
	{
		reply, err := redisClient.HMset(ctx, key, []string{"a", "b"}, []interface{}{1, 2})
		if reply != "OK" || err != nil {
			t.Errorf("Redis.HMSet failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		fields, values, err := redisClient.HGetAll(ctx, key)
		m := make(map[string]string)
		if len(fields) != 2 || len(values) != 2 || err != nil {
			t.Errorf("Redis.HGetAll failed, fields: %v, values: %v, err: %v", fields, values, err)
		}
		for i := 0; i < len(fields); i++ {
			m[fields[i]] = values[i]
		}
		if m["a"] != "1" || m["b"] != "2" {
			t.Errorf("Redis.HGetAll failed, fields: %v, values: %v, err: %v", fields, values, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestHKeys(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:hkeys"
	{
		reply, err := redisClient.HKeys(ctx, key)
		if reply == nil || len(reply) != 0 || err != nil {
			t.Errorf("Redis.HKeys failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HSet(ctx, key, []string{"a"}, []interface{}{1})
		if reply != 1 || err != nil {
			t.Errorf("Redis.HSet failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HKeys(ctx, key)
		if len(reply) != 1 || reply[0] != "a" || err != nil {
			t.Errorf("Redis.HKeys failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestHSetNX(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:hsetnx"
	{
		reply, err := redisClient.HSetNX(ctx, key, "a", 1)
		if reply != 1 || err != nil {
			t.Errorf("Redis.HSetNX failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HSetNX(ctx, key, "a", 2)
		if reply != 0 || err != nil {
			t.Errorf("Redis.HSetNX failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HGet(ctx, key, "a")
		if reply != "1" || err != nil {
			t.Errorf("Redis.HGet failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestHMset(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:hmset"
	{
		fields, values, err := redisClient.HGetAll(ctx, key)
		if len(fields) != 0 || len(values) != 0 || err != nil {
			t.Errorf("Redis.HGetAll failed, fields: %v, values: %v, err: %v", fields, values, err)
		}
	}
	{
		reply, err := redisClient.HMset(ctx, key, []string{"a", "b", "c"}, []interface{}{1, 2, 3})
		if reply != "OK" || err != nil {
			t.Errorf("Redis.HMset failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		fields, values, err := redisClient.HGetAll(ctx, key)
		if len(fields) != 3 || len(values) != 3 || err != nil {
			t.Errorf("Redis.HGetAll failed, fields: %v, values: %v, err: %v", fields, values, err)
		}
		m := make(map[string]string)
		for i := 0; i < len(fields); i++ {
			m[fields[i]] = values[i]
		}
		if m["a"] != "1" || m["b"] != "2" || m["c"] != "3" {
			t.Errorf("Redis.HGetAll failed, fields: %v, values: %v, err: %v", fields, values, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestHMget(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:hmget"
	{
		reply, err := redisClient.HMget(ctx, key)
		if len(reply) != 0 || err != ErrorParamInvalid {
			t.Errorf("Redis.HMget failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HMget(ctx, key, "a", "b")
		if len(reply) != 2 || err != nil || reply[0] != "" || reply[1] != "" {
			t.Errorf("Redis.HMget failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HMset(ctx, key, []string{"a", "b"}, []interface{}{1, 2})
		if reply != "OK" || err != nil {
			t.Errorf("Redis.HMset failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.HMget(ctx, key, "a", "b", "c")
		if len(reply) != 3 || err != nil || reply[0] != "1" || reply[1] != "2" || reply[2] != "" {
			t.Errorf("Redis.HMget failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestHScan(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:hscan"
	{
		reply, err := redisClient.HMset(ctx, key, []string{"a", "b", "c", "d", "aaa"}, []interface{}{1, 2, 3, 4, 5})
		if reply != "OK" || err != nil {
			t.Errorf("Redis.HMset failed, reply: %v, err: %v", reply, err)
		}
	}
	{
		var cursorBegin int64 = 0
		m := make(map[string]string)
		for {
			cursorNext, fields, values, err := redisClient.HScan(ctx, key, cursorBegin, WithHScanPattern("a*"), WithHScanCount(10))
			if len(fields) != len(values) {
				t.Errorf("Redis.HScan failed, cursorNext: %v, fields: %v, values: %v, err: %v", cursorNext, fields, values, err)
			}
			for i := 0; i < len(fields); i++ {
				m[fields[i]] = values[i]
			}
			cursorBegin = cursorNext
			if cursorBegin == 0 {
				break
			}
		}
		if len(m) != 2 || m["a"] != "1" || m["aaa"] != "5" {
			t.Errorf("Redis.HScan failed, m: %v", m)
		}
	}
	{
		var cursorBegin int64 = 0
		m := make(map[string]string)
		for {
			cursorNext, fields, values, err := redisClient.HScan(ctx, key, cursorBegin)
			if len(fields) != len(values) {
				t.Errorf("Redis.HScan failed, cursorNext: %v, fields: %v, values: %v, err: %v", cursorNext, fields, values, err)
			}
			for i := 0; i < len(fields); i++ {
				m[fields[i]] = values[i]
			}
			cursorBegin = cursorNext
			if cursorBegin == 0 {
				break
			}
		}
		if len(m) != 5 || m["a"] != "1" || m["b"] != "2" || m["c"] != "3" || m["d"] != "4" || m["aaa"] != "5" {
			t.Errorf("Redis.HScan failed, m: %v", m)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestPipeline(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "key:test:pipeline:zset:"
	{
		for i := 0; i < 200; i++ {
			reply, err := redisClient.ZAdd(ctx, key+strconv.FormatInt(int64(i), 10), []string{"a"}, []float64{float64(i)})
			if err != nil {
				t.Errorf("Redis.HMset failed, reply: %v, err: %v", reply, err)
			}
		}
	}
	{
		commandArgs := make([]CommandArgs, 0)
		for i := 0; i < 200; i++ {
			commandArgs = append(commandArgs, Command("ZSCORE", key+strconv.FormatInt(int64(i), 10), "a"))
		}

		replys, err := redisClient.Pipeline(ctx, commandArgs)
		if err != nil {
			t.Errorf("Redis.Pipeline failed, replys: %+v, err: %v", replys, err)
		} else {
			for i := range replys {
				score, err := redigo.Float64(replys[i].Reply, replys[i].Err)
				if score != float64(i) || err != nil {
					t.Errorf("Redis.Pipeline result invalid")
				}
			}
		}
	}
	{
		for i := 0; i < 200; i++ {
			reply, err := redisClient.Del(ctx, key+strconv.FormatInt(int64(i), 10))
			if err != nil {
				t.Errorf("Redis.HMset failed, reply: %v, err: %v", reply, err)
			}
		}
	}

	//单连接
	{
		redisClientConn, err := redisClient.WithRedisConn(ctx)
		if err != nil {
			t.Errorf("WithRedisConn err:%v", err)
			return
		}
		defer redisClientConn.Close()
		{
			for i := 0; i < 200; i++ {
				reply, err := redisClientConn.ZAdd(ctx, key+strconv.FormatInt(int64(i), 10), []string{"a"}, []float64{float64(i)})
				if err != nil {
					t.Errorf("Redis.HMset failed, reply: %v, err: %v", reply, err)
				}
			}
		}
		{
			commandArgs := make([]CommandArgs, 0)
			for i := 0; i < 200; i++ {
				commandArgs = append(commandArgs, Command("ZSCORE", key+strconv.FormatInt(int64(i), 10), "a"))
			}

			replys, err := redisClientConn.Pipeline(ctx, commandArgs)
			if err != nil {
				t.Errorf("Redis.Pipeline failed, replys: %+v, err: %v", replys, err)
			} else {
				for i := range replys {
					score, err := redigo.Float64(replys[i].Reply, replys[i].Err)
					if score != float64(i) || err != nil {
						t.Errorf("Redis.Pipeline result invalid")
					}
				}
			}
		}
		{
			for i := 0; i < 200; i++ {
				reply, err := redisClientConn.Del(ctx, key+strconv.FormatInt(int64(i), 10))
				if err != nil {
					t.Errorf("Redis.HMset failed, reply: %v, err: %v", reply, err)
				}
			}
		}
	}
}

func TestSetBytesCas(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:setcas"
	var cas int32 = 0
	err := redisClient.SetBytesCas(ctx, key, []byte("123"), cas)
	if err != nil {
		t.Errorf("Redis.SetBytesCas err:%v", err)
	}

	val, casget, err := redisClient.GetBytesCas(ctx, key)
	if err != nil {
		t.Errorf("Redis.GetBytesCas err:%v", err)
	}
	if casget != cas+1 {
		t.Errorf("Redis.GetBytesCas cas not match, casget:%d, cas:%d", casget, cas+1)
	}
	if string(val) != "123" {
		t.Errorf("Redis.GetBytesCas val not match, val:%s", string(val))
	}

	cas = casget
	err = redisClient.SetBytesCas(ctx, key, []byte("123"), cas)
	if err != nil {
		t.Errorf("Redis.GetBytesCas err:%v", err)
	}

	cas = 0
	err = redisClient.SetBytesCas(ctx, key, []byte("123"), cas)
	if err != ErrorCasConflict {
		t.Errorf("Redis.SetBytesCas not conflict err:%v", err)
	}

	reply, err := redisClient.Del(ctx, key, key)
	if reply != 1 || err != nil {
		t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
	}
}

func TestHSetCas(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:hset:cas"
	var cas int32 = 0
	{
		err := redisClient.HSetCas(ctx, key, "f1", "v1", cas)
		if err != nil {
			t.Errorf("Redis.HSet failed, err: %v", err)
		}
	}
	{
		err := redisClient.HSetCas(ctx, key, "f2", "v2", cas)
		if err != nil {
			t.Errorf("Redis.HSet failed, err: %v", err)
		}
	}
	{
		reply, casget, err := redisClient.HGetCas(ctx, key, "f1")
		if reply != "v1" || err != nil {
			t.Errorf("Redis.HGetCas failed, reply: %v, err: %v", reply, err)
		}
		if casget != cas+1 {
			t.Errorf("Redis.HGetCas cas not match, casget:%d, cas:%d", casget, cas+1)
		}
	}
	{
		err := redisClient.HSetCas(ctx, key, "f2", "v22", cas)
		if err != ErrorCasConflict {
			t.Errorf("Redis.HSetCas not conflict err:%v", err)
		}
	}
	{
		reply, err := redisClient.Del(ctx, key)
		if reply != 1 || err != nil {
			t.Errorf("Redis.Del failed, reply: %v, err: %v", reply, err)
		}
	}
}

func TestLock(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:lock"
	lock := NewLock(ctx, key, "123", 100)
	{
		ok, err := redisClient.TryLock(ctx, lock)
		if err != nil {
			t.Errorf("Redis.TryLock failed, err: %v", err)
		}
		if !ok {
			t.Errorf("Redis.TryLock not ok")
		}
	}
	{
		ok, err := redisClient.TryLock(ctx, lock)
		if err != nil {
			t.Errorf("Redis.TryLock failed, err: %v", err)
		}
		if ok {
			t.Errorf("Redis.TryLock duplicate err")
		}
	}
	{
		ok, err := redisClient.Unlock(ctx, lock)
		if err != nil || !ok {
			t.Errorf("Redis.Unlock failed, ok: %t, err: %v", ok, err)
		}
	}
}

func TestHMSetKeyCas(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:hmset:key:cas"
	type teststruct struct {
		A int `redis:"a"`
		B int `redis:"b"`
	}
	{
		var err error
		// 检查异常参数
		_, err = redisClient.HMSetKeyCas(ctx, key, 0, redigo.Args{})
		if err != ErrorParamInvalid {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v should be ErrorParamInvalid", err)
		}
		_, err = redisClient.HMSetKeyCas(ctx, key, 0, redigo.Args{"a"})
		if err != ErrorParamInvalid {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v should be ErrorParamInvalid", err)
		}
	}
	{
		// 测试基本功能
		ts := &teststruct{
			A: 111,
			B: 222,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, 0, args)
		if err != nil {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v", err)
		} else if reply != "OK" {
			t.Errorf("Redis.HMSetKeyCas failed, reply: %v, err: %v", reply, err)
		}
		// 测试写入的值是否成功
		replys, err := redigo.Strings(redisClient.HGetAllRaw(ctx, key))
		if err != nil {
			t.Errorf("Redis.HGetAllRaw failed, err: %v", err)
		}
		// t.Errorf("replys: %v", replys)
		for i := 0; i < len(replys); i += 2 {
			if replys[i] == "a" && replys[i+1] != "111" || replys[i] == "b" && replys[i+1] != "222" ||
				replys[i] == "_cas" && replys[i+1] != "1" {
				t.Errorf("Redis.HMSetKeyCas failed, key: %v, value: %v", replys[i], replys[i+1])
			}
		}
	}
	{
		// 测试cas写入冲突无法成功写入
		ts := &teststruct{
			A: 333,
			B: 444,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, 0, args)
		if reply != "" || err != ErrorCasConflict {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v should be ErrorCasConflict, reply: %v should be 10001", err,
				reply)
		}
		replys, err := redigo.Strings(redisClient.HGetAllRaw(ctx, key))
		if err != nil {
			t.Errorf("Redis.HGetAllRaw failed, err: %v", err)
		}
		// t.Errorf("replys: %v", replys)
		for i := 0; i < len(replys); i += 2 {
			if replys[i] == "a" && replys[i+1] != "111" || replys[i] == "b" && replys[i+1] != "222" ||
				replys[i] == "_cas" && replys[i+1] != "1" {
				t.Errorf("Redis.HMSetKeyCas failed, key: %v, value: %v", replys[i], replys[i+1])
			}
		}
	}
	{
		_, err := redisClient.HSet(ctx, key, []string{"_cas"}, []interface{}{math.MaxInt32})
		if err != nil {
			t.Errorf("Redis.HSet failed, err: %v", err)
		}
		// 测试MaxInt32写入归零
		ts := &teststruct{
			A: 555,
			B: 666,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, math.MaxInt32, args)
		if err != nil || reply != "OK" {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v", err)
		}
		// 测试写入的值是否成功
		replys, err := redigo.Strings(redisClient.HGetAllRaw(ctx, key))
		if err != nil {
			t.Errorf("Redis.HGetAllRaw failed, err: %v", err)
		}
		// t.Errorf("replys: %v", replys)
		for i := 0; i < len(replys); i += 2 {
			if replys[i] == "a" && replys[i+1] != "555" || replys[i] == "b" && replys[i+1] != "666" ||
				replys[i] == "_cas" && replys[i+1] != "0" {
				t.Errorf("Redis.HMSetKeyCas failed, key: %v, value: %v", replys[i], replys[i+1])
			}
		}
	}
	{
		// 测试归零后重新写入
		ts := &teststruct{
			A: 777,
			B: 888,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, 0, args)
		if err != nil {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v", err)
		} else if reply != "OK" {
			t.Errorf("Redis.HMSetKeyCas failed, reply: %v, err: %v", reply, err)
		}
		// 测试写入的值是否成功
		replys, err := redigo.Strings(redisClient.HGetAllRaw(ctx, key))
		if err != nil {
			t.Errorf("Redis.HGetAllRaw failed, err: %v", err)
		}
		// t.Errorf("replys: %v", replys)
		for i := 0; i < len(replys); i += 2 {
			if replys[i] == "a" && replys[i+1] != "777" || replys[i] == "b" && replys[i+1] != "888" ||
				replys[i] == "_cas" && replys[i+1] != "1" {
				t.Errorf("Redis.HMSetKeyCas failed, key: %v, value: %v", replys[i], replys[i+1])
			}
		}
	}
	{
		// 清除数据
		ret, err := redisClient.Del(ctx, key)
		if ret != 1 || err != nil {
			t.Errorf("Redis.Del failed, err: %v, ret: %v", err, ret)
		}
	}
}

func TestHMGetKeyCas(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:hmget:key:cas"
	type teststruct struct {
		A int `redis:"a"`
		B int `redis:"b"`
	}
	{
		// 测试基本功能
		ts := &teststruct{
			A: 111,
			B: 222,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, 0, args)
		if err != nil {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v", err)
		} else if reply != "OK" {
			t.Errorf("Redis.HMSetKeyCas failed, reply: %v, err: %v", reply, err)
		}
		// 测试写入的值是否成功
		tmp, cas, err := redisClient.HMGetKeyCas(ctx, key, redigo.Args{"a", "b", "c"})
		replys, err := redigo.Strings(tmp, err)
		if err != nil {
			t.Errorf("Redis.HMGetKeyCas failed, err: %v", err)
		} else {
			// t.Errorf("replys: %v", replys)
			if cas != 1 || len(replys) != 3 || replys[0] != "111" || replys[1] != "222" || replys[2] != "" {
				t.Errorf("Redis.HMGetKeyCas failed, cas: %v, replys: %v, len: %v", cas, replys, len(replys))
			}
		}
	}
	{
		_, err := redisClient.HSet(ctx, key, []string{"_cas"}, []interface{}{math.MaxInt32})
		if err != nil {
			t.Errorf("Redis.HSet failed, err: %v", err)
		}
		// MaxInt32写入归零
		ts := &teststruct{
			A: 555,
			B: 666,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, math.MaxInt32, args)
		if err != nil || reply != "OK" {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v", err)
		}
		// 测试写入的值是否成功
		tmp, cas, err := redisClient.HMGetKeyCas(ctx, key, redigo.Args{"a", "b", "c"})
		replys, err := redigo.Strings(tmp, err)
		if err != nil {
			t.Errorf("Redis.HMGetKeyCas failed, err: %v", err)
		} else {
			// t.Errorf("replys: %v", replys)
			if cas != 0 || len(replys) != 3 || replys[0] != "555" || replys[1] != "666" || replys[2] != "" {
				t.Errorf("Redis.HMGetKeyCas failed, cas: %v, replys: %v, len: %v", cas, replys, len(replys))
			}
		}
	}
	{
		// 测试归零后重新写入
		ts := &teststruct{
			A: 777,
			B: 888,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, 0, args)
		if err != nil {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v", err)
		} else if reply != "OK" {
			t.Errorf("Redis.HMSetKeyCas failed, reply: %v, err: %v", reply, err)
		}
		// 测试写入的值是否成功
		tmp, cas, err := redisClient.HMGetKeyCas(ctx, key, redigo.Args{"a", "b", "c"})
		replys, err := redigo.Strings(tmp, err)
		if err != nil {
			t.Errorf("Redis.HMGetKeyCas failed, err: %v", err)
		} else {
			// t.Errorf("replys: %v", replys)
			if cas != 1 || len(replys) != 3 || replys[0] != "777" || replys[1] != "888" || replys[2] != "" {
				t.Errorf("Redis.HMGetKeyCas failed, cas: %v, replys: %v, len: %v", cas, replys, len(replys))
			}
		}
	}
	{
		// 清除数据
		ret, err := redisClient.Del(ctx, key)
		if ret != 1 || err != nil {
			t.Errorf("Redis.Del failed, err: %v, ret: %v", err, ret)
		}
	}
}

func TestHGetAllKeyCas(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:hgetall:key:cas"
	type teststruct struct {
		A int `redis:"a"`
		B int `redis:"b"`
	}
	{
		// 测试基本功能
		ts := &teststruct{
			A: 111,
			B: 222,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, 0, args)
		if err != nil {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v", err)
		} else if reply != "OK" {
			t.Errorf("Redis.HMSetKeyCas failed, reply: %v, err: %v", reply, err)
		}
		// 测试写入的值是否成功
		tmp, cas, err := redisClient.HGetAllKeyCas(ctx, key)
		replys, err := redigo.Strings(tmp, err)
		if err != nil {
			t.Errorf("Redis.HGetAllKeyCas failed, err: %v", err)
		} else {
			if cas != 1 || len(replys) != 4 || replys[0] != "a" || replys[1] != "111" || replys[2] != "b" ||
				replys[3] != "222" {
				t.Errorf("Redis.HGetAllKeyCas failed, cas: %v, replys: %v, len: %v", cas, replys, len(replys))
			}
		}
	}
	{
		_, err := redisClient.HSet(ctx, key, []string{"_cas"}, []interface{}{math.MaxInt32})
		if err != nil {
			t.Errorf("Redis.HSet failed, err: %v", err)
		}
		// MaxInt32写入归零
		ts := &teststruct{
			A: 555,
			B: 666,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, math.MaxInt32, args)
		if err != nil || reply != "OK" {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v", err)
		}
		// 测试写入的值是否成功
		tmp, cas, err := redisClient.HGetAllKeyCas(ctx, key)
		replys, err := redigo.Strings(tmp, err)
		if err != nil {
			t.Errorf("Redis.HGetAllKeyCas failed, err: %v", err)
		} else {
			if cas != 0 || len(replys) != 4 || replys[0] != "a" || replys[1] != "555" || replys[2] != "b" ||
				replys[3] != "666" {
				t.Errorf("Redis.HGetAllKeyCas failed, cas: %v, replys: %v, len: %v", cas, replys, len(replys))
			}
		}
	}
	{
		// 测试归零后重新写入
		ts := &teststruct{
			A: 777,
			B: 888,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, 0, args)
		if err != nil {
			t.Errorf("Redis.HMSetKeyCas failed, err: %v", err)
		} else if reply != "OK" {
			t.Errorf("Redis.HMSetKeyCas failed, reply: %v, err: %v", reply, err)
		}
		// 测试写入的值是否成功
		tmp, cas, err := redisClient.HGetAllKeyCas(ctx, key)
		replys, err := redigo.Strings(tmp, err)
		if err != nil {
			t.Errorf("Redis.HGetAllKeyCas failed, err: %v", err)
		} else {
			if cas != 1 || len(replys) != 4 || replys[0] != "a" || replys[1] != "777" || replys[2] != "b" ||
				replys[3] != "888" {
				t.Errorf("Redis.HGetAllKeyCas failed, cas: %v, replys: %v, len: %v", cas, replys, len(replys))
			}
		}
	}
	{
		// 清除数据
		ret, err := redisClient.Del(ctx, key)
		if ret != 1 || err != nil {
			t.Errorf("Redis.Del failed, err: %v, ret: %v", err, ret)
		}
	}
}

func TestHDelKeyCas(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:hdel:key:cas"
	type teststruct struct {
		A int `redis:"a"`
		B int `redis:"b"`
	}
	{
		var err error
		// 检查异常参数
		_, err = redisClient.HDelKeyCas(ctx, key, 0, redigo.Args{})
		if err != ErrorParamInvalid {
			t.Errorf("Redis.HDelKeyCas failed, err: %v should be ErrorParamInvalid", err)
		}
	}
	{
		// 测试基本功能
		ts := &teststruct{
			A: 111,
			B: 222,
		}
		args := redigo.Args{}.AddFlat(ts)
		reply, err := redisClient.HMSetKeyCas(ctx, key, 0, args)
		if err != nil {
			t.Errorf("Redis.HDelKeyCas failed, err: %v", err)
		} else if reply != "OK" {
			t.Errorf("Redis.HDelKeyCas failed, reply: %v, err: %v", reply, err)
		}
		// 测试写入的值是否成功
		tmp, cas, err := redisClient.HGetAllKeyCas(ctx, key)
		replys, err := redigo.Strings(tmp, err)
		if err != nil || cas != 1 {
			t.Errorf("Redis.HGetAllKeyCas failed, err: %v, cas: %v", err, cas)
		}
		// t.Errorf("replys: %v", replys)
		for i := 0; i < len(replys); i += 2 {
			if replys[i] != "a" && replys[i] != "b" {
				t.Errorf("field: %v, value: %v", replys[i], replys[i+1])
			}
			if replys[i] == "a" && replys[i+1] != "111" || replys[i] == "b" && replys[i+1] != "222" {
				t.Errorf("Redis.HGetAllKeyCas failed, field: %v, value: %v", replys[i], replys[i+1])
			}
		}
		ret, err := redisClient.HDelKeyCas(ctx, key, cas, redigo.Args{}.Add("a"))
		if err != nil || ret != 1 {
			t.Errorf("Redis.HDelKeyCas failed, err: %v, ret: %v", err, ret)
		}
		// 测试删除的值是否成功
		tmp, cas, err = redisClient.HGetAllKeyCas(ctx, key)
		replys, err = redigo.Strings(tmp, err)
		if err != nil || cas != 2 {
			t.Errorf("Redis.HGetAllKeyCas failed, err: %v, cas: %v", err, cas)
		}
		if len(replys) != 2 || replys[0] != "b" || replys[1] != "222" {
			t.Errorf("Redis.HDelKeyCas failed, replys; %v", replys)
		}
	}
	{
		// 测试cas删除冲突无法成功删除
		ret, err := redisClient.HDelKeyCas(ctx, key, 2333, redigo.Args{}.Add("a", "b"))
		if ret != 0 || err != ErrorCasConflict {
			t.Errorf("Redis.HDelKeyCas failed, err: %v should be ErrorCasConflict, reply: %v should be 0", err, ret)
		}
		tmp, cas, err := redisClient.HGetAllKeyCas(ctx, key)
		replys, err := redigo.Strings(tmp, err)
		if err != nil || cas != 2 {
			t.Errorf("Redis.HGetAllKeyCas failed, err: %v, cas: %v", err, cas)
		}
		if len(replys) != 2 || replys[0] != "b" || replys[1] != "222" {
			t.Errorf("Redis.HDelKeyCas failed, replys; %v", replys)
		}
	}
	{
		_, err := redisClient.HMset(ctx, key, []string{"a", "b", "_cas"}, []interface{}{555, 666, math.MaxInt32})
		if err != nil {
			t.Errorf("Redis.HSet failed, err: %v", err)
		}
		// 测试MaxInt32删除归零
		ret, err := redisClient.HDelKeyCas(ctx, key, math.MaxInt32, redigo.Args{}.Add("a"))
		if err != nil || ret != 1 {
			t.Errorf("Redis.HDelKeyCas failed, err: %v, ret: %v", err, ret)
		}
		tmp, cas, err := redisClient.HGetAllKeyCas(ctx, key)
		replys, err := redigo.Strings(tmp, err)
		if err != nil || cas != 0 {
			t.Errorf("Redis.HGetAllKeyCas failed, err: %v, cas: %v", err, cas)
		}
		if len(replys) != 2 || replys[0] != "b" || replys[1] != "666" {
			t.Errorf("Redis.HDelKeyCas failed, replys; %v", replys)
		}
	}
	{
		// 测试归零后重新删除
		ret, err := redisClient.HDelKeyCas(ctx, key, 0, redigo.Args{}.Add("a", "b"))
		if err != nil {
			t.Errorf("Redis.HDelKeyCas failed, err: %v", err)
		} else if ret != 1 {
			t.Errorf("Redis.HDelKeyCas failed, ret: %v, err: %v", ret, err)
		}
		tmp, cas, err := redisClient.HGetAllKeyCas(ctx, key)
		replys, err := redigo.Strings(tmp, err)
		if err != nil || cas != 1 {
			t.Errorf("Redis.HGetAllKeyCas failed, err: %v, cas: %v", err, cas)
		}
		if len(replys) != 0 {
			t.Errorf("Redis.HDelKeyCas failed, replys; %v", replys)
		}
	}
	{
		// 清除数据
		ret, err := redisClient.Del(ctx, key)
		if ret != 1 || err != nil {
			t.Errorf("Redis.Del failed, err: %v, ret: %v", err, ret)
		}
	}
}

func TestGetBit(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:getbit"
	{
		// 空数据
		val, err := redisClient.GetBit(ctx, key, 2)
		if err != nil || val != 0 {
			t.Errorf("Redis.GetBit failed, err: %v, val: %v", err, val)
		}
	}
	{
		_, err := redisClient.Set(ctx, key, "\x80")
		if err != nil {
			t.Errorf("Redis.Set failed, err: %v", err)
		}
		for i := 1; i < 8; i++ {
			val, err := redisClient.GetBit(ctx, key, i)
			if err != nil || val != 0 {
				t.Errorf("Redis.GetBit failed, err: %v, val: %v, i: %v", err, val, i)
			}
		}
		val, err := redisClient.GetBit(ctx, key, 0)
		if err != nil || val != 1 {
			t.Errorf("Redis.GetBit failed, err: %v, val: %v", err, val)
		}
	}
	{
		_, err := redisClient.Del(ctx, key)
		if err != nil {
			t.Errorf("Redis.Del failed, err: %v", err)
		}
	}
}

func TestSetBit(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:setbit"
	{
		val, err := redisClient.SetBit(ctx, key, 2, 1)
		if err != nil || val != 0 {
			t.Errorf("Redis.SetBit failed, err: %v, val: %v", err, val)
		}
		val, err = redisClient.GetBit(ctx, key, 2)
		if err != nil || val != 1 {
			t.Errorf("Redis.GetBit failed, err: %v, val: %v", err, val)
		}
		val, err = redisClient.SetBit(ctx, key, 2, 0)
		if err != nil || val != 1 {
			t.Errorf("Redis.SetBit failed, err: %v, val: %v", err, val)
		}
		val, err = redisClient.GetBit(ctx, key, 2)
		if err != nil || val != 0 {
			t.Errorf("Redis.GetBit failed, err: %v, val: %v", err, val)
		}
	}
	{
		_, err := redisClient.Del(ctx, key)
		if err != nil {
			t.Errorf("Redis.Del failed, err: %v", err)
		}
	}
}

func TestBatchSetBit(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:batchsetbit"
	{
		err := redisClient.BatchSetBit(ctx, key, 1)
		if err != ErrorParamInvalid {
			t.Errorf("Redis.GetBit failed, err: %v", err)
		}
	}
	{
		err := redisClient.BatchSetBit(ctx, key, 1, 10, 3, "4")
		if err != nil {
			t.Errorf("Redis.BatchSetBit failed, err: %v", err)
		}
		for i := 1; i < 11; i++ {
			val, err := redisClient.GetBit(ctx, key, i)
			if err != nil {
				t.Errorf("Redis.GetBit failed, err: %v, val: %v, i: %v", err, val, i)
			}
			if i == 10 || i == 3 || i == 4 {
				if val != 1 {
					t.Errorf("Redis.GetBit failed, err: %v, val: %v, i: %v", err, val, i)
				}
			} else {
				if val != 0 {
					t.Errorf("Redis.GetBit failed, err: %v, val: %v, i: %v", err, val, i)
				}
			}
		}
	}
	{
		_, err := redisClient.Del(ctx, key)
		if err != nil {
			t.Errorf("Redis.Del failed, err: %v", err)
		}
	}
}

func TestBatchTestBit(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)

	key := "key:test:batchtestbit"
	{
		ret, err := redisClient.BatchTestBit(ctx, key)
		if err != ErrorParamInvalid || ret != 0 {
			t.Errorf("Redis.GetBit failed, err: %v", err)
		}
	}
	{
		err := redisClient.BatchSetBit(ctx, key, 1, 10, 3, "4")
		if err != nil {
			t.Errorf("Redis.BatchSetBit failed, err: %v", err)
		}
		val, err := redisClient.BatchTestBit(ctx, key, 1, 2, 3)
		if err != nil || val != 0 {
			t.Errorf("Redis.BatchTestBit failed, err: %v, val: %v", err, val)
		}
		val, err = redisClient.BatchTestBit(ctx, key, 4)
		if err != nil || val != 1 {
			t.Errorf("Redis.BatchTestBit failed, err: %v, val: %v", err, val)
		}
		val, err = redisClient.BatchTestBit(ctx, key, 3, 4)
		if err != nil || val != 1 {
			t.Errorf("Redis.BatchTestBit failed, err: %v, val: %v", err, val)
		}
		val, err = redisClient.BatchTestBit(ctx, key, 3, 4, 10)
		if err != nil || val != 1 {
			t.Errorf("Redis.BatchTestBit failed, err: %v, val: %v", err, val)
		}
		val, err = redisClient.BatchTestBit(ctx, key, 3, 4, 10, 3, 10, 3, 4)
		if err != nil || val != 1 {
			t.Errorf("Redis.BatchTestBit failed, err: %v, val: %v", err, val)
		}
		val, err = redisClient.BatchTestBit(ctx, key, 2)
		if err != nil || val != 0 {
			t.Errorf("Redis.BatchTestBit failed, err: %v, val: %v", err, val)
		}
		val, err = redisClient.BatchTestBit(ctx, key, 2, 6, 11, 34)
		if err != nil || val != 0 {
			t.Errorf("Redis.BatchTestBit failed, err: %v, val: %v", err, val)
		}
	}
	{
		_, err := redisClient.Del(ctx, key)
		if err != nil {
			t.Errorf("Redis.Del failed, err: %v", err)
		}
	}
}
