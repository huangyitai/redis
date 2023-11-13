package redis

import (
	"context"
	"testing"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	. "github.com/smartystreets/goconvey/convey"
)

// TestMultiLock 测试正常加锁
func TestMultiLock(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "test_mlock"
	curTs := time.Now().Unix()
	Convey("测试加锁", t, func() {
		mlock := &MultiLock{
			Key:        key,
			MemberList: redigo.Args{}.Add(1, 2, 3),
			Lease:      100,
			CurTs:      curTs,
		}
		isLocked, err := redisClient.MLock(ctx, mlock)
		So(err, ShouldEqual, nil)
		So(isLocked, ShouldEqual, true)
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		So(err, ShouldEqual, nil)
		So(len(members), ShouldEqual, 3)
		So(len(scores), ShouldEqual, 3)
		So(members[0], ShouldEqual, "1")
		So(members[1], ShouldEqual, "2")
		So(members[2], ShouldEqual, "3")
		So(scores[0], ShouldEqual, float64(mlock.CurTs+mlock.Lease))
		So(scores[1], ShouldEqual, float64(mlock.CurTs+mlock.Lease))
		So(scores[2], ShouldEqual, float64(mlock.CurTs+mlock.Lease))
	})
	Convey("测试加锁失败", t, func() {
		mlock := &MultiLock{
			Key:        key,
			MemberList: redigo.Args{}.Add(1, 2, 3),
			Lease:      100,
			CurTs:      curTs + 99,
		}
		isLocked, err := redisClient.MLock(ctx, mlock)
		So(err, ShouldEqual, nil)
		So(isLocked, ShouldEqual, false)
	})
	Convey("测试解锁成功", t, func() {
		mlock := &MultiLock{
			Key:        key,
			MemberList: redigo.Args{}.Add(1, 2, 3, 4),
			Lease:      100,
			CurTs:      curTs,
		}
		isLocked, err := redisClient.MUnlock(ctx, mlock)
		So(err, ShouldEqual, nil)
		So(isLocked, ShouldEqual, true)
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		So(err, ShouldEqual, nil)
		So(len(members), ShouldEqual, 0)
		So(len(scores), ShouldEqual, 0)
	})
	Convey("测试解锁成功", t, func() {
		mlock := &MultiLock{
			Key:        key,
			MemberList: redigo.Args{}.Add(1, 2, 3, 4),
			Lease:      100,
			CurTs:      curTs,
		}
		isLocked, err := redisClient.MUnlock(ctx, mlock)
		So(err, ShouldEqual, nil)
		So(isLocked, ShouldEqual, true)
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		So(err, ShouldEqual, nil)
		So(len(members), ShouldEqual, 0)
		So(len(scores), ShouldEqual, 0)
	})
}

// TestMultiLockExist 测试加锁
func TestMultiLockExist(t *testing.T) {
	ctx := context.Background()
	redisClient := newRedisClient(ctx)
	key := "test_mlock_exist"
	curTs := time.Now().Unix()

	mlock := &MultiLock{
		Key:        key,
		MemberList: redigo.Args{}.Add(1),
		Lease:      100,
		CurTs:      curTs,
	}
	Convey("测试加锁", t, func() {
		reply, err := redisClient.ZAdd(ctx, key, []string{"1"}, []float64{float64(curTs)})
		So(err, ShouldEqual, nil)
		So(reply, ShouldEqual, 1)
		isLocked, err := redisClient.MLock(ctx, mlock)
		So(err, ShouldEqual, nil)
		So(isLocked, ShouldEqual, false)
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		So(err, ShouldEqual, nil)
		So(len(members), ShouldEqual, 1)
		So(len(scores), ShouldEqual, 1)
		So(members[0], ShouldEqual, "1")
		So(scores[0], ShouldEqual, float64(mlock.CurTs))
	})
	Convey("测试加锁", t, func() {
		reply, err := redisClient.ZAdd(ctx, key, []string{"1"}, []float64{float64(curTs - 1)})
		So(err, ShouldEqual, nil)
		So(reply, ShouldEqual, 0)
		isLocked, err := redisClient.MLock(ctx, mlock)
		So(err, ShouldEqual, nil)
		So(isLocked, ShouldEqual, true)
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		So(err, ShouldEqual, nil)
		So(len(members), ShouldEqual, 1)
		So(len(scores), ShouldEqual, 1)
		So(members[0], ShouldEqual, "1")
		So(scores[0], ShouldEqual, float64(mlock.CurTs+mlock.Lease))
	})
	Convey("测试解锁", t, func() {
		reply, err := redisClient.ZAdd(ctx, key, []string{"1"}, []float64{float64(curTs + 100)})
		So(err, ShouldEqual, nil)
		So(reply, ShouldEqual, 0)
		isLocked, err := redisClient.MUnlock(ctx, mlock)
		So(err, ShouldEqual, nil)
		So(isLocked, ShouldEqual, true)
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		So(err, ShouldEqual, nil)
		So(len(members), ShouldEqual, 0)
		So(len(scores), ShouldEqual, 0)
	})
	Convey("测试解锁", t, func() {
		reply, err := redisClient.ZAdd(ctx, key, []string{"1"}, []float64{float64(curTs)})
		So(err, ShouldEqual, nil)
		So(reply, ShouldEqual, 1)
		isLocked, err := redisClient.MUnlock(ctx, mlock)
		So(err, ShouldEqual, nil)
		So(isLocked, ShouldEqual, true)
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		So(err, ShouldEqual, nil)
		So(len(members), ShouldEqual, 1)
		So(len(scores), ShouldEqual, 1)
		So(members[0], ShouldEqual, "1")
		So(scores[0], ShouldEqual, float64(mlock.CurTs))
	})
	Convey("测试解锁", t, func() {
		reply, err := redisClient.ZAdd(ctx, key, []string{"1"}, []float64{float64(curTs - 1)})
		So(err, ShouldEqual, nil)
		So(reply, ShouldEqual, 0)
		isLocked, err := redisClient.MUnlock(ctx, mlock)
		So(err, ShouldEqual, nil)
		So(isLocked, ShouldEqual, true)
		members, scores, err := redisClient.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
		So(err, ShouldEqual, nil)
		So(len(members), ShouldEqual, 0)
		So(len(scores), ShouldEqual, 0)
	})
}
