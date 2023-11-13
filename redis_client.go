package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"trpc.group/trpc-go/trpc-go/errs"
	"trpc.group/trpc-go/trpc-go/log"
)

const retSuccess = 0
const (
	// ErrNoValueNotValid 数据非法
	ErrNoValueNotValid = iota + 10000
	// ErrNoCasConflict cas冲突
	ErrNoCasConflict
	// ErrNoUnknown 未知错误
	ErrNoUnknown
	// ErrNoParamInvaid 参数不合法
	ErrNoParamInvaid
	// ErrNoAddressingFail 寻址错误
	ErrNoAddressingFail
	// ErrNoRedisReplyInvalid redis返回不符预期
	ErrNoRedisReplyInvalid
	// ErrNoNotSupprt 功能不支持
	ErrNoNotSupprt
	// ErrNoInvokeRspException 调用回包异常
	ErrNoInvokeRspException
	// ErrCoreTypeInvalid rediscore类型不合法
	ErrCoreTypeInvalid
)

// CasField hset用的cas字段名
const (
	CasField = "_cas"
	retOk    = "OK"
)

var (
	// ErrorParamInvalid 参数不合法
	ErrorParamInvalid = errs.New(ErrNoParamInvaid, "param invalid")
	// ErrorAddressingFail 寻址错误
	ErrorAddressingFail = errs.New(ErrNoAddressingFail, "addressing fail")
	// ErrorRedisReplyInvalid redis返回不符预期
	ErrorRedisReplyInvalid = errs.New(ErrNoRedisReplyInvalid, "redis reply invalid")
	// ErrorNotSupport 功能不支持
	ErrorNotSupport = errs.New(ErrNoNotSupprt, "not support")
	// ErrorInvokeRspException 调用回包异常
	ErrorInvokeRspException = errs.New(ErrNoInvokeRspException, "invoke no rsp")
	// ErrorValueNotValid 数据非法
	ErrorValueNotValid = errs.New(ErrNoValueNotValid, "value not valid cas struct")
	// ErrorCasConflict cas冲突
	ErrorCasConflict = errs.New(ErrNoCasConflict, "cas conflict")
	// ErrorUnknown 未知错误
	ErrorUnknown = errs.New(ErrNoUnknown, "unknown error")
)

// Redis RedisClient
type Redis struct {
	core Core
}

// NewRedis 新建RedisClient
//
//	redispool.NewRedis(ctx, []redispool.Option{
//			redispool.WithAddress("dns://cache.xytest1.vip.db:50049"),
//			redispool.WithPassword("rediskbhD87e"),
//		}...)
func NewRedis(ctx context.Context, opts ...Option) *Redis {
	return &Redis{
		core: newPoolCore(opts...),
	}
}

// Command 生成pipeline命令结果
func Command(command string, args ...interface{}) CommandArgs {
	return CommandArgs{Command: command, Args: args}
}

// WithRedisConn 创建一个单连接的Redis, 用于连接覆盖的场景
func (c *Redis) WithRedisConn(ctx context.Context) (redis *Redis, err error) {

	var cc Core
	cc, err = newConnCore(ctx, c.core)
	if err != nil {
		return
	}

	redis = &Redis{
		core: cc,
	}

	return
}

// Close Close
func (c *Redis) Close() error {
	return c.core.Close()
}

// Do 执行单条指令
func (c *Redis) Do(ctx context.Context, commandName string, args ...interface{}) (reply interface{}, err error) {
	return c.core.Do(ctx, commandName, args...)
}

// Pipeline 执行批量命令
func (c *Redis) Pipeline(ctx context.Context, args []CommandArgs) (replys []ReplyErr, err error) {
	return c.core.Pipeline(ctx, args)
}

/*
// GetConn 获取redigo的conn
func (c *Redis) GetConn(ctx context.Context) (addressing *goingclient.Addressing, conn redigo.Conn, err error) {
	cn, et := c.core.GetConn(ctx)
	if et != nil {
		return
	}
	conn = cn.RedConn
	return
}
*/

func (c *Redis) get(ctx context.Context, key string) (reply interface{}, err error) {
	reply, err = c.core.Do(ctx, "GET", key)
	return
}

func (c *Redis) scriptDo(ctx context.Context, s *Script, keysAndArgs ...interface{}) (interface{}, error) {
	v, err := c.core.Do(ctx, "EVALSHA", s.Args(s.Hash, keysAndArgs)...)
	if e, ok := err.(redigo.Error); ok && strings.HasPrefix(string(e), "NOSCRIPT ") {
		v, err = c.core.Do(ctx, "EVAL", s.Args(s.Src, keysAndArgs)...)
	}
	return v, err
}

// RunScript 执行脚本
func (c *Redis) RunScript(ctx context.Context, script *Script, keysAndArgs ...interface{}) (interface{}, error) {
	return c.scriptDo(ctx, script, keysAndArgs...)
}

// GetBytes get命令封装, 返回的value为[]byte类型
// 在拉取成功情况下, reply为value, err为nil
// 在拉取失败情况下, 如果是key不存在, 则reply为空, err为redigo.ErrNil
func (c *Redis) GetBytes(ctx context.Context, key string) (value []byte, err error) {
	value, err = redigo.Bytes(c.get(ctx, key))
	return
}

// GetBytesCas 获取[]byte类型value(带cas)
func (c *Redis) GetBytesCas(ctx context.Context, key string) (val []byte, cas int32, err error) {
	reply, errTmp := redigo.Bytes(c.get(ctx, key))
	// 注意空值返回0
	if errTmp == redigo.ErrNil {
		return
	} else if errTmp != nil {
		err = errTmp
		return
	}

	val, cas, err = Unpack(reply)
	// 如果是负数, 需要0才能写入
	return
}

// GetString get命令封装, 返回的value为string类型
// 在拉取成功情况下, reply为value, err为nil
// 在拉取失败情况下, 如果是key不存在, 则reply为空, err为redigo.ErrNil
func (c *Redis) GetString(ctx context.Context, key string) (value string, err error) {
	value, err = redigo.String(c.get(ctx, key))
	return
}

// GetStringCas 获取string类型value(带cas)
func (c *Redis) GetStringCas(ctx context.Context, key string) (val string, cas int32, err error) {
	var valBytes []byte
	valBytes, cas, err = c.GetBytesCas(ctx, key)
	if err == nil {
		val = string(valBytes)
	}
	return
}

// Del del命令封装, 返回删除成功的key数量
func (c *Redis) Del(ctx context.Context, keys ...string) (ret int64, err error) {
	if len(keys) == 0 {
		err = errs.New(ErrNoParamInvaid, "keys empty")
		return
	}
	args := make([]interface{}, 0, len(keys))
	for i := 0; i < len(keys); i++ {
		args = append(args, keys[i])
	}
	ret, err = redigo.Int64(c.core.Do(ctx, "DEL", args...))
	return
}

// Set set命令封装
// 在设置成功情况下, reply为OK, err为nil
// 在设置失败情况下, 如果是NX/XX类失败, 则reply为空, err为redigo.ErrNil
// 通过SetOption支持EX(秒)/PX(毫秒)过期时间和NX/XX能力
// 示例:
//
//	redis.Set(ctx, key, value, WithSetEX(1), WithSetNX())
//	redis.Set(ctx, key, value, WithSetPX(100), WithSetXX())
func (c *Redis) Set(ctx context.Context, key string, value interface{}, opts ...SetOption) (
	reply string, err error) {
	setOptions := new(SetOptions)
	for i := 0; i < len(opts); i++ {
		opts[i](setOptions)
	}

	args := []interface{}{key, value}

	switch setOptions.ExpireType {
	case ExpireTypeEX:
		args = append(args, "EX", setOptions.TTL)
	case ExpireTypePX:
		args = append(args, "PX", setOptions.TTL)
	}
	switch setOptions.ExistType {
	case ExistTypeNX:
		args = append(args, "NX")
	case ExistTypeXX:
		args = append(args, "XX")
	}

	// 这里有可能返回 (nil) / "OK"
	reply, err = redigo.String(c.core.Do(ctx, "SET", args...))
	return
}

// SetStringCas 带cas的set string命令（仅redis ssd版本）
func (c *Redis) SetStringCas(ctx context.Context, key, val string, cas int32) (reply string, err error) {
	reply, err = redigo.String(c.core.Do(ctx, "CAS", key, cas, val))
	return
}

// SetBytesCas 带cas的set命令
// Set的cas小于0不用cas
func (c *Redis) SetBytesCas(
	ctx context.Context, key string, data []byte, cas int32, args ...interface{}) (err error) {
	var val []byte
	if cas < 0 {
		cas = -1
		val = Pack(data, 0)
	} else {
		val = Pack(data, cas)
	}

	script := NewScript(1, SetScriptKeyValueV2)
	ss := make([]interface{}, 0)
	ss = append(ss, key)
	ss = append(ss, string(val))
	ss = append(ss, cas)
	ss = append(ss, args...)
	ret, errTmp := redigo.Int(c.scriptDo(ctx, script, ss...))
	if errTmp != nil {
		err = errTmp
		return
	}
	switch ret {
	case ErrNoValueNotValid:
		err = ErrorValueNotValid
	case ErrNoCasConflict:
		err = ErrorCasConflict
	case retSuccess:
		err = nil
	default:
		err = errs.New(ErrNoUnknown, fmt.Sprintf("ret:%d", ret))
	}
	return
}

// DelCas del命令封装, 带cas的del命令
func (c *Redis) DelCas(ctx context.Context, key string, cas int32) (ret int64, err error) {
	if key == "" {
		err = errs.New(ErrNoParamInvaid, "key empty")
		return
	}
	script := NewScript(1, DelScriptCas)
	ss := make([]interface{}, 0)
	ss = append(ss, key)
	ss = append(ss, cas)

	if ret, err = redigo.Int64(c.scriptDo(ctx, script, ss...)); err != nil {
		return
	}

	switch ret {
	case ErrNoValueNotValid:
		err = ErrorValueNotValid
	case ErrNoCasConflict:
		err = ErrorCasConflict
	case retSuccess:
		err = nil
	default:
		err = errs.New(ErrNoUnknown, fmt.Sprintf("ret:%d", ret))
	}
	return
}

func (c *Redis) zAdd(ctx context.Context, key string, members []string, scores []float64, opts ...ZAddOption) (
	reply interface{}, err error) {
	if len(scores) == 0 || len(scores) != len(members) {
		return 0, errs.New(ErrNoParamInvaid, "member empty or scores and members length not equal")
	}
	zAddOptions := new(ZAddOptions)
	for i := 0; i < len(opts); i++ {
		opts[i](zAddOptions)
	}

	args := make([]interface{}, 0, 2*len(scores)+4)
	args = append(args, key)
	switch zAddOptions.ExistType {
	case ExistTypeNX:
		args = append(args, "NX")
	case ExistTypeXX:
		args = append(args, "XX")
	}
	if zAddOptions.ChOn {
		args = append(args, "CH")
	}
	if zAddOptions.IncrOn {
		args = append(args, "INCR")
	}

	for i := 0; i < len(scores); i++ {
		args = append(args, scores[i])
		args = append(args, members[i])
	}

	reply, err = c.core.Do(ctx, "ZADD", args...)
	return
}

// ZAdd zadd命令封装
// 此函数不用于zadd incr, zadd incr请使用ZAddIncr
// 在设置成功情况下,
//
//	如果不设置ch, reply为key中原来不存在的member数量, err为nil
//	如果设置ch, reply为key中原来不存在的member数量加上改变了原有值的member数量, err为nil
//
// 在设置失败情况下, 如果是NX/XX类失败, 则reply为0, err为redigo.ErrNil
// 通过ZAddOption支持ch, nx/xx
// 示例:
//
//	redis.ZAdd(ctx, key, members, scores, WithZAddXX(), WithZAddCH())
//	redis.ZAdd(ctx, key, members, scores, WithZAddNX())
func (c *Redis) ZAdd(ctx context.Context, key string, members []string, scores []float64, opts ...ZAddOption) (
	reply int64, err error) {
	return redigo.Int64(c.zAdd(ctx, key, members, scores, opts...))
}

// ZAddIncr zadd命令封装
// 此函数专用于zadd incr
// 之所以分开两个函数, 主要原因有两个, 一是不想让调用方处理返回值两种情况, 二是incr只支持同时操作一个member
// 如果选择incr参数, 在设置成功情况下, 返回的是指定member的新分值, 是一个浮点数
// 如果设置失败, 且是NX/XX类失败, 则reply为0, err为redigo.ErrNil
// 示例:
//
//	redis.ZAddIncr(ctx, key, member, score, WithZAddCH(), WithZAddNX())
//	redis.ZAddIncr(ctx, key, member, score, WithZAddXX(), WithZAddCH())
func (c *Redis) ZAddIncr(ctx context.Context, key string, member string, score float64, opts ...ZAddOption) (
	reply float64, err error) {
	opts = append(opts, WithZAddIncr())
	return redigo.Float64(c.zAdd(ctx, key, []string{member}, []float64{score}, opts...))
}

// ZCount zcount命令封装
// min, max传入string的原因是, 不止支持浮点数, redis实现默认是闭区间, 可以使用小括号来使用开区间
// 示例:
//
//	redis.ZCount(ctx, key, "1", "(3")
//	redis.ZCount(ctx, key, "-inf", "3")
func (c *Redis) ZCount(ctx context.Context, key string, min string, max string) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "ZCOUNT", key, min, max))
	return
}

// ZRank zrank命令封装
// 如果成功返回member在zset中排位, 从0开始
// 如果member不存在, 或者key不存在, ret为0, err为redigo.ErrNil
func (c *Redis) ZRank(ctx context.Context, key string, member string) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "ZRANK", key, member))
	return
}

// ZCard zcard命令封装
// 如果成功返回key中member数量, 没有key或者空zset返回0
func (c *Redis) ZCard(ctx context.Context, key string) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "ZCARD", key))
	return
}

// ZRem zrem命令封装
// 如果成功返回删除的member数量, 没有key或没删掉东西返回0
func (c *Redis) ZRem(ctx context.Context, key string, members ...string) (ret int64, err error) {
	if len(members) == 0 {
		err = errs.New(ErrNoParamInvaid, "members empty")
		return
	}
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	for i := 0; i < len(members); i++ {
		args = append(args, members[i])
	}
	ret, err = redigo.Int64(c.core.Do(ctx, "ZREM", args...))
	return
}

// DoWithRangeReply 处理range的响应包
func DoWithRangeReply(reply []string, withscores bool) (members []string, scores []float64, err error) {
	if withscores {
		if len(reply)%2 != 0 {
			err = ErrorRedisReplyInvalid
			return
		}
		members = make([]string, 0, len(reply)/2)
		scores = make([]float64, 0, len(reply)/2)
		for i := 0; i < len(reply); i += 2 {
			members = append(members, reply[i])
			score, errTmp := strconv.ParseFloat(reply[i+1], 64)
			if errTmp != nil {
				err = ErrorRedisReplyInvalid
				members = members[:0]
				scores = scores[:0]
				return
			}
			scores = append(scores, score)
		}
	} else {
		members = make([]string, 0, len(reply))
		for i := 0; i < len(reply); i++ {
			members = append(members, reply[i])
		}
	}
	return
}

func (c *Redis) zRange(
	ctx context.Context, cmd string, key string, start int64, stop int64, opts ...ZRangeOption) (
	members []string, scores []float64, err error) {
	zrangeOptions := new(ZRangeOptions)
	for i := 0; i < len(opts); i++ {
		opts[i](zrangeOptions)
	}

	args := []interface{}{key, start, stop}
	if zrangeOptions.WithScores {
		args = append(args, "WITHSCORES")
	}
	var reply []string
	begTime := time.Now()
	replyTmp, errTmp := c.core.Do(ctx, cmd, args...)
	log.DebugContextf(ctx, "DoWithRangeReply|coredo cost:%v", time.Since(begTime))

	reply, err = redigo.Strings(replyTmp, errTmp)
	log.DebugContextf(ctx, "DoWithRangeReply|strings cost:%v", time.Since(begTime))

	if err != nil {
		return
	}
	members, scores, err = DoWithRangeReply(reply, zrangeOptions.WithScores)
	log.DebugContextf(ctx, "DoWithRangeReply|cost:%v", time.Since(begTime))
	return
}

// ZRange zrange命令封装
// 通过ZRangeOption指定WITHSCORES
// 示例:
//
//	redis.ZRange(ctx, key, 0, -1)
//	redis.ZRange(ctx, key, 0, -1, WithZRangeWithScores())
func (c *Redis) ZRange(ctx context.Context, key string, start int64, stop int64, opts ...ZRangeOption) (
	members []string, scores []float64, err error) {
	return c.zRange(ctx, "ZRANGE", key, start, stop, opts...)
}

// ZRevRange zrevrange命令封装
// 通过ZRangeOption指定WITHSCORES
// 示例:
//
//	redis.ZRevRange(ctx, key, 0, -1)
//	redis.ZRevRange(ctx, key, 0, -1, WithZRangeWithScores())
func (c *Redis) ZRevRange(
	ctx context.Context, key string, start int64, stop int64, opts ...ZRangeOption) (
	members []string, scores []float64, err error) {
	return c.zRange(ctx, "ZREVRANGE", key, start, stop, opts...)
}

func (c *Redis) zrangeByScore(
	ctx context.Context, cmd string, key string, min string, max string, opts ...ZRangeByScoreOption) (
	members []string, scores []float64, err error) {
	args := []interface{}{key, min, max}
	zrangeByScoreOptions := new(ZRangeByScoreOptions)
	for i := 0; i < len(opts); i++ {
		opts[i](zrangeByScoreOptions)
	}

	if zrangeByScoreOptions.WithScores {
		args = append(args, "WITHSCORES")
	}
	if zrangeByScoreOptions.LimitOn {
		args = append(args, "LIMIT", zrangeByScoreOptions.LimitOffset, zrangeByScoreOptions.LimitCount)
	}

	var reply []string
	reply, err = redigo.Strings(c.core.Do(ctx, cmd, args...))
	if err != nil {
		return
	}

	members, scores, err = DoWithRangeReply(reply, zrangeByScoreOptions.WithScores)
	return
}

// ZRangeByScore zrangebyscore命令封装
// 通过ZRangeByScoreOption指定WITHSCORES, LIMIT
// min, max传入string的原因是, 不止支持浮点数, redis实现默认是闭区间, 可以使用小括号来使用开区间
// 示例:
//
//	redis.ZRangeByScore(ctx, key, "1", "(3")
//	redis.ZRangeByScore(ctx, key, "-inf", "+inf", WithZRangeByScoreLimitOffset(1),
//
// WithZRangeByScoreLimitCount(2), WithZRangeByScoreWithScores())
func (c *Redis) ZRangeByScore(
	ctx context.Context, key string, min string, max string, opts ...ZRangeByScoreOption) (
	members []string, scores []float64, err error) {
	return c.zrangeByScore(ctx, "ZRANGEBYSCORE", key, min, max, opts...)
}

// ZRevRangeByScore zrevrangebyscore命令封装
// 通过ZRangeByScoreOption指定WITHSCORES, LIMIT
// min, max传入string的原因是, 不止支持浮点数, redis实现默认是闭区间, 可以使用小括号来使用开区间
// 示例:
//
//	redis.ZRangeByScore(ctx, key, "1", "(3")
//	redis.ZRangeByScore(ctx, key, "-inf", "+inf", WithZRangeByScoreLimitOffset(1),
//
// WithZRangeByScoreLimitCount(2), WithZRangeByScoreWithScores())
func (c *Redis) ZRevRangeByScore(
	ctx context.Context, key string, min string, max string, opts ...ZRangeByScoreOption) (
	members []string, scores []float64, err error) {
	return c.zrangeByScore(ctx, "ZREVRANGEBYSCORE", key, max, min, opts...)
}

// ZScore zscore命令封装
// key或者member不存在, score为0, err为redigo.ErrNil
// 存在则score为对应member分值, err为nil
func (c *Redis) ZScore(ctx context.Context, key string, member string) (score float64, err error) {
	score, err = redigo.Float64(c.core.Do(ctx, "ZSCORE", key, member))
	return
}

// ZIncrBy zincrby命令封装
// score返回member对应的新分值
func (c *Redis) ZIncrBy(ctx context.Context, key string, member string, incr float64) (ret float64, err error) {
	ret, err = redigo.Float64(c.core.Do(ctx, "ZINCRBY", key, incr, member))
	return
}

// ZRemRangeByRank zremrangebyrank命令封装
// 排名从0开始, ret返回删除成功的member数量
func (c *Redis) ZRemRangeByRank(ctx context.Context, key string, start int64, stop int64) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "ZREMRANGEBYRANK", key, start, stop))
	return
}

// ZRemRangeByScore zremrangebyscore命令封装
// ret返回删除成功的member数量
// min, max传入string的原因是, 不止支持浮点数, redis实现默认是闭区间, 可以使用小括号来使用开区间
// 示例:
//
//	redis.ZRemRangeByScore(ctx, key, "(1", "(3")
func (c *Redis) ZRemRangeByScore(ctx context.Context, key string, min string, max string) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "ZREMRANGEBYSCORE", key, min, max))
	return
}

// Mget mget命令封装
func (c *Redis) mget(ctx context.Context, keys ...string) (values interface{}, err error) {
	if len(keys) == 0 {
		err = errs.New(ErrNoParamInvaid, "keys empty")
		return
	}
	args := make([]interface{}, 0, len(keys))
	for i := 0; i < len(keys); i++ {
		args = append(args, keys[i])
	}
	values, err = c.core.Do(ctx, "MGET", args...)
	return
}

// MgetString mget命令封装, 返回的value为string
func (c *Redis) MgetString(ctx context.Context, keys ...string) (values []string, err error) {
	values, err = redigo.Strings(c.mget(ctx, keys...))
	return
}

// MgetBytes mget命令封装, 返回的value为[]byte
func (c *Redis) MgetBytes(ctx context.Context, keys ...string) (values [][]byte, err error) {
	values, err = redigo.ByteSlices(c.mget(ctx, keys...))
	return
}

// Mset mset命令封装
// 返回OK...
func (c *Redis) Mset(ctx context.Context, keys []string, values []interface{}) (reply string, err error) {
	if len(keys) != len(values) || len(keys) == 0 {
		err = errs.New(ErrNoParamInvaid, "keys empty or keys and values not match")
		return
	}
	args := make([]interface{}, 0, len(keys)*2)
	for i := 0; i < len(keys); i++ {
		args = append(args, keys[i])
		args = append(args, values[i])
	}
	reply, err = redigo.String(c.core.Do(ctx, "MSET", args...))
	return
}

// Incr incr命令封装
// 返回值ret为key加1后的value
func (c *Redis) Incr(ctx context.Context, key string) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "INCR", key))
	return
}

// Decr decr命令封装
// 返回值ret为key减1后的value
func (c *Redis) Decr(ctx context.Context, key string) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "DECR", key))
	return
}

// IncrBy incrby命令封装
// 返回值ret为key加delta后的value
func (c *Redis) IncrBy(ctx context.Context, key string, delta int64) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "INCRBY", key, delta))
	return
}

// DecrBy decrby命令封装
// 返回值ret为key减delta后的value
func (c *Redis) DecrBy(ctx context.Context, key string, delta int64) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "DECRBY", key, delta))
	return
}

// Exists exists命令封装
// 返回值为存在的key数量, 注意这里没去重, 例如存在a, b两个key
// Exists(ctx, "a", "b", "a", "c", "d"), 返回值为3
func (c *Redis) Exists(ctx context.Context, keys ...string) (ret int64, err error) {
	if len(keys) == 0 {
		err = errs.New(ErrNoParamInvaid, "keys empty")
		return
	}
	args := make([]interface{}, 0, len(keys))
	for i := 0; i < len(keys); i++ {
		args = append(args, keys[i])
	}
	ret, err = redigo.Int64(c.core.Do(ctx, "EXISTS", args...))
	return
}

// Expire expire命令封装
// 返回值: 1说明设置成功, 0说明key不存在
func (c *Redis) Expire(ctx context.Context, key string, seconds int64) (ret int, err error) {
	ret, err = redigo.Int(c.core.Do(ctx, "EXPIRE", key, seconds))
	return
}

// ExpireAt expireat命令封装
// 返回值: 1说明设置成功, 0说明key不存在
func (c *Redis) ExpireAt(ctx context.Context, key string, timestamp int64) (ret int, err error) {
	ret, err = redigo.Int(c.core.Do(ctx, "EXPIREAT", key, timestamp))
	return
}

// LPush lpush命令封装
// 返回值: 列表的最终长度
func (c *Redis) LPush(ctx context.Context, key string, values ...interface{}) (length int64, err error) {
	if len(values) == 0 {
		err = errs.New(ErrNoParamInvaid, "values empty")
		return
	}
	args := make([]interface{}, 0, len(values)+1)
	args = append(args, key)
	args = append(args, values...)
	length, err = redigo.Int64(c.core.Do(ctx, "LPUSH", args...))
	return
}

// RPush rpush命令封装
// 返回值: 列表的最终长度
func (c *Redis) RPush(ctx context.Context, key string, values ...interface{}) (length int64, err error) {
	if len(values) == 0 {
		err = errs.New(ErrNoParamInvaid, "values empty")
		return
	}
	args := make([]interface{}, 0, len(values)+1)
	args = append(args, key)
	args = append(args, values...)
	length, err = redigo.Int64(c.core.Do(ctx, "RPUSH", args...))
	return
}

// LPop lpop命令封装
// 返回值: 如果非空, 弹出并返回列表最左边item; 如果空返回ErrNil
func (c *Redis) LPop(ctx context.Context, key string) (reply string, err error) {
	reply, err = redigo.String(c.core.Do(ctx, "LPOP", key))
	return
}

// RPop rpop命令封装
// 返回值: 如果非空, 弹出并返回列表最右边item; 如果空返回ErrNil
func (c *Redis) RPop(ctx context.Context, key string) (reply string, err error) {
	reply, err = redigo.String(c.core.Do(ctx, "RPOP", key))
	return
}

// LRange lrange命令封装
// start从0开始, end可以用负数, -1表示最后一个元素
// 返回值是[start, end]闭区间的item列表
func (c *Redis) LRange(ctx context.Context, key string, start int64, stop int64) (values []string, err error) {
	values, err = redigo.Strings(c.core.Do(ctx, "LRANGE", key, start, stop))
	return
}

// LRem lrem命令封装
// 删除列表中值为value的item
// count > 0 从前往后删 count 个
// count < 0 从后往前删 -count 个
// count = 0 全删
// 返回值为删除成功的数量
func (c *Redis) LRem(ctx context.Context, key string, count int64, value interface{}) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "LREM", key, count, value))
	return
}

// LLen llen命令封装
// 返回列表长度
func (c *Redis) LLen(ctx context.Context, key string) (length int64, err error) {
	length, err = redigo.Int64(c.core.Do(ctx, "LLEN", key))
	return
}

// LIndex lindex命令封装
// 返回指定下标的item
// 从0开始从前往后, 从-1开始从后往前
func (c *Redis) LIndex(ctx context.Context, key string, index int64) (value string, err error) {
	value, err = redigo.String(c.core.Do(ctx, "LINDEX", key, index))
	return
}

// SAdd sadd命令封装
// 返回最后集合的item数量
func (c *Redis) SAdd(ctx context.Context, key string, members ...interface{}) (size int64, err error) {
	if len(members) == 0 {
		err = errs.New(ErrNoParamInvaid, "members empty")
		return
	}
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	args = append(args, members...)
	size, err = redigo.Int64(c.core.Do(ctx, "SADD", args...))
	return
}

// SCard scard命令封装
// 返回最终集合中item数量
func (c *Redis) SCard(ctx context.Context, key string) (size int64, err error) {
	size, err = redigo.Int64(c.core.Do(ctx, "SCARD", key))
	return
}

// SIsMember sismember命令封装
// 返回值: member在集合中返回1, 不在返回0
func (c *Redis) SIsMember(ctx context.Context, key string, member interface{}) (ret int, err error) {
	ret, err = redigo.Int(c.core.Do(ctx, "SISMEMBER", key, member))
	return
}

// SMembers smembers命令封装
// 返回集合中member列表
func (c *Redis) SMembers(ctx context.Context, key string) (members []string, err error) {
	members, err = redigo.Strings(c.core.Do(ctx, "SMEMBERS", key))
	return
}

// SRem srem命令封装
// 返回真正删除的集合member数量
func (c *Redis) SRem(ctx context.Context, key string, members ...interface{}) (ret int, err error) {
	if len(members) == 0 {
		err = errs.New(ErrNoParamInvaid, "members empty")
		return
	}
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	for i := 0; i < len(members); i++ {
		args = append(args, members[i])
	}
	ret, err = redigo.Int(c.core.Do(ctx, "SREM", args...))
	return
}

func generateHSetArgs(key string, fields []string, values []interface{}) (args []interface{}, err error) {
	if len(fields) != len(values) || len(fields) == 0 {
		err = errs.New(ErrNoParamInvaid, "fields empty or fields and values not match")
		return
	}
	args = make([]interface{}, 0, len(fields)+len(values)+1)
	args = append(args, key)
	for i := 0; i < len(fields); i++ {
		args = append(args, fields[i])
		args = append(args, values[i])
	}
	return
}

// Hlen hlen命令封装
// 返回hash的field数量
func (c *Redis) Hlen(ctx context.Context, key string) (ret uint64, err error) {
	return redigo.Uint64(c.core.Do(ctx, "HLEN", key))
}

// HSet hset命令封装
// 返回新增的field数量
// 注意: 这里在4.0.0版本后才支持多field/value, 低于该版本多field/value请使用HMSet
func (c *Redis) HSet(ctx context.Context, key string, fields []string, values []interface{}) (
	ret int64, err error) {
	var args []interface{}
	args, err = generateHSetArgs(key, fields, values)
	if err != nil {
		return
	}
	ret, err = redigo.Int64(c.core.Do(ctx, "HSET", args...))
	return
}

// HSetCas 带cas的hset命令封装
func (c *Redis) HSetCas(ctx context.Context, key, field, data string, cas int32) (err error) {
	s := NewScript(1, HSetScript)
	ret, errTmp := redigo.Int(c.scriptDo(ctx, s, key, field, data, cas))
	if errTmp != nil {
		err = errTmp
		return
	}
	switch ret {
	case ErrNoValueNotValid:
		err = ErrorValueNotValid
	case ErrNoCasConflict:
		err = ErrorCasConflict
	case retSuccess:
		err = nil
	default:
		err = errs.New(ErrNoUnknown, fmt.Sprintf("ret:%d", ret))
	}
	return
}

// HDelCas 带cas的hdel命令封装, 只支持单个field
func (c *Redis) HDelCas(ctx context.Context, key, field string, cas int32) (err error) {
	s := NewScript(1, HDelScript)
	ret, errTmp := redigo.Int(c.scriptDo(ctx, s, key, field, cas))
	if errTmp != nil {
		err = errTmp
		return
	}
	switch ret {
	case ErrNoValueNotValid:
		err = ErrorValueNotValid
	case ErrNoCasConflict:
		err = ErrorCasConflict
	case retSuccess:
		err = nil
	default:
		err = errs.New(ErrNoUnknown, fmt.Sprintf("ret:%d", ret))
	}
	return
}

// HSetAndGet 设置并返回(在脚本里)
func (c *Redis) HSetAndGet(ctx context.Context, key, field, data string, cas int32) (
	fieldVal string, newcas int32, err error) {
	s := NewScript(1, HSetAndGetScript)
	reply, errTmp := redigo.Values(c.scriptDo(ctx, s, key, field, data, cas))
	if errTmp != nil {
		err = errTmp
		return
	}

	var ret int32
	rawVal := make([]byte, 0)

	if len(reply) == 1 {
		_, err = redigo.Scan(reply, &ret)
	} else {
		_, err = redigo.Scan(reply, &ret, &rawVal)
	}
	if err != nil {
		return
	}

	switch ret {
	case ErrNoValueNotValid:
		err = ErrorValueNotValid
	case ErrNoCasConflict:
		err = ErrorCasConflict
	case retSuccess:
		err = nil
	default:
		err = errs.New(ErrNoUnknown, fmt.Sprintf("ret:%d", ret))
	}
	if err != nil {
		return
	}

	var fieldByte []byte
	fieldByte, newcas, err = Unpack(rawVal)

	if err == nil {
		fieldVal = string(fieldByte)
	}

	return
}

// HGet hget命令封装
// 返回指定key的field对应的value
func (c *Redis) HGet(ctx context.Context, key string, field string) (reply string, err error) {
	reply, err = redigo.String(c.core.Do(ctx, "HGET", key, field))
	return
}

// HGetCas 带cas的hget命令封装
func (c *Redis) HGetCas(ctx context.Context, key, field string) (reply string, cas int32, err error) {
	replyByte, errTmp := redigo.Bytes(c.core.Do(ctx, "HGET", key, field))
	// 注意空值返回0
	if errTmp == redigo.ErrNil {
		return
	} else if errTmp != nil {
		err = errTmp
		return
	}

	var valByte []byte
	valByte, cas, err = Unpack(replyByte)
	// 如果是负数, 需要0才能写入
	if err == nil {
		reply = string(valByte)
	}
	return
}

// HGetAll hgetall命令封装
// 返回指定key的所有fields和value
func (c *Redis) HGetAll(ctx context.Context, key string) (fields []string, values []string, err error) {
	var reply []string
	reply, err = redigo.Strings(c.core.Do(ctx, "HGETALL", key))
	if len(reply)%2 == 1 {
		err = ErrorRedisReplyInvalid
		return
	}
	length := len(reply) / 2
	fields = make([]string, 0, length)
	values = make([]string, 0, length)
	for i := 0; i < len(reply); i += 2 {
		fields = append(fields, reply[i])
		values = append(values, reply[i+1])
	}
	return
}

// HGetAllRaw hgetall命令封装
// 返回interface
func (c *Redis) HGetAllRaw(ctx context.Context, key string) (reply interface{}, err error) {
	reply, err = c.core.Do(ctx, "HGETALL", key)
	return
}

// HKeys HKEYS 命令封装
// 返回指定 key 的所有 fields
func (c *Redis) HKeys(ctx context.Context, key string) (fields []string, err error) {
	reply, err := redigo.Strings(c.HKeysRaw(ctx, key))
	return reply, err
}

// HKeysRaw HKEYS 命令封装
// 返回指定 key 的所有 fields
func (c *Redis) HKeysRaw(ctx context.Context, key string) (fields interface{}, err error) {
	reply, err := c.core.Do(ctx, "HKEYS", key)
	return reply, err
}

// Hexists hexists 命令封装
// 如果key下存在field， 则返回1, 否则返回0
// Hexists(ctx, "a"), 返回值为1
func (c *Redis) Hexists(ctx context.Context, key, field string) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "HEXISTS", key, field))
	return
}

// HSetNX hsetnx 命令封装
// 返回值, 为1说明设置成功, 为0说明没设置成功
func (c *Redis) HSetNX(ctx context.Context, key string, field string, value interface{}) (ret int, err error) {
	ret, err = redigo.Int(c.core.Do(ctx, "HSETNX", key, field, value))
	return
}

// HMset hmset 命令封装
// 在hset支持多field/value后会废弃
// 返回值为OK...
func (c *Redis) HMset(
	ctx context.Context, key string, fields []string, values []interface{}) (reply string, err error) {
	var args []interface{}
	args, err = generateHSetArgs(key, fields, values)
	if err != nil {
		return
	}
	reply, err = redigo.String(c.core.Do(ctx, "HMSET", args...))
	return
}

// HMget hmget命令封装
// 返回对应field列表的value列表, 不存在为空串
func (c *Redis) HMget(ctx context.Context, key string, fields ...string) (values []string, err error) {
	if len(fields) == 0 {
		err = errs.New(ErrNoParamInvaid, "fields empty")
		return
	}
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for i := 0; i < len(fields); i++ {
		args = append(args, fields[i])
	}
	values, err = redigo.Strings(c.core.Do(ctx, "HMGET", args...))
	return
}

// HDel hDel 命令封装
// 删除指定的field
func (c *Redis) HDel(ctx context.Context, key string, fields ...string) (rm int, err error) {
	if len(fields) == 0 {
		err = errs.New(ErrNoParamInvaid, "fields empty")
		return
	}
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for i := 0; i < len(fields); i++ {
		args = append(args, fields[i])
	}
	rm, err = redigo.Int(c.core.Do(ctx, "HDEL", args...))
	return
}

// HScan hscan命令封装
// 初次传入游标为0, 返回下一次搜索的游标, 及本次迭代的field/value列表
// 下一次游标为0说明搜索完成
// 可以通过WithHScanPattern支持MATCH, WithHScanCount支持COUNT
// 举例:
//
//	c.HScan(ctx, key, 0, WithHScanPattern("a*"), WithHScanCount(10))
func (c *Redis) HScan(ctx context.Context, key string, cursor int64, opts ...HScanOption) (
	cursorNext int64, fields []string, values []string, err error) {
	hscanOptions := new(HScanOptions)
	for i := 0; i < len(opts); i++ {
		opts[i](hscanOptions)
	}

	args := []interface{}{key, cursor}
	if hscanOptions.PatternOn {
		args = append(args, "MATCH", hscanOptions.Pattern)
	}
	if hscanOptions.CountOn {
		args = append(args, "COUNT", hscanOptions.Count)
	}
	var reply []interface{}
	reply, err = redigo.Values(c.core.Do(ctx, "HSCAN", args...))
	if err != nil {
		return
	}
	var data []string
	_, err = redigo.Scan(reply, &cursorNext, &data)
	if err != nil {
		return
	}

	if len(data)%2 == 1 {
		err = ErrorRedisReplyInvalid
		return
	}
	fields = make([]string, 0, len(data)/2)
	values = make([]string, 0, len(data)/2)
	for i := 0; i < len(data); i += 2 {
		fields = append(fields, data[i])
		values = append(values, data[i+1])
	}
	return
}

// HIncrBy HINCRBY命令封装
// score返回field对应的新分值
func (c *Redis) HIncrBy(ctx context.Context, key string, field string, incr int64) (ret int64, err error) {
	ret, err = redigo.Int64(c.core.Do(ctx, "HINCRBY", key, field, incr))
	return
}

// SScan sscan命令封装
// 初次传入游标为0, 返回下一次搜索的游标, 及本次迭代的field列表
// 下一次游标为0说明搜索完成
// 可以通过WithHScanPattern支持MATCH, WithHScanCount支持COUNT
// 举例:
//
//	c.SScan(ctx, key, 0, WithHScanPattern("a*"), WithHScanCount(10))
func (c *Redis) SScan(ctx context.Context, key string, cursor int64, opts ...HScanOption) (
	cursorNext int64, fields []string, err error) {
	hscanOptions := new(HScanOptions)
	for i := 0; i < len(opts); i++ {
		opts[i](hscanOptions)
	}

	args := []interface{}{key, cursor}
	if hscanOptions.PatternOn {
		args = append(args, "MATCH", hscanOptions.Pattern)
	}
	if hscanOptions.CountOn {
		args = append(args, "COUNT", hscanOptions.Count)
	}
	var reply []interface{}
	reply, err = redigo.Values(c.core.Do(ctx, "SSCAN", args...))
	if err != nil {
		return
	}
	var data []string
	_, err = redigo.Scan(reply, &cursorNext, &data)
	if err != nil {
		return
	}

	fields = make([]string, 0, len(data))
	for i := 0; i < len(data); i++ {
		fields = append(fields, data[i])
	}
	return
}

// HMSetKeyCas 按key做cas的hmset
// 示例
//
//	redis.HMSetKeyCas(ctx, key, cas, redigo.Args{}.AddFlat(postOrm))
func (c *Redis) HMSetKeyCas(ctx context.Context, key string, cas int32, args redigo.Args) (reply string, err error) {
	if len(args) == 0 {
		err = errs.New(ErrNoParamInvaid, "args empty")
		return
	}
	if len(args)%2 != 0 {
		err = errs.New(ErrNoParamInvaid, "len(args) not even")
		return
	}
	script := NewScript(1, HMSetWithKeyCas)
	keysAndArgs := redigo.Args{}.Add(key).Add(cas).Add(args...)
	reply, err = redigo.String(c.scriptDo(ctx, script, keysAndArgs...))
	if err != nil {
		return
	}
	if reply == strconv.FormatInt(ErrNoCasConflict, 10) {
		reply = "" // 清空
		err = ErrorCasConflict
		return
	}
	return
}

// HMGetKeyCas 按key做cas的hmget
// 示例
//
//	reply, cas, err := redis.HMSetKeyCas(ctx, key, cas, []interface{}{"a", "b", "c"})
//	replyStrs, err := redigo.String(reply, err)
func (c *Redis) HMGetKeyCas(ctx context.Context, key string, args redigo.Args) (reply interface{}, cas int32,
	err error) {
	script := NewScript(1, HMGetWithKeyCas)
	keysAndArgs := redigo.Args{}.Add(key).Add(args...)
	replys, err := redigo.Values(c.scriptDo(ctx, script, keysAndArgs...))
	if err != nil {
		return
	}
	if len(replys) != len(args)+1 {
		err = ErrorRedisReplyInvalid
		return
	}
	casUint8, ok := replys[0].([]uint8)
	if !ok {
		err = ErrorValueNotValid
		return
	}
	casStr := uint8Slice2String(casUint8)
	casInt64, errTmp := strconv.ParseInt(casStr, 10, 64)
	if errTmp != nil {
		err = ErrorValueNotValid
		return
	}
	// 去掉cas, c.scriptDo 返回的就是 []interface{}, 直接切片即可
	cas, reply = int32(casInt64), replys[1:]
	return
}

// HGetAllKeyCas 按key做cas的hgetall
// 示例
//
//	redis.HMSetKeyCas(ctx, key)
//	replyStrs, err := redigo.String(reply, err)
func (c *Redis) HGetAllKeyCas(ctx context.Context, key string) (reply interface{}, cas int32, err error) {
	replys, errTmp := redigo.Values(c.HGetAllRaw(ctx, key))
	if errTmp != nil {
		err = errTmp
		return
	}
	if len(replys)%2 != 0 {
		err = ErrorRedisReplyInvalid
		return
	}
	// 如果没有cas字段, 默认是0
	for i := 0; i < len(replys); i += 2 {
		fieldUint8, okField := replys[i].([]uint8)
		valueUint8, okValue := replys[i+1].([]uint8)
		if !okField || !okValue {
			err = ErrorValueNotValid
			return
		}
		field := uint8Slice2String(fieldUint8)
		if field == CasField {
			casInt64, errTmp := strconv.ParseInt(uint8Slice2String(valueUint8), 10, 64)
			if errTmp != nil {
				err = ErrorValueNotValid
				return
			}
			// 去掉cas
			cas, reply = int32(casInt64), append(replys[:i], replys[i+2:]...)
			break
		}
	}
	return
}

// HDelKeyCas 按key做cas的hdel
// 示例
//
//	redis.HMSetKeyCas(ctx, key, cas, []interface{}{"a", "b", "c"})
func (c *Redis) HDelKeyCas(ctx context.Context, key string, cas int32, args redigo.Args) (ret int64, err error) {
	if len(args) == 0 {
		err = errs.New(ErrNoParamInvaid, "args empty")
		return
	}
	keysAndArgs := redigo.Args{}.Add(key).Add(cas).Add(args...)
	script := NewScript(1, HDelWithKeyCas)
	ret, err = redigo.Int64(c.scriptDo(ctx, script, keysAndArgs...))
	if err != nil {
		return
	}
	if ret == -ErrNoCasConflict {
		ret = 0 // 清空
		err = ErrorCasConflict
		return
	}
	return
}

// SRandMember srandmember命令封装
// 返回集合中随机member
// 示例
//
//	uid, err := redigo.Uint64(redis.SRandMember(ctx, key)) // 获取单个
//	uids, err := redigo.Strings(redis.SRandMember(ctx, key, WithSRandMemberCount(10)))
func (c *Redis) SRandMember(ctx context.Context, key string, opts ...SRandMemberOption) (reply interface{},
	err error) {
	options := new(SRandMemberOptions)
	for _, opt := range opts {
		opt(options)
	}
	args := make([]interface{}, 0, 2)
	args = append(args, key)
	if options.Count != 0 {
		args = append(args, options.Count)
	}
	reply, err = c.core.Do(ctx, "SRANDMEMBER", args...)
	return
}

// GetBit getbit命令封装
// 返回指定key指定offset的值, 返回0或者1, offset从0开始, 如果offset超范围或者key为空返回0
// 示例
//
//	val, err := redisClient.GetBit(ctx, key, 2) // 获取位置为2的比特值
func (c *Redis) GetBit(ctx context.Context, key string, offset interface{}) (bitVal int, err error) {
	bitVal, err = redigo.Int(c.core.Do(ctx, "GETBIT", key, offset))
	return
}

// SetBit setbit命令封装
// 设置key指定offset的值, 返回offset所在位置原始比特值, offset从0开始, 如果offset超范围或者key为空, 自动扩展长度并返回0
// 示例
//
//	val, err := redisClient.SetBit(ctx, key, 2, 0)	// 设置位置为2的比特值为0
//	val, err := redisClient.SetBit(ctx, key, 2, 1)	// 设置位置为2的比特值为1
func (c *Redis) SetBit(ctx context.Context, key string, offset interface{}, val interface{}) (bitVal int, err error) {
	bitVal, err = redigo.Int(c.core.Do(ctx, "SETBIT", key, offset, val))
	return
}

// BatchSetBit 批量设置比特位
// 设置key指定offset列表所在比特位的值, val只能为0或者1
// 示例
//
//	err := redisClient.BatchSetBit(ctx, key, 1, 1, "2", 3)	    // 设置位置1,2,3的比特值为1
//	err := redisClient.BatchSetBit(ctx, key, "0", 1, "2", 3)	    // 设置位置1,2,3的比特值为0
func (c *Redis) BatchSetBit(ctx context.Context, key string, val interface{}, offsets ...interface{}) (err error) {
	if len(offsets) == 0 {
		err = ErrorParamInvalid
		return
	}
	script := NewScript(1, BatchSetBit)
	args := make(redigo.Args, 0, len(offsets)+2)
	args = args.Add(key, val).Add(offsets...)
	_, err = c.scriptDo(ctx, script, args...)
	return
}

// BatchTestBit 批量检查比特位
// 检查key指定offset列表所在比特位的值, 如果全为1, 返回1, 否则返回0
// 示例
//
//	val, err := redisClient.BatchTestBit(ctx, key, 4, "2", 3)	    // 检查位置2,3,4的比特值是否均为1
func (c *Redis) BatchTestBit(ctx context.Context, key string, offsets ...interface{}) (ret int, err error) {
	if len(offsets) == 0 {
		err = ErrorParamInvalid
		return
	}
	script := NewScript(1, BatchTestBit)
	args := make(redigo.Args, 0, len(offsets)+1)
	args = args.Add(key).Add(offsets...)
	ret, err = redigo.Int(c.scriptDo(ctx, script, args...))
	return
}
