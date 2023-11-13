package redis

import (
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"trpc.group/trpc-go/trpc-go/client"
)

// Option option
type Option func(*Options)

// Options option
type Options struct {
	Address        string
	ConnectTimeout time.Duration
	WriteTimeout   time.Duration
	ReadTimeout    time.Duration
	Password       string
	MaxIdle        int
	MaxActive      int
	IdleTimeout    time.Duration
	Wait           bool
	TestOnBorrow   func(c redigo.Conn, t time.Time) error
	ConnTryTimes   int
	TrpcInvoke     bool            //使用trpc client invoke模式
	ClientOpts     []client.Option //走trpc client时的参数
}

// WithAddress 后端统一寻址API：
//
//	ip://ip:port            ip:port 主要用于测试环境
//	l5://modid:cmdid        cgo封装的l5寻址组件
//	cl5://modid:cmdid:key   CstHash l5 一致性哈希有状态路由
//	gl5://modid:cmdid       golang l5 纯go实现的l5
//	nl5://l5domainname      name l5 纯go实现的域名解析l5
//	cmlb://appid            cgo封装的cmlb寻址组件 sysid默认为1
//	cmlb://sysid:appid      cgo封装的cmlb寻址组件
//	cmlbtest://appid        cgo封装的cmlb寻址组件 测试环境
//	dns://id.qq.com:80      域名解析寻址
//	sock:///tmp/aa.sock     本机unix通信sock文件寻址
//	ons://zkname            OMG名字服务
func WithAddress(address string) Option {
	return func(redisOptions *Options) {
		redisOptions.Address = address
	}
}

// WithConnectTimeout WithConnectTimeout
func WithConnectTimeout(connectTimeout time.Duration) Option {
	return func(redisOptions *Options) {
		redisOptions.ConnectTimeout = connectTimeout
	}
}

// WithWriteTimeout WithWriteTimeout
func WithWriteTimeout(writeTimeout time.Duration) Option {
	return func(redisOptions *Options) {
		redisOptions.WriteTimeout = writeTimeout
	}
}

// WithReadTimeout WithReadTimeout
func WithReadTimeout(readTimeout time.Duration) Option {
	return func(redisOptions *Options) {
		redisOptions.ReadTimeout = readTimeout
	}
}

// WithPassword WithPassword
func WithPassword(password string) Option {
	return func(redisOptions *Options) {
		redisOptions.Password = password
	}
}

// WithMaxIdle WithMaxIdle
func WithMaxIdle(maxIdle int) Option {
	return func(redisOptions *Options) {
		redisOptions.MaxIdle = maxIdle
	}
}

// WithMaxActive WithMaxActive
func WithMaxActive(maxActive int) Option {
	return func(redisOptions *Options) {
		redisOptions.MaxActive = maxActive
	}
}

// WithIdleTimeout WithIdleTimeout
func WithIdleTimeout(idleTimeout time.Duration) Option {
	return func(redisOptions *Options) {
		redisOptions.IdleTimeout = idleTimeout
	}
}

// WithWait WithWait
func WithWait(wait bool) Option {
	return func(redisOptions *Options) {
		redisOptions.Wait = wait
	}
}

// WithTestOnBorrow WithTestOnBorrow
func WithTestOnBorrow(testOnBorrow func(c redigo.Conn, t time.Time) error) Option {
	return func(redisOptions *Options) {
		redisOptions.TestOnBorrow = testOnBorrow
	}
}

// WithConnTryTimes WithConnTryTimes
func WithConnTryTimes(times int) Option {
	return func(redisOptions *Options) {
		if times < 1 {
			times = 1
		}
		redisOptions.ConnTryTimes = times
	}
}

// WithTrpcInvoke WithTrpcInvoke
func WithTrpcInvoke(trpcInvoke bool) Option {
	return func(redisOptions *Options) {
		redisOptions.TrpcInvoke = trpcInvoke
	}
}

// WithTrpcInvokeOption WithTrpcInvokeOption
func WithTrpcInvokeOption(opts ...client.Option) Option {
	return func(redisOptions *Options) {
		redisOptions.ClientOpts = append(redisOptions.ClientOpts, opts...)
	}
}

// SetOption SetOption
type SetOption func(*SetOptions)

const (
	// ExpireTypeNone 超时类型默认值
	ExpireTypeNone = 0
	// ExpireTypeEX  秒级超时
	ExpireTypeEX = 1
	// ExpireTypePX 毫秒级超时
	ExpireTypePX = 2
)

const (
	// ExistTypeNone 默认值
	ExistTypeNone = 0
	// ExistTypeNX 只在键不存在时， 才对键进行设置操作
	ExistTypeNX = 1
	// ExistTypeXX 只在键已经存在时， 才对键进行设置操作
	ExistTypeXX = 2
)

// SetOptions SetOptions
type SetOptions struct {
	TTL        int64
	ExpireType int
	ExistType  int
}

// WithSetEX WithSetEX
func WithSetEX(ttl int64) SetOption {
	return func(setOptions *SetOptions) {
		setOptions.TTL = ttl
		setOptions.ExpireType = ExpireTypeEX
	}
}

// WithSetPX WithSetPX
func WithSetPX(ttl int64) SetOption {
	return func(setOptions *SetOptions) {
		setOptions.TTL = ttl
		setOptions.ExpireType = ExpireTypePX
	}
}

// WithSetNX WithSetNX
func WithSetNX() SetOption {
	return func(setOptions *SetOptions) {
		setOptions.ExistType = ExistTypeNX
	}
}

// WithSetXX WithSetXX
func WithSetXX() SetOption {
	return func(setOptions *SetOptions) {
		setOptions.ExistType = ExistTypeXX
	}
}

// ZAddOptions ZAddOptions
type ZAddOptions struct {
	ExistType int
	ChOn      bool
	IncrOn    bool
}

// ZAddOption ZAddOption
type ZAddOption func(*ZAddOptions)

// WithZAddNX WithZAddNX
func WithZAddNX() ZAddOption {
	return func(zaddOptions *ZAddOptions) {
		zaddOptions.ExistType = ExistTypeNX
	}
}

// WithZAddXX WithZAddXX
func WithZAddXX() ZAddOption {
	return func(zaddOptions *ZAddOptions) {
		zaddOptions.ExistType = ExistTypeXX
	}
}

// WithZAddIncr WithZAddIncr
func WithZAddIncr() ZAddOption {
	return func(zaddOptions *ZAddOptions) {
		zaddOptions.IncrOn = true
	}
}

// WithZAddCH WithZAddCH
func WithZAddCH() ZAddOption {
	return func(zaddOptions *ZAddOptions) {
		zaddOptions.ChOn = true
	}
}

// ZRangeOptions ZRangeOptions
type ZRangeOptions struct {
	WithScores bool
}

// ZRangeOption ZRangeOption
type ZRangeOption func(*ZRangeOptions)

// WithZRangeWithScores WithZRangeWithScores
func WithZRangeWithScores() ZRangeOption {
	return func(zrangeOptions *ZRangeOptions) {
		zrangeOptions.WithScores = true
	}
}

// ZRangeByScoreOptions ZRangeByScoreOptions
type ZRangeByScoreOptions struct {
	WithScores  bool
	LimitOn     bool
	LimitOffset int64
	LimitCount  int64
}

// ZRangeByScoreOption ZRangeByScoreOption
type ZRangeByScoreOption func(*ZRangeByScoreOptions)

// WithZRangeByScoreWithScores WithZRangeByScoreWithScores
func WithZRangeByScoreWithScores() ZRangeByScoreOption {
	return func(zrangeByScoreOptions *ZRangeByScoreOptions) {
		zrangeByScoreOptions.WithScores = true
	}
}

// WithZRangeByScoreLimitOffset WithZRangeByScoreLimitOffset
func WithZRangeByScoreLimitOffset(offset int64) ZRangeByScoreOption {
	return func(zrangeByScoreOptions *ZRangeByScoreOptions) {
		zrangeByScoreOptions.LimitOn = true
		zrangeByScoreOptions.LimitOffset = offset
	}
}

// WithZRangeByScoreLimitCount WithZRangeByScoreLimitCount
func WithZRangeByScoreLimitCount(count int64) ZRangeByScoreOption {
	return func(zrangeByScoreOptions *ZRangeByScoreOptions) {
		zrangeByScoreOptions.LimitOn = true
		zrangeByScoreOptions.LimitCount = count
	}
}

// HScanOptions HScanOptions
type HScanOptions struct {
	Pattern   string
	Count     int64
	PatternOn bool
	CountOn   bool
}

// HScanOption HScanOption
type HScanOption func(*HScanOptions)

// WithHScanPattern WithHScanPattern
func WithHScanPattern(pattern string) HScanOption {
	return func(hscanOptions *HScanOptions) {
		hscanOptions.Pattern = pattern
		hscanOptions.PatternOn = true
	}
}

// WithHScanCount WithHScanCount
func WithHScanCount(count int64) HScanOption {
	return func(hscanOptions *HScanOptions) {
		hscanOptions.Count = count
		hscanOptions.CountOn = true
	}
}

// SRandMemberOptions SRandMemberOptions
type SRandMemberOptions struct {
	Count int64
}

// SRandMemberOption SRandMemberOption
type SRandMemberOption func(*SRandMemberOptions)

// WithSRandMemberCount 指定 SRandMember 的数量
func WithSRandMemberCount(count int64) SRandMemberOption {
	return func(opts *SRandMemberOptions) {
		opts.Count = count
	}
}
