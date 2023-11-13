package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	redigo "github.com/gomodule/redigo/redis"
	"trpc.group/trpc-go/trpc-go/log"
)

// RedisHostConfig redis Host配置结构
type RedisHostConfig struct {
	Host        string `toml:"host" json:"host"`
	Port        int    `toml:"port" json:"port"`
	Target      string `toml:"target" json:"target"`
	Timeout     int    `default:"500" toml:"timeout" json:"timeout"`
	Passwd      string `toml:"password" json:"password"`
	PoolIdle    int    `toml:"poolidle" json:"poolidle"`
	PoolOpen    int    `toml:"poolopen" json:"poolopen"`
	IdleTimeout int    `toml:"idletimeout" json:"idletimeout"`
}

// NewRedisWithConf conf
func NewRedisWithConf(ctx context.Context, pRedisConfig *RedisHostConfig, opts ...Option) *Redis {
	target := pRedisConfig.Target
	if target == "" {
		target = fmt.Sprintf("dns://%s:%d", pRedisConfig.Host, pRedisConfig.Port)
	}
	allOpts := []Option{
		WithAddress(target),
		WithPassword(pRedisConfig.Passwd),
		WithConnectTimeout(time.Duration(pRedisConfig.Timeout) * time.Millisecond),
		WithReadTimeout(time.Duration(pRedisConfig.Timeout) * time.Millisecond),
		WithWriteTimeout(time.Duration(pRedisConfig.Timeout) * time.Millisecond),
		WithMaxActive(pRedisConfig.PoolOpen),
		WithMaxIdle(pRedisConfig.PoolIdle),
		WithIdleTimeout(time.Duration(pRedisConfig.IdleTimeout) * time.Second),
	}
	allOpts = append(allOpts, opts...)
	redisCli := NewRedis(context.Background(), allOpts...)
	return redisCli
}

// IfcRedisKey key
type IfcRedisKey interface {
	RedisKey() string
	proto.Message
}

// GetBytesAndUnmarshal GetBytes and unmarshal value
func GetBytesAndUnmarshal(ctx context.Context, red *Redis, value IfcRedisKey) (cas int32, err error) {
	tsBeg := time.Now()

	key := value.RedisKey()

	defer func() {
		cost := time.Since(tsBeg)
		if err != nil {
			log.ErrorContextf(ctx, "GetBytesAndUnmarshal|cost: %v|err: %v|key: %v|value: %+v|cas: %v",
				cost, err, key, value, cas)
		} else {
			log.DebugContextf(ctx, "GetBytesAndUnmarshal|cost: %v|key: %v|value: %+v|cas: %v",
				cost, key, value, cas)
		}
	}()

	var data []byte
	var errTmp error
	data, cas, errTmp = red.GetBytesCas(ctx, key)
	if errTmp != nil {
		err = fmt.Errorf("GetBytes failed: %v", errTmp)
		return
	}

	if len(data) == 0 {
		return
	}

	errTmp = proto.Unmarshal(data, value)
	if errTmp != nil {
		err = fmt.Errorf("proto.Unmarshal failed: %v", errTmp)
		return
	}
	return
}

// MarshalAndSetBytes marshal and SetBytes
func MarshalAndSetBytes(ctx context.Context, red *Redis, value IfcRedisKey, cas int32) (err error) {
	tsBeg := time.Now()

	key := value.RedisKey()

	defer func() {
		cost := time.Since(tsBeg)
		if err != nil {
			log.ErrorContextf(ctx, "MarshalAndSetBytes|cost: %v|err: %v|key: %v|value: %+v|cas: %v",
				cost, err, key, value, cas)
		} else {
			log.DebugContextf(ctx, "MarshalAndSetBytes|cost: %v|err: %v|value: %+v|cas: %v", cost, key, value, cas)
		}
	}()

	if value == nil {
		return
	}

	data, errTmp := proto.Marshal(value)
	if errTmp != nil {
		err = errTmp
		log.ErrorContextf(ctx, "proto.Marshal failed, value: %+v", value)
		return
	}

	errTmp = red.SetBytesCas(ctx, key, data, cas)
	if errTmp != nil {
		err = errTmp
		log.ErrorContextf(ctx, "SetBytes failed: %v", errTmp)
		return
	}

	return
}

// IsRdsNilErr 检测返回是否为nil，防止多库引入导致判断出错
func IsRdsNilErr(err error) bool {
	return err == redigo.ErrNil
}
