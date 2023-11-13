package redis

import (
	"context"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey"
	redigo "github.com/gomodule/redigo/redis"
	//. "github.com/smartystreets/goconvey/convey"
)

func TestRedisDo(t *testing.T) {
	red := NewRedis(context.Background(), []Option{
		WithAddress("ip://127.0.0.1:6379"),
		WithConnectTimeout(time.Duration(100) * time.Millisecond),
		WithWriteTimeout(time.Duration(100) * time.Millisecond),
		WithReadTimeout(time.Duration(100) * time.Millisecond),
		//WithPassword("redisFh1GJTS"),
		WithConnTryTimes(3),
	}...)

	pDWT := ApplyFunc(redigo.DoWithTimeout,
		func(c redigo.Conn, timeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
			//time.Sleep(time.Duration(3000) * time.Millisecond)
			time.Sleep(timeout)
			return nil, nil
		},
	)
	defer pDWT.Reset()

	ctx, cf := context.WithTimeout(context.Background(), time.Duration(140)*time.Millisecond)
	defer cf()
	//time.Sleep(time.Duration(100) * time.Millisecond)
	tsBeg := time.Now()
	_, err := red.RunScript(ctx, NewScript(1, "get KEYS[1]"), "key")
	t.Logf("err:%v, cost:%v", err, time.Since(tsBeg))
}
