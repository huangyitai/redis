package redis

import (
	"trpc.group/trpc-go/trpc-go/codec"
)

func init() {
	codec.Register("redis_adp", nil, DefaultClientCodec)
}

var (
	// DefaultClientCodec redis的codec
	DefaultClientCodec = &ClientCodec{}
)

// ClientCodec 解码redis client请求
type ClientCodec struct{}

// Encode 设置redis client请求的元数据
func (c *ClientCodec) Encode(msg codec.Msg, a []byte) (b []byte, err error) {

	/*
		//自身
		if msg.CallerServiceName() == "" {
			msg.WithCallerApp("redis")
			msg.WithCallerServer(path.Base(os.Args[0]))
			msg.WithCallerService("service")
			msg.WithCallerServiceName(fmt.Sprintf("trpc.redis.%s.service", msg.CallerServer()))
		}

		if msg.CallerServer() == "" {
			callers := strings.Split(msg.CallerServiceName(), ".") // trpc.app.server.service
			if len(callers) > 3 {
				msg.WithCallerApp(callers[1])
				msg.WithCallerServer(callers[2])
				msg.WithCallerService(callers[3])
			}
		}

		//下游
		callee := strings.Split(msg.CalleeServiceName(), ".") // trpc.app.server.service
		if len(callee) > 3 {
			msg.WithCalleeApp(callee[1])
			msg.WithCalleeServer(callee[2])
			msg.WithCalleeService(callee[3])
		}
	*/

	return nil, nil
}

// Decode 解析redis client回包里的元数据
func (c *ClientCodec) Decode(msg codec.Msg, b []byte) (by []byte, err error) {

	return nil, nil
}
