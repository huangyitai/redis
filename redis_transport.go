package redis

import (
	"context"
	"net"
	"time"

	"trpc.group/trpc-go/trpc-go/codec"
	"trpc.group/trpc-go/trpc-go/errs"
	"trpc.group/trpc-go/trpc-go/log"
	"trpc.group/trpc-go/trpc-go/transport"
)

func init() {
	transport.RegisterClientTransport("redis_adp", DefaultClientTransport)
}

// ClientTransport 适配trpcclient的redis transport
type ClientTransport struct {
	opts *transport.ClientTransportOptions
}

// DefaultClientTransport 默认
var DefaultClientTransport = NewClientTransport()

// NewClientTransport 创建redis transport
func NewClientTransport(opt ...transport.ClientTransportOption) transport.ClientTransport {

	opts := &transport.ClientTransportOptions{}

	// 将传入的func option写到opts字段中
	for _, o := range opt {
		o(opts)
	}

	return &ClientTransport{
		opts: opts,
	}
}

// RoundTrip redis收发包
func (ct *ClientTransport) RoundTrip(
	ctx context.Context, b []byte, callOpts ...transport.RoundTripOption) (c []byte, err error) {

	msg := codec.Message(ctx)
	req, ok := msg.ClientReqHead().(*Request)
	if !ok {
		return nil, errs.NewFrameError(errs.RetClientEncodeFail,
			"redis client transport: ReqHead should be type of *redis.Request")
	}
	rsp, ok := msg.ClientRspHead().(*Response)
	if !ok {
		return nil, errs.NewFrameError(errs.RetClientEncodeFail,
			"redis client transport: RspHead should be type of *redis.Response")
	}

	opts := &transport.RoundTripOptions{}
	for _, o := range callOpts {
		o(opts)
	}
	begTime := time.Now()
	conn := req.conn
	if conn == nil {
		conn, err = getconn(ctx, req.core)
		if err != nil {
			return nil, errs.NewFrameError(errs.RetClientNetErr, err.Error())
		}
		defer func() {
			_ = conn.RedConn.Close() //skip err
		}()
	}
	log.DebugContextf(ctx, "RoundTrip|cost:%v", time.Since(begTime))
	raddr, _ := net.ResolveTCPAddr("tcp", conn.Ep.Address()) //redis就认为是tcp
	msg.WithRemoteAddr(raddr)

	begin := time.Now()
	defer func() {
		conn.Ep.Report(time.Since(begin), err)
	}()

	switch req.CmdType {
	case RedisDo:
		err = do(ctx, conn.RedConn, req, rsp)
		if err != nil {
			return
		}
	case RedisPipeline:
		err = pipeline(ctx, conn.RedConn, req, rsp)
		if err != nil {
			return
		}
	}

	return
}
