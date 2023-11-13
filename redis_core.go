package redis

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"trpc.group/trpc-go/trpc-go/client"
	"trpc.group/trpc-go/trpc-go/codec"
	"trpc.group/trpc-go/trpc-go/errs"
	"trpc.group/trpc-go/trpc-go/log"
	"trpc.group/trpc-go/trpc-go/naming/selector"
)

// Pool 连接池
type Pool struct {
	pool     map[string]*redigo.Pool
	poolLock sync.RWMutex
}

// NewRedisPool 创建连接池
func NewRedisPool() *Pool {
	redisPool := new(Pool)
	redisPool.pool = make(map[string]*redigo.Pool)
	return redisPool
}

// Core RedisCore
type Core interface {
	Do(ctx context.Context, commandName string, args ...interface{}) (reply interface{}, err error)
	Pipeline(ctx context.Context, args []CommandArgs) (replys []ReplyErr, err error)
	GetConn(ctx context.Context) (conn *Connection, err error)
	Close() error
}

// CommandArgs 命令和参数
type CommandArgs struct {
	Command string
	Args    []interface{}
}

// Request redis invoke请求数据
type Request struct {
	conn      *Connection
	core      Core
	CmdType   int
	CmdArgs   []CommandArgs
	wrTimeout time.Duration
}

// ReplyErr 回包和错误
type ReplyErr struct {
	Reply interface{}
	Err   error
}

// Response redis invoke返回数据
type Response struct {
	Ret       int32
	Msg       string
	ReplyErrs []ReplyErr
}

const (
	// RedisDo do命令
	RedisDo = iota + 1
	// RedisPipeline pipeline命令
	RedisPipeline
)

// doTimeout 从下面中选取一个更早计算超时
// - now + wrTimeout
// - ctx.Deadline()
func doTimeout(ctx context.Context, now time.Time, wrTimeout time.Duration) (timeout time.Duration) {
	timeout = wrTimeout

	d, ok := ctx.Deadline()
	log.TraceContextf(ctx, "deadline:%v, timeout:%s", d, d.Sub(now))
	if ok && !d.IsZero() && d.Before(now.Add(wrTimeout)) {
		timeout = d.Sub(now)
	}
	return
}

func do(ctx context.Context, conn redigo.Conn, req *Request, rsp *Response) (err error) {
	begTime := time.Now()
	//msg := codec.Message(ctx)
	//do之前可能会getconn, 减去其耗时, 再选取一个更早的时间
	timeout := doTimeout(ctx, time.Now(), req.wrTimeout)
	log.TraceContextf(ctx, "doTimeout:%s", timeout)

	//redigo.DoWithTimeout只能控制读超时, connectTimeout和writeTimeout是在dial时指定了
	reply, errTmp := redigo.DoWithTimeout(conn, timeout, req.CmdArgs[0].Command, req.CmdArgs[0].Args...)
	if errTmp != nil {
		if e, ok := errTmp.(net.Error); ok {
			if e.Timeout() {
				return errs.NewFrameError(errs.RetClientTimeout, errTmp.Error())
			}
			if strings.Contains(errTmp.Error(), "connection refused") {
				return errs.NewFrameError(errs.RetClientConnectFail, errTmp.Error())
			}
		}
		//return errs.NewFrameError(errs.RetClientNetErr, err.Error())
	}
	rsp.ReplyErrs = append(rsp.ReplyErrs, ReplyErr{Reply: reply, Err: errTmp})
	log.DebugContextf(ctx, "redisdo|cost:%v", time.Since(begTime))
	//暂时不打日志, 会搞乱文件
	//log.DebugContextf(ctx, "DoWithTimeout err:%v, reply:%+v, req:%+v", errTmp, rsp.ReplyErrs[0], req)

	return
}

func pipeline(ctx context.Context, conn redigo.Conn, req *Request, rsp *Response) (err error) {

	for _, ca := range req.CmdArgs {
		err = conn.Send(ca.Command, ca.Args...)
		if err != nil {
			//Send里面使用bufio， 有4k缓冲区, 写缓冲区不会出错, 但超过会调用flush产生网络IO, 这时候可能出错
			return errs.NewFrameError(errs.RetClientNetErr, err.Error())
		}
	}

	err = conn.Flush()
	if err != nil {
		return errs.NewFrameError(errs.RetClientNetErr, err.Error())
	}

	for i := 0; i < len(req.CmdArgs); i++ {
		reply, replyErr := conn.Receive()
		if netErr, ok := replyErr.(net.Error); ok {
			if netErr.Timeout() {
				return errs.NewFrameError(errs.RetClientTimeout, netErr.Error())
			}
		}
		rsp.ReplyErrs = append(rsp.ReplyErrs, ReplyErr{Reply: reply, Err: replyErr})
	}

	log.TraceContextf(ctx, "Pipeline replys:%+v, req:%+v", rsp.ReplyErrs, req)
	return
}

func getconn(ctx context.Context, core Core) (conn *Connection, err error) {
	conn, err = core.GetConn(ctx)
	if err != nil {
	} else if conn == nil || conn.RedConn == nil {
		err = errs.NewFrameError(errs.RetClientRouteErr, "GetConn conn nil")
	} else if conn.RedConn.Err() != nil {
		err = conn.RedConn.Err()
	}

	if err != nil { //connet或者TestOnBorrow失败
		log.ErrorContextf(ctx, "pool.Get err:%v", err)
	}

	return
}

// PoolCore PoolCore
type PoolCore struct {
	Client     client.Client
	ClientOpts []client.Option
	redisOpts  *Options
	redisPool  *Pool
}

func newPoolCore(opts ...Option) Core {

	core := &PoolCore{
		Client: client.New(),
		redisOpts: &Options{
			ConnectTimeout: time.Second,
			WriteTimeout:   time.Second,
			ReadTimeout:    time.Second,
			MaxIdle:        1000,
			MaxActive:      3000,
			Wait:           true,
			TestOnBorrow: func(c redigo.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
			ClientOpts: []client.Option{
				client.WithTarget("ip://127.0.0.1:5288"),
				//不是用这个地址, 这个是避免client.Invoke的selectNode里的resolve走到dns解析
				client.WithProtocol("redis_adp"),
				client.WithDisableServiceRouter(),
			},
			TrpcInvoke:   true,
			ConnTryTimes: 1,
		},
		redisPool: NewRedisPool(),
	}

	for i := 0; i < len(opts); i++ {
		opts[i](core.redisOpts)
	}

	return core
}

// Do 执行命令
func (c *PoolCore) Do(ctx context.Context, commandName string, args ...interface{}) (reply interface{}, err error) {
	if c.redisOpts.Address == "" {
		err = errs.New(ErrNoAddressingFail, "opts.Address is empty")
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if c.redisOpts.TrpcInvoke {
		replys, et := c.invoke(ctx, nil, commandName, []CommandArgs{{Command: commandName, Args: args}})
		if et != nil {
			return nil, et
		}
		if len(replys) > 0 { //invoke已处理replys为空
			reply = replys[0].Reply
			err = replys[0].Err
		}
	} else {
		conn, et := c.GetConn(ctx)
		if et != nil {
			return nil, et
		}
		begin := time.Now()
		defer func() {
			conn.Ep.Report(time.Since(begin), err)
			_ = conn.RedConn.Close() //skip err
		}()
		reply, err = conn.RedConn.Do(commandName, args...)
	}

	return reply, err
}

// Pipeline 执行批量命令
func (c *PoolCore) Pipeline(ctx context.Context, args []CommandArgs) (replys []ReplyErr, err error) {
	if c.redisOpts.Address == "" {
		err = errs.New(ErrNoAddressingFail, "opts.Address is empty")
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if c.redisOpts.TrpcInvoke {
		replys, err = c.invoke(ctx, nil, "pipeline", args)
	} else {
		return nil, ErrorNotSupport
	}

	return
}

// getNode 获取节点
func getNode(addr string) (Endpoint, error) {
	addrs := strings.Split(addr, "://")
	if len(addrs) != 2 {
		return nil, errs.New(ErrNoAddressingFail, "address invalid, no target")
	}

	switch addrs[0] {
	case "polaris": //这个用新的
		targets := strings.Split(addrs[1], "/") //polaris服务名不允许包含/
		if len(targets) != 2 {
			return nil, errs.New(ErrNoAddressingFail, "address invalid, no namespace")
		}
		s := selector.Get(addrs[0])
		if s == nil {
			return nil, errs.New(ErrNoAddressingFail, fmt.Sprintf("selector %s not exist", addrs[0]))
		}
		node, err := s.Select(targets[1], selector.WithNamespace(targets[0]))
		if err != nil {
			return nil, err
		}
		ep := &trpcEP{
			s: s,
			n: node,
		}
		return ep, nil
	}
	return nil, nil
}

// GetConn 获取redis链接，用于pipeline, conn.Send() ...
func (c *PoolCore) GetConn(ctx context.Context) (conn *Connection, err error) {
	getConn := func(ctx context.Context) (conn *Connection, err error) {
		ep, et := getNode(c.redisOpts.Address)
		if et != nil {
			return nil, et
		}
		key := fmt.Sprintf("%s:%s", ep.Address(), c.redisOpts.Password)

		var ok bool
		var pool *redigo.Pool
		c.redisPool.poolLock.RLock()
		pool, ok = c.redisPool.pool[key]
		c.redisPool.poolLock.RUnlock()

		if ok {
			conn = &Connection{RedConn: pool.Get(), Ep: ep}
			return
		}

		c.redisPool.poolLock.Lock()
		defer c.redisPool.poolLock.Unlock()

		pool, ok = c.redisPool.pool[key]
		if ok {
			conn = &Connection{RedConn: pool.Get(), Ep: ep}
			return
		}

		pool = &redigo.Pool{
			MaxActive:   c.redisOpts.MaxActive,
			MaxIdle:     c.redisOpts.MaxIdle,
			IdleTimeout: c.redisOpts.IdleTimeout,
			Dial: func() (redigo.Conn, error) {
				return redigo.Dial("tcp",
					ep.Address(),
					redigo.DialConnectTimeout(c.redisOpts.ConnectTimeout),
					redigo.DialWriteTimeout(c.redisOpts.WriteTimeout),
					redigo.DialReadTimeout(c.redisOpts.ReadTimeout),
					redigo.DialPassword(c.redisOpts.Password),
				)
			},
			Wait:         c.redisOpts.Wait,
			TestOnBorrow: c.redisOpts.TestOnBorrow,
		}
		c.redisPool.pool[key] = pool

		conn = &Connection{RedConn: pool.Get(), Ep: ep}
		return
	}

	for i := 0; i < c.redisOpts.ConnTryTimes; i++ {
		conn, err = getConn(ctx)
		if err != nil || conn == nil || conn.RedConn == nil {
			log.ErrorContextf(ctx, "GetConn err:%v or conn nil", err)
			continue
		}
		if conn.RedConn.Err() != nil {
			err = conn.RedConn.Err()
			log.ErrorContextf(ctx, "GetConn conn.Err:%v", err)
			continue
		}
		err = nil
		break
	}

	if err != nil {
		log.ErrorContextf(ctx, "GetConn fail finally, err:%v", err)
	}

	return
}

func (c *PoolCore) invoke(ctx context.Context, conn *Connection, commandName string, commandArgs []CommandArgs) (
	replys []ReplyErr, err error) {

	if len(commandArgs) <= 0 {
		return nil, errs.New(ErrNoParamInvaid, "commandArgs empty")
	}

	cmdType := RedisDo
	if commandName == "pipeline" {
		cmdType = RedisPipeline
	}

	req := &Request{
		conn:      conn,
		core:      c,
		CmdType:   cmdType,
		CmdArgs:   commandArgs,
		wrTimeout: c.redisOpts.WriteTimeout + c.redisOpts.ReadTimeout,
	}
	rsp := &Response{}

	ctx, msg := codec.WithCloneMessage(ctx)
	msg.WithClientRPCName(c.redisOpts.Address + "/" + commandName)
	msg.WithCalleeServiceName(c.redisOpts.Address)
	msg.WithSerializationType(-1) //不序列化
	msg.WithCompressType(0)       //不压缩
	msg.WithClientReqHead(req)
	msg.WithClientRspHead(rsp)
	msg.WithCalleeMethod(commandName)

	opts := make([]client.Option, 0, len(c.redisOpts.ClientOpts)+1)
	opts = append(opts, c.redisOpts.ClientOpts...)
	//总超时设置为: 连接超时*连接重试次数 + 写超时 + 读超时
	timeout := c.redisOpts.ConnectTimeout*time.Duration(c.redisOpts.ConnTryTimes) +
		c.redisOpts.WriteTimeout + c.redisOpts.ReadTimeout
	if d, ok := ctx.Deadline(); ok && time.Now().Add(timeout).After(d) {
		timeout = time.Until(d)
		//timeout = d.Sub(time.Now())
	}
	opts = append(opts, client.WithTimeout(timeout))

	err = c.Client.Invoke(ctx, req, rsp, opts...)
	if err != nil {
		log.ErrorContextf(ctx, "Client.Invoke err:%v, req:%+v", err, req)
		return nil, err
	}

	if rsp.Ret != 0 { //mo filter会把err放入ret和msg, 需要判断下
		log.ErrorContextf(ctx, "Client.Invoke ret:%d, msg:%s", rsp.Ret, rsp.Msg)
		return nil, fmt.Errorf("ret:%d, msg:%s", rsp.Ret, rsp.Msg)
	}

	if len(rsp.ReplyErrs) <= 0 {
		return nil, ErrorInvokeRspException
	}

	return rsp.ReplyErrs, nil
}

// Close Close
func (c *PoolCore) Close() error {
	return nil
}

// ConnCore 单连接core, 继承自PoolCore
type ConnCore struct {
	*PoolCore
	Conn *Connection
}

func newConnCore(ctx context.Context, parent Core) (rc Core, err error) {
	core, ok := parent.(*PoolCore)
	if !ok {
		err = errs.New(ErrCoreTypeInvalid, "not poolCore")
		return
	}

	var conn *Connection
	conn, err = core.GetConn(ctx)
	if err != nil {
		return
	}

	rc = &ConnCore{
		PoolCore: core,
		Conn:     conn,
	}

	return
}

// Do Do
func (c *ConnCore) Do(ctx context.Context, commandName string, args ...interface{}) (reply interface{}, err error) {

	log.TraceContextf(ctx, "ConnCore.Do begin...")

	if ctx == nil {
		ctx = context.Background()
	}

	if c.redisOpts.TrpcInvoke {
		replys, errTmp :=
			c.invoke(ctx, c.Conn, commandName, []CommandArgs{{Command: commandName, Args: args}})
		if errTmp != nil {
			return nil, errTmp
		}
		if len(replys) > 0 {
			reply = replys[0].Reply
			err = replys[0].Err
		}
	} else {
		begin := time.Now()
		reply, err = c.Conn.RedConn.Do(commandName, args...)
		c.Conn.Ep.Report(time.Since(begin), err)
	}

	return reply, err
}

// Pipeline Pipeline
func (c *ConnCore) Pipeline(ctx context.Context, args []CommandArgs) (replys []ReplyErr, err error) {

	log.TraceContextf(ctx, "ConnCore.Pipeline begin...")

	if ctx == nil {
		ctx = context.Background()
	}

	if c.redisOpts.TrpcInvoke {
		replys, err = c.invoke(ctx, c.Conn, "pipeline", args)
	} else {
		return nil, ErrorNotSupport
	}

	return
}

// GetConn conn
func (c *ConnCore) GetConn(ctx context.Context) (*Connection, error) {
	return c.Conn, nil
}

// Close Close
func (c *ConnCore) Close() error {
	return c.Conn.RedConn.Close()
}
