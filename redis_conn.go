package redis

import (
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"trpc.group/trpc-go/trpc-go/naming/registry"
	"trpc.group/trpc-go/trpc-go/naming/selector"
)

// Endpoint redis节点
type Endpoint interface {
	Address() string
	Report(cost time.Duration, err error)
}

type trpcEP struct {
	s selector.Selector
	n *registry.Node
}

func (e *trpcEP) Address() string {
	if e == nil || e.n == nil {
		return ""
	}
	return e.n.Address
}

func (e *trpcEP) Report(cost time.Duration, err error) {
	if e == nil || e.s == nil || e.n == nil {
		return
	}
	_ = e.s.Report(e.n, cost, err)
}

// Connection 连接
type Connection struct {
	RedConn redigo.Conn
	Ep      Endpoint
}
