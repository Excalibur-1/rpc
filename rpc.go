package rpc

import (
	"context"
	"fmt"

	"github.com/Excalibur-1/configuration"
	"github.com/Excalibur-1/trace"

	"google.golang.org/grpc"
)

type ServerCfg struct {
	*ServerConfig
	Tag []trace.Tag `json:"tag"`
}

type ClientCfg struct {
	*ClientConfig
	Target string `json:"target"`
}

type Engineer interface {
	Server(namespace, app, group, path string, handlers ...grpc.UnaryServerInterceptor) (*Server, *ServerCfg)
	ClientConn(systemId string, handlers ...grpc.UnaryClientInterceptor) (conn *grpc.ClientConn, cc *ClientCfg)
}

type rpcEngine struct {
	systemId string
	cfg      configuration.Configuration
}

func Engine(systemId string, cfg configuration.Configuration) Engineer {
	fmt.Println("Loading Rpc Engine ver:1.0.0")
	return &rpcEngine{cfg: cfg, systemId: systemId}
}

func (r *rpcEngine) Server(namespace, app, group, path string, handlers ...grpc.UnaryServerInterceptor) (server *Server, sc *ServerCfg) {
	sc = new(ServerCfg)
	if err := r.cfg.Clazz(namespace, app, group, "", path, sc); err == nil {
		server = NewServer(sc.ServerConfig)
		if len(handlers) > 0 {
			server.Use(handlers...)
		}
	} else {
		panic(fmt.Sprintf("从配置中心读取RPC服务器配置:/%s/%s/%s出错\n", app, group, path))
	}
	return
}

func (r *rpcEngine) ClientConn(systemId string, handlers ...grpc.UnaryClientInterceptor) (conn *grpc.ClientConn, cc *ClientCfg) {
	cc = new(ClientCfg)
	if err := r.cfg.Clazz("myconf", "base", "rpc", "", systemId, cc); err == nil {
		if cc.Timeout == 0 || cc.KeepAliveInterval == 0 || cc.KeepAliveTimeout == 0 {
			panic(fmt.Sprintf("Timeout,KeepAliveInterval以及KeepAliveTimeout,必须大于零\n"))
		}
		client := NewClient(cc.ClientConfig)
		if len(handlers) > 0 {
			client.Use(handlers...)
		}
		if conn, err = client.Dial(context.Background(), cc.Target, []string{r.systemId}); err != nil {
			panic(fmt.Sprintf("RPC连接远程服务出错:%+v\n", err))
		}
	} else {
		panic(fmt.Sprintf("从配置中心读取gRpc客户端基础配置出错:%+v\n", err))
	}
	return
}
