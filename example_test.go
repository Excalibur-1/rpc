package rpc_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Excalibur-1/configuration"
	"github.com/rs/zerolog/log"

	"google.golang.org/grpc"

	"github.com/Excalibur-1/rpc"
	"github.com/Excalibur-1/rpc/testproto"
)

const systemId = "10000"

func TestExampleServer(t *testing.T) {
	conf := configuration.DefaultEngine()
	s, _ := rpc.Engine(systemId, conf).Server("myconf", "base", "app", systemId)
	// apply server interceptor middleware
	s.Use(func(ctx context.Context, req interface{}, args *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		_ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		resp, err := handler(_ctx, req)
		return resp, err
	})
	testproto.RegisterGreeterServer(s.Server(), &helloServer{})
	s.Start()
}

func TestExampleClient(t *testing.T) {
	conf := configuration.DefaultEngine()
	conn, _ := rpc.Engine(systemId, conf).ClientConn("1000", func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) (ret error) {
		_ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		ret = invoker(_ctx, method, req, reply, cc, opts...)
		return
	})
	defer conn.Close()

	c := testproto.NewGreeterClient(conn)
	name := "2233"
	rp, err := c.SayHello(context.Background(), &testproto.HelloRequest{Name: name, Age: 18})
	if err != nil {
		log.Error().Err(err).Msg("could not greet")
		return
	}
	fmt.Println("rp", *rp)
}

type helloServer struct {
}

func (s *helloServer) SayHello(ctx context.Context, in *testproto.HelloRequest) (*testproto.HelloReply, error) {
	return &testproto.HelloReply{Message: "Hello " + in.Name, Success: true}, nil
}

func (s *helloServer) StreamHello(ss testproto.Greeter_StreamHelloServer) error {
	for i := 0; i < 3; i++ {
		in, err := ss.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		ret := &testproto.HelloReply{Message: "Hello " + in.Name, Success: true}
		err = ss.Send(ret)
		if err != nil {
			return err
		}
	}
	return nil

}
