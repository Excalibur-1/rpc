package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	nmd "github.com/Excalibur-1/code"
	"github.com/Excalibur-1/gutil"
	pb "github.com/Excalibur-1/rpc/testproto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

const testAddr = "127.0.0.1:9090"

var (
	outPut        []string
	testOnce      sync.Once
	server        *Server
	clientConfig  = ClientConfig{Dial: gutil.Duration(time.Second * 10), Timeout: gutil.Duration(time.Second * 10), EnableLog: true}
	clientConfig2 = ClientConfig{Dial: gutil.Duration(time.Second * 10), Timeout: gutil.Duration(time.Second * 10), EnableLog: true,
		Method: map[string]*ClientConfig{`/testproto.Greeter/SayHello`: {Timeout: gutil.Duration(time.Millisecond * 200)}},
	}
)

func Test_Rpc(t *testing.T) {
	go testOnce.Do(runServer(t))
	go runClient(context.Background(), &clientConfig, t, "test_rpc", 0)
	testValidation(t)
	testServerRecovery(t)
	testClientRecovery(t)
	testTimeoutOpt(t)
	testErrorDetail(t)
	testMetaCodeStatus(t)
	testRemotePort(t)
	testLinkTimeout(t)
	testClientConfig(t)
	testAllErrorCase(t)
	testInterceptorChain(t)
	testGracefulShutDown(t)
}

func BenchmarkServer(b *testing.B) {
	server := NewServer(&ServerConfig{Network: "tcp", Addr: testAddr, Timeout: gutil.Duration(time.Second)})
	go func() {
		pb.RegisterGreeterServer(server.Server(), &helloServer{})
		if _, err := server.Start(); err != nil {
			os.Exit(0)
			return
		}
	}()
	defer func() {
		server.Server().Stop()
	}()
	client := NewClient(&clientConfig)
	conn, err := client.Dial(context.Background(), testAddr, []string{"10000"})
	if err != nil {
		conn.Close()
		b.Fatalf("did not connect: %v", err)
	}
	b.ResetTimer()
	b.RunParallel(func(parab *testing.PB) {
		for parab.Next() {
			c := pb.NewGreeterClient(conn)
			resp, err := c.SayHello(context.Background(), &pb.HelloRequest{Name: "benchmark_test", Age: 1})
			if err != nil {
				conn.Close()
				b.Fatalf("c.SayHello failed: %v,req: %v %v", err, "benchmark", 1)
			}
			if !resp.Success {
				b.Error("response not success!")
			}
		}
	})
	conn.Close()
}

func TestMetadata(t *testing.T) {
	cli, cancel := NewTestServerClient(func(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
		assert.Equal(t, "2.2.3.3", nmd.ToString(ctx, nmd.RemoteIP))
		assert.Equal(t, "2233", nmd.ToString(ctx, nmd.RemotePort))
		return &pb.HelloReply{}, nil
	}, &ServerConfig{
		Network:           "tcp",
		Addr:              "0.0.0.0:9000",
		Timeout:           gutil.Duration(time.Second),
		IdleTimeout:       gutil.Duration(time.Second * 180),
		MaxLifeTime:       gutil.Duration(time.Hour * 2),
		ForceCloseWait:    gutil.Duration(time.Second * 20),
		KeepAliveInterval: gutil.Duration(time.Second * 60),
		KeepAliveTimeout:  gutil.Duration(time.Second * 20),
		EnableLog:         true,
	}, &ClientConfig{
		Dial:              gutil.Duration(time.Second * 10),
		Timeout:           gutil.Duration(time.Millisecond * 250),
		KeepAliveInterval: gutil.Duration(time.Second * 60),
		KeepAliveTimeout:  gutil.Duration(time.Second * 20),
		EnableLog:         true,
	})
	defer cancel()

	ctx := nmd.NewContext(context.Background(), nmd.Metadata{
		nmd.RemoteIP:   "2.2.3.3",
		nmd.RemotePort: "2233",
	})
	_, err := cli.SayHello(ctx, &pb.HelloRequest{Name: "test"})
	assert.Nil(t, err)
}

func TestStartWithAddr(t *testing.T) {
	configuredAddr := "127.0.0.1:0"
	server = NewServer(&ServerConfig{Network: "tcp", Addr: configuredAddr, Timeout: gutil.Duration(time.Second), EnableLog: true})
	if _, realAddr, err := server.StartWithAddr(); err == nil && realAddr != nil {
		assert.NotEqual(t, realAddr.String(), configuredAddr)
	} else {
		assert.NotNil(t, realAddr)
		assert.Nil(t, err)
	}
}

type testServer struct {
	helloFn func(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error)
}

func (t *testServer) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
	return t.helloFn(ctx, req)
}

func (t *testServer) StreamHello(pb.Greeter_StreamHelloServer) error { panic("not implemented") }

type helloServer struct {
	t *testing.T
}

func (s *helloServer) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	if in.Name == "test_rpc" {
		if in.Age == 0 {
			runClient(ctx, &clientConfig, s.t, "test_rpc", 1)
		}
	} else if in.Name == "recovery_test" {
		panic("test recovery")
	} else if in.Name == "graceful_shutdown" {
		time.Sleep(time.Second * 3)
	} else if in.Name == "timeout_test" {
		if in.Age > 10 {
			s.t.Fatalf("can not deliver requests over 10 times because of link timeout")
			return &pb.HelloReply{Message: "Hello " + in.Name, Success: true}, nil
		}
		time.Sleep(time.Millisecond * 10)
		_, err := runClient(ctx, &clientConfig, s.t, "timeout_test", in.Age+1)
		return &pb.HelloReply{Message: "Hello " + in.Name, Success: true}, err
	} else if in.Name == "timeout_test2" {
		if in.Age > 10 {
			s.t.Fatalf("can not deliver requests over 10 times because of link timeout")
			return &pb.HelloReply{Message: "Hello " + in.Name, Success: true}, nil
		}
		time.Sleep(time.Millisecond * 10)
		_, err := runClient(ctx, &clientConfig2, s.t, "timeout_test2", in.Age+1)
		return &pb.HelloReply{Message: "Hello " + in.Name, Success: true}, err
	} else if in.Name == "error_detail" {
		err, _ := nmd.Error(nmd.Code(123456), "test_error_detail").WithDetails(&pb.HelloReply{Success: true})
		return nil, err
	} else if in.Name == "nmd_status" {
		reply := &pb.HelloReply{Message: "status", Success: true}
		st, _ := nmd.Error(nmd.RequestErr, "RequestErr").WithDetails(reply)
		return nil, st
	} else if in.Name == "general_error" {
		return nil, fmt.Errorf("hai is error")
	} else if in.Name == "nmd_code_error" {
		return nil, nmd.Conflict
	} else if in.Name == "pb_error_error" {
		return nil, nmd.Error(nmd.Code(11122), "hai")
	} else if in.Name == "nmd_status_error" {
		return nil, nmd.Error(nmd.RequestErr, "RequestErr")
	} else if in.Name == "test_remote_port" {
		if strconv.Itoa(int(in.Age)) != nmd.ToString(ctx, nmd.RemotePort) {
			return nil, fmt.Errorf("error port %d", in.Age)
		}
		reply := &pb.HelloReply{Message: "status", Success: true}
		return reply, nil
	} else if in.Name == "time_opt" {
		time.Sleep(time.Second)
		reply := &pb.HelloReply{Message: "status", Success: true}
		return reply, nil
	}

	return &pb.HelloReply{Message: "Hello " + in.Name, Success: true}, nil
}

func (s *helloServer) StreamHello(ss pb.Greeter_StreamHelloServer) error {
	for i := 0; i < 3; i++ {
		in, err := ss.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		ret := &pb.HelloReply{Message: "Hello " + in.Name, Success: true}
		err = ss.Send(ret)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewTestServerClient .
func NewTestServerClient(invoker func(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error), svrcfg *ServerConfig, clicfg *ClientConfig) (pb.GreeterClient, func() error) {
	srv := NewServer(svrcfg)
	pb.RegisterGreeterServer(srv.Server(), &testServer{helloFn: invoker})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	ch := make(chan bool, 1)
	go func() {
		ch <- true
		srv.Serve(lis)
	}()
	<-ch
	println(lis.Addr().String())
	conn, err := NewConn(lis.Addr().String(), clicfg, []string{"10000"})
	if err != nil {
		panic(err)
	}
	return pb.NewGreeterClient(conn), func() error { return srv.Shutdown(context.Background()) }
}

func runServer(t *testing.T, interceptors ...grpc.UnaryServerInterceptor) func() {
	return func() {
		server = NewServer(&ServerConfig{Network: "tcp", Addr: testAddr, Timeout: gutil.Duration(time.Second), EnableLog: true})
		pb.RegisterGreeterServer(server.Server(), &helloServer{t})
		server.Use(
			func(ctx context.Context, req interface{}, args *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
				outPut = append(outPut, "1")
				resp, err := handler(ctx, req)
				outPut = append(outPut, "2")
				return resp, err
			},
			func(ctx context.Context, req interface{}, args *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
				outPut = append(outPut, "3")
				resp, err := handler(ctx, req)
				outPut = append(outPut, "4")
				return resp, err
			})
		if _, err := server.Start(); err != nil {
			t.Fatal(err)
		}
	}
}

func runClient(ctx context.Context, cc *ClientConfig, t *testing.T, name string, age int32, interceptors ...grpc.UnaryClientInterceptor) (resp *pb.HelloReply, err error) {
	client := NewClient(cc)
	client.Use(interceptors...)
	conn, err := client.Dial(context.Background(), testAddr, []string{"10000"})
	if err != nil {
		panic(fmt.Errorf("did not connect: %v,req: %v %v", err, name, age))
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)
	resp, err = c.SayHello(ctx, &pb.HelloRequest{Name: name, Age: age})
	return
}

func testValidation(t *testing.T) {
	_, err := runClient(context.Background(), &clientConfig, t, "", 0)
	if !nmd.EqualError(nmd.ValidateErr, err) {
		t.Fatalf("testValidation should return nmd.RequestErr,but is %v", err)
	}
}

func testTimeoutOpt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	client := NewClient(&clientConfig)
	conn, err := client.Dial(ctx, testAddr, []string{"10000"})
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)
	start := time.Now()
	_, err = c.SayHello(ctx, &pb.HelloRequest{Name: "time_opt", Age: 0}, WithTimeoutCallOption(time.Millisecond*500))
	if err == nil {
		t.Fatalf("recovery must return error")
	}
	if time.Since(start) < time.Millisecond*400 {
		t.Fatalf("client timeout must be greater than 400 Milliseconds;err:=%v", err)
	}
}

func testAllErrorCase(t *testing.T) {
	ctx := context.Background()
	t.Run("general_error", func(t *testing.T) {
		_, err := runClient(ctx, &clientConfig, t, "general_error", 0)
		assert.Contains(t, err.Error(), "hai")
		ec := nmd.Cause(err)
		assert.Equal(t, -500, ec.Code())
		// remove this assert in future
		assert.Equal(t, "-500", ec.Message())
	})
	t.Run("nmd_code_error", func(t *testing.T) {
		_, err := runClient(ctx, &clientConfig, t, "nmd_code_error", 0)
		ec := nmd.Cause(err)
		assert.Equal(t, nmd.Conflict.Code(), ec.Code())
		// remove this assert in future
		assert.Equal(t, "-409", ec.Message())
	})
	t.Run("pb_error_error", func(t *testing.T) {
		_, err := runClient(ctx, &clientConfig, t, "pb_error_error", 0)
		ec := nmd.Cause(err)
		assert.Equal(t, 11122, ec.Code())
		assert.Equal(t, "hai", ec.Message())
	})
	t.Run("nmd_status_error", func(t *testing.T) {
		_, err := runClient(ctx, &clientConfig, t, "nmd_status_error", 0)
		ec := nmd.Cause(err)
		assert.Equal(t, nmd.RequestErr.Code(), ec.Code())
		assert.Equal(t, "RequestErr", ec.Message())
	})
}

func testRemotePort(t *testing.T) {
	ctx := nmd.NewContext(context.Background(), nmd.Metadata{nmd.RemotePort: "8000"})
	_, err := runClient(ctx, &clientConfig, t, "test_remote_port", 8000)
	if err != nil {
		t.Fatalf("testRemotePort return error %v", err)
	}
}

func testLinkTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	_, err := runClient(ctx, &clientConfig, t, "timeout_test", 0)
	if err == nil {
		t.Fatalf("testLinkTimeout must return error")
	}
	if !nmd.EqualError(nmd.Deadline, err) {
		t.Fatalf("testLinkTimeout must return error RPCDeadline,err:%v", err)
	}

}

func testClientConfig(t *testing.T) {
	_, err := runClient(context.Background(), &clientConfig2, t, "timeout_test2", 0)
	if err == nil {
		t.Fatalf("testLinkTimeout must return error")
	}
	if !nmd.EqualError(nmd.Deadline, err) {
		t.Fatalf("testLinkTimeout must return error RPCDeadline,err:%v", err)
	}
}

func testGracefulShutDown(t *testing.T) {
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := runClient(context.Background(), &clientConfig, t, "graceful_shutdown", 0)
			if err != nil {
				panic(fmt.Errorf("run graceful_shutdown client return(%v)", err))
			}
			if !resp.Success || resp.Message != "Hello graceful_shutdown" {
				panic(fmt.Errorf("run graceful_shutdown client return(%v,%v)", err, *resp))
			}
		}()
	}
	go func() {
		time.Sleep(time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		server.Shutdown(ctx)
	}()
	wg.Wait()
}

func testClientRecovery(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&clientConfig)
	client.Use(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) (ret error) {
		invoker(ctx, method, req, reply, cc, opts...)
		panic("client recovery test")
	})

	conn, err := client.Dial(ctx, testAddr, []string{"10000"})
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	_, err = c.SayHello(ctx, &pb.HelloRequest{Name: "other_test", Age: 0})
	if err == nil {
		t.Fatalf("recovery must return error")
	}
	e, ok := errors.Cause(err).(nmd.Coder)
	if !ok {
		t.Fatalf("recovery must return nmd error")
	}

	if !nmd.EqualError(nmd.ServerErr, e) {
		t.Fatalf("recovery must return nmd.RPCClientErr")
	}
}

func testServerRecovery(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&clientConfig)

	conn, err := client.Dial(ctx, testAddr, []string{"10000"})
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	_, err = c.SayHello(ctx, &pb.HelloRequest{Name: "recovery_test", Age: 0})
	if err == nil {
		t.Fatalf("recovery must return error")
	}
	e, ok := errors.Cause(err).(nmd.Coder)
	if !ok {
		t.Fatalf("recovery must return nmd error")
	}

	if e.Code() != nmd.ServerErr.Code() {
		t.Fatalf("recovery must return nmd.ServerErr")
	}
}

func testInterceptorChain(t *testing.T) {
	time.Sleep(time.Millisecond)
	if outPut[0] != "1" || outPut[1] != "3" || outPut[2] != "1" || outPut[3] != "3" || outPut[4] != "4" || outPut[5] != "2" || outPut[6] != "4" || outPut[7] != "2" {
		t.Fatalf("outPut shoud be [1 3 1 3 4 2 4 2]!")
	}
}

func testErrorDetail(t *testing.T) {
	_, err := runClient(context.Background(), &clientConfig2, t, "error_detail", 0)
	if err == nil {
		t.Fatalf("testErrorDetail must return error")
	}
	if ec, ok := errors.Cause(err).(nmd.Coder); !ok {
		t.Fatalf("testErrorDetail must return nmd error")
	} else if ec.Code() != 123456 || ec.Message() != "test_error_detail" || len(ec.Details()) == 0 {
		t.Fatalf("testErrorDetail must return code:123456 and message:test_error_detail, code: %d, message: %s, details length: %d", ec.Code(), ec.Message(), len(ec.Details()))
	} else if _, ok := ec.Details()[len(ec.Details())-1].(*pb.HelloReply); !ok {
		t.Fatalf("expect get pb.HelloReply %#v", ec.Details()[len(ec.Details())-1])
	}
}

func testMetaCodeStatus(t *testing.T) {
	_, err := runClient(context.Background(), &clientConfig2, t, "nmd_status", 0)
	if err == nil {
		t.Fatalf("testMetaCodeStatus must return error")
	}
	st, ok := errors.Cause(err).(*nmd.Status)
	if !ok {
		t.Fatalf("testMetaCodeStatus must return *nmd.Status")
	}
	if st.Code() != int(nmd.RequestErr) && st.Message() != "RequestErr" {
		t.Fatalf("testMetaCodeStatus must return code: -400, message: RequestErr get: code: %d, message: %s", st.Code(), st.Message())
	}
	detail := st.Details()[0].(*pb.HelloReply)
	if !detail.Success || detail.Message != "status" {
		t.Fatalf("wrong detail")
	}
}
