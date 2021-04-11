package rpc

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/Excalibur-1/code"
	"github.com/Excalibur-1/gutil"
	"github.com/Excalibur-1/trace"
	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // NOTE: use grpc gzip by header grpc-accept-encoding
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

var _abortIndex int8 = math.MaxInt8 / 2
var validate = validator.New()

// ServerConfig 服务器配置信息
type ServerConfig struct {
	Network           string         `json:"network"`           // 网络为rpc监听网络，默认值为tcp
	Addr              string         `json:"address"`           // 地址是rpc监听地址，默认值为0.0.0.0:9000
	Timeout           gutil.Duration `json:"timeout"`           // 超时是每个rpc调用的上下文超时。
	IdleTimeout       gutil.Duration `json:"idleTimeout"`       // IdleTimeout是一段持续时间，在这段时间内可以通过发送GoAway关闭空闲连接。 空闲持续时间是自最近一次未完成RPC的数量变为零或建立连接以来定义的。
	MaxLifeTime       gutil.Duration `json:"maxLife"`           // MaxLifeTime是连接通过发送GoAway关闭之前可能存在的最长时间的持续时间。 将向+/- 10％的随机抖动添加到MaxConnectionAge中以分散连接风暴.
	ForceCloseWait    gutil.Duration `json:"closeWait"`         // ForceCloseWait是MaxLifeTime之后的附加时间，在此之后将强制关闭连接。
	KeepAliveInterval gutil.Duration `json:"keepaliveInterval"` // 如果服务器没有看到任何活动，则KeepAliveInterval将在此时间段之后，对客户端进行ping操作以查看传输是否仍然有效。
	KeepAliveTimeout  gutil.Duration `json:"keepaliveTimeout"`  // 进行keepalive检查ping之后，服务器将等待一段时间的超时，并且即使在关闭连接后也看不到活动。
	EnableLog         bool           `json:"enableLog"`         // 是否打开日记
}

// Server 是框架的服务器端实例，它包含RpcServer，拦截器和拦截器。
// 通过使用NewServer()创建Server的实例。
type Server struct {
	conf     *ServerConfig
	mutex    sync.RWMutex
	server   *grpc.Server
	handlers []grpc.UnaryServerInterceptor
}

// NewServer 带有默认服务器拦截器的新的空白Server实例。
func NewServer(conf *ServerConfig, opt ...grpc.ServerOption) (s *Server) {
	s = new(Server)
	if err := s.SetConfig(conf); err != nil {
		panic(errors.Errorf("rpc set config failed!err: %s", err.Error()))
	}
	keepParam := grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle:     time.Duration(s.conf.IdleTimeout),
		MaxConnectionAgeGrace: time.Duration(s.conf.ForceCloseWait),
		Time:                  time.Duration(s.conf.KeepAliveInterval),
		Timeout:               time.Duration(s.conf.KeepAliveTimeout),
		MaxConnectionAge:      time.Duration(s.conf.MaxLifeTime),
	})
	opt = append(opt, keepParam, grpc.UnaryInterceptor(s.interceptor))
	s.server = grpc.NewServer(opt...)
	s.Use(s.recovery(), s.handle(), serverLogging(conf.EnableLog), s.validate())
	return
}

// SetConfig 热重载服务器配置
func (s *Server) SetConfig(conf *ServerConfig) (err error) {
	s.mutex.Lock()
	s.conf = conf
	s.mutex.Unlock()
	return nil
}

// Use 将全局拦截器附加到服务器.
// 例如:这是速率限制器或错误管理拦截器的正确位置.
func (s *Server) Use(handlers ...grpc.UnaryServerInterceptor) *Server {
	finalSize := len(s.handlers) + len(handlers)
	if finalSize >= int(_abortIndex) {
		panic("rpc: server use too many handlers")
	}
	mergedHandlers := make([]grpc.UnaryServerInterceptor, finalSize)
	copy(mergedHandlers, s.handlers)
	copy(mergedHandlers[len(s.handlers):], handlers)
	s.handlers = mergedHandlers
	return s
}

// Server 返回用于注册服务的rpc服务器.
func (s *Server) Server() *grpc.Server {
	return s.server
}

// RegisterValidation 将验证功能添加到由键表示的验证者的验证者映射中
// 注意:如果密钥已经存在,则先前的验证功能将被替换。
// 注意:此方法不是线程安全的,因此应在进行任何验证之前先将它们全部注册
func (s *Server) RegisterValidation(key string, fn validator.Func) error {
	return validate.RegisterValidation(key, fn)
}

// Shutdown 可以正常停止服务器。
// 它停止服务器接受新的连接和RPC,并阻止直到所有未完成的RPC完成或到达上下文截止日期为止.
func (s *Server) Shutdown(ctx context.Context) (err error) {
	ch := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(ch)
	}()
	select {
	case <-ctx.Done():
		s.server.Stop()
		err = ctx.Err()
	case <-ch:
	}
	return
}

// Run 运行create tcp侦听器,并启动goroutine为每个传入请求提供服务。
// 除非调用Stop或GracefulStop,否则Run将返回非nil错误。
func (s *Server) Run(addr string) error {
	fmt.Printf("启动rpc监听地址: %s\n", addr)
	if lis, err := net.Listen("tcp", addr); err != nil {
		err = errors.WithStack(err)
		return err
	} else {
		reflection.Register(s.server)
		return s.Serve(lis)
	}
}

// RunUnix 创建一个unix侦听器并启动goroutine来处理每个传入的请求.
// 除非调用Stop或GracefulStop,否则RunUnix将返回非nil错误.
func (s *Server) RunUnix(file string) error {
	fmt.Printf("启动rpc监听unix文件: %s\n", file)
	if lis, err := net.Listen("unix", file); err != nil {
		err = errors.WithStack(err)
		return err
	} else {
		reflection.Register(s.server)
		return s.Serve(lis)
	}
}

// Serve 在侦听器lis上接受传入连接,从而为每个连接创建一个新的ServerTransport和服务goroutine。
// 除非调用Stop或GracefulStop,否则Serve将返回非nil错误.
func (s *Server) Serve(lis net.Listener) error {
	return s.server.Serve(lis)
}

// Start 开始使用配置的listen addr创建一个新的goroutine运行服务器,如果发生任何错误,它将惊慌返回服务器本身.
func (s *Server) Start() (*Server, error) {
	if _, err := s.startWithAddr(); err != nil {
		return nil, err
	} else {
		return s, nil
	}
}

// StartWithAddr 使用配置的监听地址创建一个新的goroutine运行服务器,如果发生任何错误,它将崩溃
// 返回服务器本身和实际的监听地址(如果配置的监听端口为零,则操作系统将分配一个未使用的端口)
func (s *Server) StartWithAddr() (*Server, net.Addr, error) {
	if addr, err := s.startWithAddr(); err != nil {
		return nil, nil, err
	} else {
		return s, addr, nil
	}
}

func (s *Server) startWithAddr() (net.Addr, error) {
	if lis, err := net.Listen(s.conf.Network, s.conf.Addr); err != nil {
		return nil, err
	} else {
		fmt.Printf("rpc: start grpc listen addr: %v\n", lis.Addr())
		reflection.Register(s.server)
		go func() {
			if err := s.Serve(lis); err != nil {
				panic(err)
			}
		}()
		return lis.Addr(), nil
	}
}

// 拦截器是许多拦截器链中的单个拦截器。
// 执行以从左到右的顺序进行,包括传递上下文.
// 例如:ChainUnaryServer(1、2、3）将在3之前的2之前执行1,而3将看到1和2的上下文更改.
func (s *Server) interceptor(ctx context.Context, req interface{}, args *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	var i int
	var chain grpc.UnaryHandler

	n := len(s.handlers)
	if n == 0 {
		return handler(ctx, req)
	}

	chain = func(ic context.Context, ir interface{}) (interface{}, error) {
		if i == n-1 {
			return handler(ic, ir)
		}
		i++
		return s.handlers[i](ic, ir, args, chain)
	}

	return s.handlers[0](ctx, req, args, chain)
}

// recovery是从任何紧急情况中恢复的服务器拦截器。
func (s *Server) recovery() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, args *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if _err := recover(); _err != nil {
				const size = 64 << 10
				buf := make([]byte, size)
				rs := runtime.Stack(buf, false)
				if rs > size {
					rs = size
				}
				buf = buf[:rs]
				pl := fmt.Sprintf("grpc server panic: %v\n%v\n%s\n", req, _err, buf)
				_, _ = fmt.Fprintf(os.Stderr, pl)
				err = status.Errorf(codes.Unknown, code.ServerErr.Error())
			}
		}()
		resp, err = handler(ctx, req)
		return
	}
}

// handle为OpenTracing\Logging\LinkTimeout返回一个新的一元服务器拦截器。
func (s *Server) handle() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, args *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		// 对于性能进行监测等等
		s.mutex.RLock()
		conf := s.conf
		s.mutex.RUnlock()
		// 从rpc上下文获取派生超时，与配置的看守进行比较，并使用最小值
		timeout := time.Duration(conf.Timeout)
		if dl, ok := ctx.Deadline(); ok {
			_timeout := time.Until(dl)
			if _timeout-time.Millisecond*20 > 0 {
				_timeout = _timeout - time.Millisecond*20
			}
			if timeout > _timeout {
				timeout = _timeout
			}
		}
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()

		// 获取rpc元数据(trace＆remote_ip＆color)
		var t trace.Trace
		cmd := code.Metadata{}
		if gmd, ok := metadata.FromIncomingContext(ctx); ok {
			t, _ = trace.Extract(trace.GRPCFormat, gmd)
			for k, v := range gmd {
				if code.IsIncomingKey(k) {
					cmd[k] = v[0]
				}
			}
		}
		if t == nil {
			t = trace.New(args.FullMethod)
		} else {
			t.SetTitle(args.FullMethod)
		}

		var addr string
		if pr, ok := peer.FromContext(ctx); ok {
			addr = pr.Addr.String()
			t.SetTag(trace.String(trace.TagAddress, addr))
		}
		defer t.Finish(&err)

		// 使用公共元数据上下文而不是rpc上下文
		ctx = code.NewContext(ctx, cmd)
		ctx = trace.NewContext(ctx, t)

		resp, err = handler(ctx, req)
		return resp, FromError(err).Err()
	}
}

// 验证返回一个客户端拦截器,以验证每个RPC调用的传入请求.
func (s *Server) validate() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, args *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if err = validate.Struct(req); err != nil {
			err = code.Error(code.ValidateErr, err.Error())
			return
		}
		resp, err = handler(ctx, req)
		return
	}
}
