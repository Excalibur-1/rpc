package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/Excalibur-1/code"
	"github.com/rs/zerolog/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// 客户端日志
func clientLogging(enableLog bool) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		startTime := time.Now()
		var peerInfo peer.Peer
		opts = append(opts, grpc.Peer(&peerInfo))

		// 调用者请求
		err := invoker(ctx, method, req, reply, cc, opts...)

		// after request
		cause := code.Cause(err)
		dt := time.Since(startTime)
		// 组装客户端日志
		if enableLog {
			logFn := log.Info().Str("path", method).
				Str("ip", peerInfo.Addr.String()).
				Int("ret", cause.Code()).
				Float64("ts", dt.Seconds()).
				Str("args", req.(fmt.Stringer).String()).
				Str("reply", reply.(fmt.Stringer).String())
			if err != nil {
				logFn.Err(err)
				logFn.Str("stack", fmt.Sprintf("%+v", err))
			}
			logFn.Msg("rpc-access-log")
		}
		return err
	}
}

// 服务器日志记录
func serverLogging(enableLog bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		startTime := time.Now()
		caller := code.ToString(ctx, code.Caller)
		if caller == "" {
			caller = "no_user"
		}
		var ip string
		if peerInfo, ok := peer.FromContext(ctx); ok {
			ip = peerInfo.Addr.String()
		}
		var quota float64
		if deadline, ok := ctx.Deadline(); ok {
			quota = time.Until(deadline).Seconds()
		}

		// call server handler
		resp, err := handler(ctx, req)

		// after server response
		cause := code.Cause(err)
		dt := time.Since(startTime)
		// monitor

		if enableLog && dt > 500*time.Millisecond {
			logFn := log.Info().Str("user", caller).
				Str("ip", ip).
				Str("path", info.FullMethod).
				Int("ret", cause.Code()).
				Float64("ts", dt.Seconds()).
				Float64("timeout_quota", quota).
				Str("req", req.(fmt.Stringer).String())
			if err != nil {
				logFn.Err(err)
				logFn.Str("stack", fmt.Sprintf("%+v", err))
			}
			logFn.Msg("rpc-access-log")
		}
		return resp, err
	}
}
