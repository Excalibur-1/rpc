package rpc

import (
	"context"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Excalibur-1/code"
)

//  convert code.Coder to grpc code
func toGrpcCode(coder code.Coder) codes.Code {
	switch coder.Code() {
	case code.OK.Code():
		return codes.OK
	case code.RequestErr.Code():
		return codes.InvalidArgument
	case code.NothingFound.Code():
		return codes.NotFound
	case code.Unauthorized.Code():
		return codes.Unauthenticated
	case code.AccessDenied.Code():
		return codes.PermissionDenied
	case code.LimitExceed.Code():
		return codes.ResourceExhausted
	case code.MethodNotAllowed.Code():
		return codes.Unimplemented
	case code.Deadline.Code():
		return codes.DeadlineExceeded
	case code.ServiceUnavailable.Code():
		return codes.Unavailable
	}
	return codes.Unknown
}

// convert grpc.Status to code.Code
func toCode(gst *status.Status) code.Code {
	co := gst.Code()
	switch co {
	case codes.OK:
		return code.OK
	case codes.InvalidArgument:
		return code.RequestErr
	case codes.NotFound:
		return code.NothingFound
	case codes.PermissionDenied:
		return code.AccessDenied
	case codes.Unauthenticated:
		return code.Unauthorized
	case codes.ResourceExhausted:
		return code.LimitExceed
	case codes.Unimplemented:
		return code.MethodNotAllowed
	case codes.DeadlineExceeded:
		return code.Deadline
	case codes.Unavailable:
		return code.ServiceUnavailable
	case codes.Unknown:
		return code.String(gst.Message())
	}
	return code.ServerErr
}

// FromError convert error for service reply and try to convert it to grpc.Status.
func FromError(svrErr error) (gst *status.Status) {
	var err error
	svrErr = errors.Cause(svrErr)
	if coder, ok := svrErr.(code.Coder); ok {
		// TODO: deal with err
		if gst, err = grpcStatusFromCode(coder); err == nil {
			return
		}
	}
	// for some special error convert context.Canceled to code.Canceled,
	// context.DeadlineExceeded to code.DeadlineExceeded only for raw error
	// if err be wrapped will not effect.
	switch svrErr {
	case context.Canceled:
		gst, _ = grpcStatusFromCode(code.Canceled)
	case context.DeadlineExceeded:
		gst, _ = grpcStatusFromCode(code.Deadline)
	default:
		gst, _ = status.FromError(svrErr)
	}
	return
}

func grpcStatusFromCode(coder code.Coder) (*status.Status, error) {
	var st *code.Status
	switch v := coder.(type) {
	case *code.Status:
		st = v
	case code.Code:
		st = code.FromCode(v)
	default:
		st = code.Error(code.Code(coder.Code()), coder.Message())
		for _, detail := range coder.Details() {
			if msg, ok := detail.(proto.Message); ok {
				_, _ = st.WithDetails(msg)
			}
		}
	}
	gst := status.New(codes.Unknown, strconv.Itoa(st.Code()))
	return gst.WithDetails(st.Proto())
}

// ToCoder convert grpc.status to code.Codes
func ToCoder(gst *status.Status) code.Coder {
	details := gst.Details()
	for _, detail := range details {
		// convert detail to status only use first detail
		if pb, ok := detail.(proto.Message); ok {
			return code.FromProto(pb)
		}
	}
	return toCode(gst)
}
