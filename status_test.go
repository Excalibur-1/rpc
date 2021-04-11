package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Excalibur-1/code"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCodeConvert(t *testing.T) {
	var table = map[codes.Code]code.Code{
		codes.OK: code.OK,
		// codes.Canceled
		codes.Unknown:          code.ServerErr,
		codes.InvalidArgument:  code.RequestErr,
		codes.DeadlineExceeded: code.Deadline,
		codes.NotFound:         code.NothingFound,
		// codes.AlreadyExists
		codes.PermissionDenied:  code.AccessDenied,
		codes.ResourceExhausted: code.LimitExceed,
		// codes.FailedPrecondition
		// codes.Aborted
		// codes.OutOfRange
		codes.Unimplemented: code.MethodNotAllowed,
		codes.Unavailable:   code.ServiceUnavailable,
		// codes.DataLoss
		codes.Unauthenticated: code.Unauthorized,
	}
	for k, v := range table {
		assert.Equal(t, toCode(status.New(k, "-500")), v)
	}
	for k, v := range table {
		assert.Equal(t, toGrpcCode(v), k, fmt.Sprintf("togRPC code error: %d -> %d", v, k))
	}
}

func TestNoDetailsConvert(t *testing.T) {
	gst := status.New(codes.Unknown, "-2233")
	assert.Equal(t, toCode(gst).Code(), -2233)

	gst = status.New(codes.Internal, "")
	assert.Equal(t, toCode(gst).Code(), -500)
}

func TestFromError(t *testing.T) {
	t.Run("input general error", func(t *testing.T) {
		err := errors.New("general error")
		gst := FromError(err)

		assert.Equal(t, codes.Unknown, gst.Code())
		assert.Contains(t, gst.Message(), "general")
	})
	t.Run("input wrap error", func(t *testing.T) {
		err := errors.Wrap(code.RequestErr, "hh")
		gst := FromError(err)

		assert.Equal(t, "-400", gst.Message())
	})
	t.Run("input code.Code", func(t *testing.T) {
		err := code.RequestErr
		gst := FromError(err)

		// assert.Equal(t, codes.InvalidArgument, gst.Code())
		// NOTE: set all grpc.status as Unknown when error is code.Codes for compatible
		assert.Equal(t, codes.Unknown, gst.Code())
		// NOTE: gst.Message == str(code.Code) for compatible php leagcy code
		assert.Equal(t, err.Message(), gst.Message())
	})
	t.Run("input raw Canceled", func(t *testing.T) {
		gst := FromError(context.Canceled)

		assert.Equal(t, codes.Unknown, gst.Code())
		assert.Equal(t, "-498", gst.Message())
	})
	t.Run("input raw DeadlineExceeded", func(t *testing.T) {
		gst := FromError(context.DeadlineExceeded)

		assert.Equal(t, codes.Unknown, gst.Code())
		assert.Equal(t, "-504", gst.Message())
	})
	t.Run("input code.Status", func(t *testing.T) {
		m := &timestamp.Timestamp{Seconds: time.Now().Unix()}
		err, _ := code.Error(code.Unauthorized, "unauthorized").WithDetails(m)
		gst := FromError(err)

		// NOTE: set all grpc.status as Unknown when error is code.Codes for compatible
		assert.Equal(t, codes.Unknown, gst.Code())
		assert.Len(t, gst.Details(), 1)
		details := gst.Details()
		assert.IsType(t, err.Proto(), details[0])
	})
}

func TestToCoder(t *testing.T) {
	t.Run("input general grpc.Status", func(t *testing.T) {
		gst := status.New(codes.Unknown, "unknown")
		ec := ToCoder(gst)

		assert.Equal(t, int(code.ServerErr), ec.Code())
		assert.Equal(t, "-500", ec.Message())
		assert.Len(t, ec.Details(), 0)
	})
	t.Run("input code.Status", func(t *testing.T) {
		m := &timestamp.Timestamp{Seconds: time.Now().Unix()}
		st, _ := code.Errorf(code.Unauthorized, "Unauthorized").WithDetails(m)
		gst := status.New(codes.InvalidArgument, "requesterr")
		gst, _ = gst.WithDetails(st.Proto())
		ec := ToCoder(gst)

		assert.Equal(t, int(code.Unauthorized), ec.Code())
		assert.Equal(t, "Unauthorized", ec.Message())
		assert.Len(t, ec.Details(), 1)
		assert.IsType(t, m, ec.Details()[0])
	})
}
