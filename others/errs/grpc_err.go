package errs

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	others "test.com/others"
)

func GrpcError(err *MError) error {
	return status.Error(codes.Code(err.Code), err.Msg)
}

func ToMError(err error) *MError {
	fromError, _ := status.FromError(err)
	return NewError(ErrorCode(fromError.Code()), fromError.Message())
}

func ParseGrpcError(err error) (others.InterationCode, string) {
	fromError, _ := status.FromError(err)
	return others.InterationCode(fromError.Code()), fromError.Message()
}
