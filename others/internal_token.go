package common

import (
	"context"
	"crypto/subtle"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"test.com/others/secretfile"
	"test.com/others/tenant"
)

const (
	InternalTokenHeader = "x-internal-token"
	TenantIdHeader      = "x-tenant-id"
)

var internalToken = secretfile.Load("INTERNAL_API_TOKEN", ".iteration_internal_token", 24)

func InternalToken() string {
	return internalToken
}

func InternalAuthServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		got := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get(InternalTokenHeader); len(v) > 0 {
				got = strings.TrimSpace(v[0])
			}
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(internalToken)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "缺少或错误的内部调用凭证")
		}
		return handler(ctx, req)
	}
}

func InternalAuthClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, InternalTokenHeader, internalToken)
		if id := tenant.IdFromContext(ctx); id > 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, TenantIdHeader, strconv.FormatInt(id, 10))
		}
		if info, ok := tenant.FromContext(ctx); ok && info.Code != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-tenant-code", info.Code)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
