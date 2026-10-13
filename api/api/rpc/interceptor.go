package rpc

import (
	"context"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"test.com/api/api/midd"
	common "test.com/others"
)

func MetadataInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		identity := midd.IdentityFrom(ctx)

		pairs := []string{
			"x-tenant-id", strconv.FormatInt(identity.TenantId, 10),
			"x-member-id", strconv.FormatInt(identity.MemberId, 10),
		}

		if token := common.InternalToken(); token != "" {
			pairs = append(pairs, common.InternalTokenHeader, token)
		}

		if identity.TenantCode != "" {
			pairs = append(pairs, "x-tenant-code", identity.TenantCode)
		}

		if requestId := midd.RequestIdFrom(ctx); requestId != "" {
			pairs = append(pairs, "x-request-id", requestId)
		}

		ctx = metadata.AppendToOutgoingContext(ctx, pairs...)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
