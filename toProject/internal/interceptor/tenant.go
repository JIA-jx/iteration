package interceptor

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"test.com/others/tenant"
)

func TenantUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		tenantCtx, err := WithTenantFromMetadataStrict(ctx)
		if err != nil {
			return nil, err
		}
		return handler(tenantCtx, req)
	}
}

func WithTenantFromMetadataStrict(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "缺少租户标识")
	}

	raw := firstNonEmpty(md.Get("x-tenant-id"))
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || parsed <= 0 {
		return nil, status.Error(codes.InvalidArgument, "缺少或非法的租户标识")
	}

	info := tenant.Info{Id: parsed, Code: tenant.DefaultCode}
	if v := firstNonEmpty(md.Get("x-tenant-code")); v != "" {
		info.Code = v
	}

	return tenant.WithContext(ctx, info), nil
}

func WithTenantFromMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return tenant.WithContext(ctx, tenant.Info{Id: tenant.DefaultId, Code: tenant.DefaultCode})
	}

	info := tenant.Info{Id: tenant.DefaultId, Code: tenant.DefaultCode}

	if v := firstNonEmpty(md.Get("x-tenant-id")); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
			info.Id = parsed
		}
	}
	if v := firstNonEmpty(md.Get("x-tenant-code")); v != "" {
		info.Code = v
	}

	return tenant.WithContext(ctx, info)
}

func firstNonEmpty(vals []string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}
