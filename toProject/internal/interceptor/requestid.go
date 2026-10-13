package interceptor

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

const RequestIdMetadataKey = "x-request-id"

func RequestIdFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, v := range md.Get(RequestIdMetadataKey) {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}
