package midd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	RequestIdHeader = "X-Request-Id"
	requestIdKey    = "requestId"
)

type requestIdCtxKey struct{}

func WithRequestId(ctx context.Context, id string) context.Context {
	if strings.TrimSpace(id) == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIdCtxKey{}, id)
}

func RequestIdFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if id, ok := ctx.Value(requestIdCtxKey{}).(string); ok {
		return id
	}
	return ""
}

func NewRequestId() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

func RequestIdMidd() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := strings.TrimSpace(c.GetHeader(RequestIdHeader))
		if id == "" {
			id = NewRequestId()
		}

		c.Set(requestIdKey, id)
		c.Header(RequestIdHeader, id)

		if c.Request != nil {
			c.Request = c.Request.WithContext(WithRequestId(c.Request.Context(), id))
		}

		c.Next()
	}
}
