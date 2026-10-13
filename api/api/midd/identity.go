package midd

import (
	"context"
	"strings"

	"github.com/gin-gonic/gin"
)

type Identity struct {
	TenantId   int64
	MemberId   int64
	TenantCode string
}

const (
	TenantHeader  = "X-Tenant-Id"
	MemberHeader  = "X-Member-Id"
	DefaultTenant = int64(1)
)

type identityKey struct{}

func WithIdentity(ctx context.Context, id Identity) context.Context {
	if id.TenantId <= 0 {
		id.TenantId = DefaultTenant
	}
	return context.WithValue(ctx, identityKey{}, id)
}

func IdentityFrom(ctx context.Context) Identity {
	if ctx == nil {
		return Identity{TenantId: DefaultTenant}
	}
	if id, ok := ctx.Value(identityKey{}).(Identity); ok {
		if id.TenantId <= 0 {
			id.TenantId = DefaultTenant
		}
		return id
	}
	return Identity{TenantId: DefaultTenant}
}

func AttachIdentity(c *gin.Context, memberId, tenantId int64) {
	if c == nil || c.Request == nil {
		return
	}
	if tenantId <= 0 {
		tenantId = DefaultTenant
	}
	identity := Identity{
		TenantId: tenantId,
		MemberId: memberId,
	}
	c.Set("tenantId", identity.TenantId)
	c.Request = c.Request.WithContext(WithIdentity(c.Request.Context(), identity))
}

func AttachTenantCode(c *gin.Context, tenantCode string) {
	if c == nil || c.Request == nil {
		return
	}
	tenantCode = strings.TrimSpace(tenantCode)
	if tenantCode == "" {
		return
	}
	identity := IdentityFrom(c.Request.Context())
	identity.TenantCode = tenantCode
	c.Request = c.Request.WithContext(WithIdentity(c.Request.Context(), identity))
}

func IdentityMidd() gin.HandlerFunc {
	return func(c *gin.Context) {
		memberId := c.GetInt64("memberId")

		identity := Identity{TenantId: DefaultTenant, MemberId: memberId}
		c.Set("tenantId", identity.TenantId)
		c.Request = c.Request.WithContext(WithIdentity(c.Request.Context(), identity))

		c.Next()
	}
}
