package interceptor

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"test.com/others/tenant"
)

const TenantHeader = "X-Tenant-Id"

const TenantCodeHeader = "X-Tenant-Code"

const InternalTokenHeader = "X-Internal-Token"

func TenantGinMiddleware(internalToken string) gin.HandlerFunc {
	return func(c *gin.Context) {
		info := tenant.Info{Id: tenant.DefaultId, Code: tenant.DefaultCode}

		rawId := strings.TrimSpace(c.GetHeader(TenantHeader))
		rawCode := strings.TrimSpace(c.GetHeader(TenantCodeHeader))

		if rawId != "" || rawCode != "" {
			if trusted(c.GetHeader(InternalTokenHeader), internalToken) {
				if parsed, err := strconv.ParseInt(rawId, 10, 64); err == nil && parsed > 0 {
					info.Id = parsed
				}
				if rawCode != "" {
					info.Code = rawCode
				}
			} else {
				zap.L().Warn("tenant header rejected: internal token missing or invalid",
					zap.String("path", c.Request.URL.Path),
					zap.String("client_ip", c.ClientIP()))
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
					"code": 403,
					"msg":  "缺少或错误的 X-Internal-Token",
				})
				return
			}
		}

		if c.Request != nil {
			c.Request = c.Request.WithContext(tenant.WithContext(c.Request.Context(), info))
		}

		c.Next()
	}
}

func trusted(got, want string) bool {
	got = strings.TrimSpace(got)
	if got == "" || want == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
