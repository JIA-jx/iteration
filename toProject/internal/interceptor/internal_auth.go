package interceptor

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const internalTokenHeader = "X-Internal-Token"

var (
	internalTokenOnce sync.Once
	internalTokenVal  string
)

func resolveInternalToken(configured string) string {
	internalTokenOnce.Do(func() {
		internalTokenVal = strings.TrimSpace(configured)
		if internalTokenVal == "" {
			internalTokenVal = strings.TrimSpace(os.Getenv("INTERNAL_API_TOKEN"))
		}
		if internalTokenVal == "" {
			buf := make([]byte, 24)
			if _, err := rand.Read(buf); err != nil {
				internalTokenVal = ""
				return
			}
			internalTokenVal = hex.EncodeToString(buf)
			zap.L().Warn("internal token not configured, generated one for this process",
				zap.String("header", internalTokenHeader),
				zap.String("token", internalTokenVal))
		}
	})
	return internalTokenVal
}

func InternalAuthMiddleware(configured string, enabled bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !enabled {
			c.Next()
			return
		}

		want := resolveInternalToken(configured)
		if want == "" {
			c.Next()
			return
		}

		got := strings.TrimSpace(c.GetHeader(internalTokenHeader))
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1 {
			c.Next()
			return
		}

		zap.L().Warn("internal auth rejected",
			zap.String("path", c.Request.URL.Path),
			zap.String("client_ip", c.ClientIP()))

		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"code": 401,
			"msg":  "缺少或错误的 X-Internal-Token",
		})
	}
}
