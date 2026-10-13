package midd

import (
	"crypto/rsa"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"test.com/others/jwts"
	"test.com/toUser/pkg/model"
)

var publicKey *rsa.PublicKey

func InitJWT() {
	var err error
	publicKey, err = jwts.LoadRSAPublicKey("D:/tmping/iteration/api/api/midd/conf/rsa/rsa_public_key.pem")
	if err != nil {
		panic("load rsa public key failed: " + err.Error())
	}
}

func MemberIdFromToken(token string) (int64, error) {
	token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
	if token == "" {
		return 0, errors.New("empty token")
	}
	if publicKey == nil {
		return 0, errors.New("rsa public key not loaded")
	}

	raw, err := jwts.ParseAccessToken(token, publicKey, "")
	if err != nil {
		return 0, err
	}

	memberId, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	return memberId, nil
}

func MemberIdFromTokenWithIP(c *gin.Context, token string) (int64, error) {
	token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
	if token == "" {
		return 0, errors.New("empty token")
	}
	if publicKey == nil {
		return 0, errors.New("rsa public key not loaded")
	}

	raw, err := jwts.ParseAccessToken(token, publicKey, c.ClientIP())
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(raw, 10, 64)
}

func JWT_Midd() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("Authorization")
		if token == "" || len(token) < 8 || token[:7] != "Bearer " {
			c.JSON(http.StatusUnauthorized, gin.H{"code": model.NoLogin, "msg": "未登录"})
			c.Abort()
			return
		}

		token = token[7:]
		ip := c.ClientIP()

		subject, tenantId, err := jwts.ParseAccessTokenWithTenant(token, publicKey, ip)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{
				"code": model.NoLogin,
				"msg":  "无效token",
			})
			c.Abort()
			return
		}

		memberId, err := strconv.ParseInt(subject, 10, 64)
		if err != nil || memberId <= 0 {
			c.JSON(http.StatusUnauthorized, gin.H{
				"code": model.NoLogin,
				"msg":  "无效token",
			})
			c.Abort()
			return
		}

		c.Set("memberId", memberId)
		c.Set("tokenIP", ip)
		AttachIdentity(c, memberId, tenantId)

		c.Next()
	}
}
