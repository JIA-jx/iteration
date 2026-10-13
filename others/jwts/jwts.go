package jwts

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/martian/log"
	"os"
	"time"
)

type JwtToken struct {
	AccessToken  string
	RefreshToken string
	AccessExp    int64
	RefreshExp   int64
}

func LoadRSAPrivateKey(filePath string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("读取私钥文件失败: %w", err)
	}

	block, _ := pem.Decode(data)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		return nil, errors.New("私钥格式错误")
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("解析私钥失败: %w", err)
	}
	return privateKey, nil
}

func LoadRSAPublicKey(filePath string) (*rsa.PublicKey, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("读取公钥文件失败: %w", err)
	}

	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PUBLIC KEY" {
		return nil, errors.New("公钥格式错误")
	}

	publicKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("解析公钥失败: %w", err)
	}
	rsaKey, ok := publicKey.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("公钥不是 RSA 类型")
	}
	return rsaKey, nil
}

func CreateToken(token string,
	accessExp time.Duration, accessPrivateKey *rsa.PrivateKey,
	refreshExp time.Duration, refreshPrivateKey *rsa.PrivateKey,
	ip string) *JwtToken {
	return CreateTokenWithTenant(token, 0, accessExp, accessPrivateKey, refreshExp, refreshPrivateKey, ip)
}

func CreateTokenWithTenant(token string, tenantId int64,
	accessExp time.Duration, accessPrivateKey *rsa.PrivateKey,
	refreshExp time.Duration, refreshPrivateKey *rsa.PrivateKey,
	ip string) *JwtToken {

	aExp := time.Now().Add(accessExp).Unix()
	accessClaims := jwt.MapClaims{
		"token": token,
		"exp":   aExp,
		"ip":    ip,
		"type":  "access",
	}
	if tenantId > 0 {
		accessClaims["tenant"] = tenantId
	}
	accessToken := jwt.NewWithClaims(jwt.SigningMethodRS256, accessClaims)
	aToken, err := accessToken.SignedString(accessPrivateKey)
	if err != nil {
		log.Errorf("生成access token失败: %w", err)
		return nil
	}

	rExp := time.Now().Add(refreshExp).Unix()
	refreshClaims := jwt.MapClaims{
		"token": token,
		"exp":   rExp,
		"type":  "refresh",
	}
	if tenantId > 0 {
		refreshClaims["tenant"] = tenantId
	}
	refreshToken := jwt.NewWithClaims(jwt.SigningMethodRS256, refreshClaims)
	rToken, err := refreshToken.SignedString(refreshPrivateKey)
	if err != nil {
		log.Errorf("生成refresh token失败: %w", err)
		return nil
	}

	return &JwtToken{
		AccessExp:    aExp,
		AccessToken:  aToken,
		RefreshExp:   rExp,
		RefreshToken: rToken,
	}
}

func ParseAccessToken(tokenString string, publicKey *rsa.PublicKey, ip string) (string, error) {
	subject, _, err := parseTokenWithType(tokenString, publicKey, ip, "access")
	return subject, err
}

func ParseRefreshToken(tokenString string, publicKey *rsa.PublicKey) (string, error) {
	subject, _, err := parseTokenWithType(tokenString, publicKey, "", "refresh")
	return subject, err
}

func ParseAccessTokenWithTenant(tokenString string, publicKey *rsa.PublicKey, ip string) (string, int64, error) {
	return parseTokenWithType(tokenString, publicKey, ip, "access")
}

func ParseRefreshTokenWithTenant(tokenString string, publicKey *rsa.PublicKey) (string, int64, error) {
	return parseTokenWithType(tokenString, publicKey, "", "refresh")
}

func parseTokenWithType(tokenString string, publicKey *rsa.PublicKey, ip string, tokenType string) (string, int64, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("不支持的签名算法: %v", token.Header["alg"])
		}
		return publicKey, nil
	})
	if err != nil {
		return "", 0, fmt.Errorf("解析 token 失败: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return "", 0, errors.New("token 无效")
	}

	if claims["type"] != tokenType {
		return "", 0, errors.New("token 类型不匹配" + tokenType + " token）")
	}

	exp, ok := claims["exp"].(float64)
	if !ok || int64(exp) <= time.Now().Unix() {
		return "", 0, errors.New("token 已过期")
	}

	if tokenType == "access" && ip != "" && claims["ip"] != ip {
		return "", 0, errors.New("IP不合法")
	}

	val, ok := claims["token"].(string)
	if !ok {
		return "", 0, errors.New("token 内容格式错误")
	}

	var tenantId int64
	if raw, exists := claims["tenant"]; exists {
		if f, isFloat := raw.(float64); isFloat {
			tenantId = int64(f)
		}
	}

	return val, tenantId, nil
}
