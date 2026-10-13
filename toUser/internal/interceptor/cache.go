package interceptor

import (
	"context"
	"encoding/json"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"test.com/others/encrypts"
	"test.com/toUser/internal/repo"
	"time"
)

type CacheInterceptor struct {
	cache    repo.Cache
	cacheMap map[string]any
}

type CacheRespOption struct {
	path   string
	typ    any
	expire time.Duration
}

func (c *CacheInterceptor) Cache() grpc.ServerOption {
	return grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		respType := c.cacheMap[info.FullMethod]
		if respType == nil {
			return handler(ctx, req)
		}

		ctx_, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		marshal, _ := json.Marshal(req)
		cacheKey := encrypts.Md5(string(marshal))
		respJson, _ := c.cache.Get(ctx_, info.FullMethod+"::"+cacheKey)
		if respJson != "" {
			json.Unmarshal([]byte(respJson), &respType)
			zap.L().Info(info.FullMethod + " 走缓存")
			return respType, nil
		}
		resp, err = handler(ctx, req)
		bytes, _ := json.Marshal(resp)
		c.cache.Put(ctx_, info.FullMethod+"::"+cacheKey, string(bytes), 5*time.Minute)
		zap.L().Info(info.FullMethod + " 写缓存")
		return
	})
}
