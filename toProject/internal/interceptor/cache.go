package interceptor

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"test.com/others/encrypts"
	"test.com/rpcOrgrpc/task"
	"test.com/toProject/internal/cachekv"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/repo"
)

var (
	cacheHit = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_cache_hit_total",
		Help: "gRPC 方法级缓存命中次数",
	}, []string{"method"})

	cacheMiss = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_cache_miss_total",
		Help: "gRPC 方法级缓存未命中（回源）次数",
	}, []string{"method"})

	cacheDecodeError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_cache_decode_error_total",
		Help: "缓存内容无法反序列化（通常是发版后结构体变更）次数",
	}, []string{"method"})

	cacheInvalidated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grpc_cache_invalidated_total",
		Help: "按分组批量失效的缓存 key 数",
	})
)

const kvKeyPrefix = cachekv.GRPCPrefix

type cacheEntry struct {
	newResp func() interface{}
	expire  time.Duration
	group   string
}

type CacheInterceptor struct {
	cache   repo.Cache
	entries map[string]cacheEntry
}

func New() *CacheInterceptor {
	entries := map[string]cacheEntry{
		"/task.service.v1.TaskService/TaskList": {
			newResp: func() interface{} { return &task.TaskListResponse{} },
			expire:  5 * time.Minute,
			group:   "task",
		},
	}
	return &CacheInterceptor{cache: dao.Rc, entries: entries}
}

func (c *CacheInterceptor) CacheInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		entry, ok := c.entries[info.FullMethod]
		if !ok {
			return handler(ctx, req)
		}

		key := c.buildKey(ctx, info.FullMethod, req)

		readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
		raw, gErr := c.cache.Get(readCtx, key)
		if gErr == nil && raw != "" {
			out := entry.newResp()
			if jErr := json.Unmarshal([]byte(raw), out); jErr == nil {
				readCancel()
				cacheHit.WithLabelValues(info.FullMethod).Inc()
				return out, nil
			}

			cacheDecodeError.WithLabelValues(info.FullMethod).Inc()
			zap.L().Warn("grpc cache decode failed, drop stale key",
				zap.String("method", info.FullMethod), zap.String("key", key))
			_, _ = c.cache.Del(readCtx, key)
		}
		readCancel()
		cacheMiss.WithLabelValues(info.FullMethod).Inc()

		resp, err = handler(ctx, req)
		if err != nil {
			return resp, err
		}

		body, mErr := json.Marshal(resp)
		if mErr != nil {
			zap.L().Warn("grpc cache marshal failed",
				zap.String("method", info.FullMethod), zap.Error(mErr))
			return resp, nil
		}

		writeCtx, writeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer writeCancel()

		if pErr := c.cache.Put(writeCtx, key, string(body), entry.expire); pErr != nil {
			zap.L().Warn("grpc cache put failed",
				zap.String("method", info.FullMethod), zap.Error(pErr))
			return resp, nil
		}

		if entry.group != "" {
			if _, sErr := c.cache.SAdd(writeCtx, cachekv.GroupIndex(entry.group), key); sErr != nil {
				zap.L().Warn("grpc cache index sadd failed",
					zap.String("group", entry.group), zap.Error(sErr))
			}
		}

		return resp, nil
	}
}

func (c *CacheInterceptor) InvalidateGroup(ctx context.Context, group string) {
	if n := cachekv.InvalidateGroup(ctx, c.cache, group); n > 0 {
		cacheInvalidated.Add(float64(n))
	}
}

func (c *CacheInterceptor) buildKey(ctx context.Context, method string, req interface{}) string {
	body, _ := json.Marshal(req)
	return kvKeyPrefix + strings.TrimPrefix(method, "/") + "::" + scopeOf(ctx) + "::" + encrypts.Md5(string(body))
}

func scopeOf(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "global"
	}
	tenant := firstOr(md.Get("x-tenant-id"), "0")
	member := firstOr(md.Get("x-member-id"), "0")
	return "t" + tenant + ":m" + member
}

func firstOr(vals []string, def string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return def
}
