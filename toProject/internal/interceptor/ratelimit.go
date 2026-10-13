package interceptor

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"test.com/others/ratelimit"
	"test.com/toProject/internal/dao"
)

var (
	grpcRateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_ratelimit_rejected_total",
		Help: "gRPC calls rejected by the distributed rate limiter",
	}, []string{"dimension"})

	grpcRateDegraded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grpc_ratelimit_degraded_total",
		Help: "gRPC calls served while the distributed limiter was degraded",
	})
)

type RateLimitInterceptor struct {
	tenantLimiter ratelimit.Limiter
	memberLimiter ratelimit.Limiter
}

func NewRateLimitInterceptor(tenantRule, memberRule ratelimit.Rule) *RateLimitInterceptor {
	var rdb *redis.Client
	if dao.Rc != nil {
		rdb = dao.Rc.Rdb
	}

	return &RateLimitInterceptor{
		tenantLimiter: ratelimit.NewResilientLimiter(
			ratelimit.NewRedisLimiter(rdb, tenantRule),
			ratelimit.NewLocalLimiter(tenantRule),
		),
		memberLimiter: ratelimit.NewResilientLimiter(
			ratelimit.NewRedisLimiter(rdb, memberRule),
			ratelimit.NewLocalLimiter(memberRule),
		),
	}
}

func (r *RateLimitInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		tenantId := firstValue(md, "x-tenant-id", "1")
		memberId := firstValue(md, "x-member-id", "0")

		res := r.tenantLimiter.Allow(ctx, tenantId)
		if res.Degraded {
			grpcRateDegraded.Inc()
		}
		if !res.Allowed {
			grpcRateLimited.WithLabelValues("tenant").Inc()
			return nil, status.Errorf(codes.ResourceExhausted,
				"租户请求过于频繁，请 %dms 后重试", res.RetryAfter.Milliseconds())
		}

		if memberId != "0" && memberId != "" {
			memberRes := r.memberLimiter.Allow(ctx, tenantId+":"+memberId)
			if memberRes.Degraded {
				grpcRateDegraded.Inc()
			}
			if !memberRes.Allowed {
				grpcRateLimited.WithLabelValues("member").Inc()
				return nil, status.Errorf(codes.ResourceExhausted,
					"操作过于频繁，请 %dms 后重试", memberRes.RetryAfter.Milliseconds())
			}
		}

		return handler(ctx, req)
	}
}

func DefaultTenantRule() ratelimit.Rule {
	return ratelimit.Rule{
		Name:   "grpc_tenant",
		Limit:  2000,
		Window: time.Second,
		Burst:  4000,
	}
}

func DefaultMemberRule() ratelimit.Rule {
	return ratelimit.Rule{
		Name:   "grpc_member",
		Limit:  100,
		Window: time.Second,
		Burst:  200,
	}
}

func firstValue(md metadata.MD, key, def string) string {
	if md == nil {
		return def
	}
	vals := md.Get(key)
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			return v
		}
	}
	return def
}
