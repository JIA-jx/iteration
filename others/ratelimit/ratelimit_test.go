package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestLocalLimiterAllowsUpToLimitThenRejects(t *testing.T) {
	rule := Rule{Name: "user", Limit: 3, Window: time.Second, Burst: 3}
	l := NewLocalLimiter(rule)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if res := l.Allow(ctx, "u1"); !res.Allowed {
			t.Fatalf("第 %d 次应放行: %+v", i+1, res)
		}
	}

	res := l.Allow(ctx, "u1")
	if res.Allowed {
		t.Fatalf("第 4 次应被拒绝")
	}
	if res.RetryAfter <= 0 {
		t.Fatalf("被拒绝时应给出 RetryAfter, got %v", res.RetryAfter)
	}

	if other := l.Allow(ctx, "u2"); !other.Allowed {
		t.Fatalf("不同 key 之间不应互相影响")
	}
}

func TestLocalLimiterRefills(t *testing.T) {
	rule := Rule{Name: "user", Limit: 10, Window: 100 * time.Millisecond, Burst: 1}
	l := NewLocalLimiter(rule)
	ctx := context.Background()

	if res := l.Allow(ctx, "u1"); !res.Allowed {
		t.Fatalf("首次应放行")
	}
	if res := l.Allow(ctx, "u1"); res.Allowed {
		t.Fatalf("令牌耗尽后应拒绝")
	}

	time.Sleep(120 * time.Millisecond)
	if res := l.Allow(ctx, "u1"); !res.Allowed {
		t.Fatalf("补充令牌后应放行: %+v", res)
	}
}

func TestLocalLimiterEvictsWhenTooManyKeys(t *testing.T) {
	rule := Rule{Name: "ip", Limit: 1, Window: time.Millisecond, Burst: 1, MaxLocalKey: 8}
	l := NewLocalLimiter(rule)
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		l.Allow(ctx, string(rune('a'+i%26))+time.Now().String())
	}

	l.mu.Lock()
	size := len(l.buckets)
	l.mu.Unlock()
	if size > rule.MaxLocalKey {
		t.Fatalf("本地桶数量 %d 超过上限 %d", size, rule.MaxLocalKey)
	}
}

func TestRedisLimiterDegradesWithoutClient(t *testing.T) {
	l := NewRedisLimiter(nil, Rule{Name: "user", Limit: 1, Window: time.Second})
	res := l.Allow(context.Background(), "u1")
	if !res.Allowed || !res.Degraded {
		t.Fatalf("无 Redis 客户端时应降级放行: %+v", res)
	}
}

func TestResilientLimiterFallsBackWhenDegraded(t *testing.T) {
	primary := NewRedisLimiter(nil, Rule{Name: "user", Limit: 1, Window: time.Second, Burst: 1})
	fallback := NewLocalLimiter(Rule{Name: "user-local", Limit: 1, Window: time.Second, Burst: 1})

	l := NewResilientLimiter(primary, fallback)
	ctx := context.Background()

	first := l.Allow(ctx, "u1")
	if !first.Allowed || !first.Degraded {
		t.Fatalf("首次应降级放行: %+v", first)
	}
	second := l.Allow(ctx, "u1")
	if second.Allowed {
		t.Fatalf("降级后仍应受本地限流保护: %+v", second)
	}
}

func TestSetRejectsOnTightestDimension(t *testing.T) {
	tenantLimit := NewLocalLimiter(Rule{Name: "tenant", Limit: 100, Window: time.Second, Burst: 100})
	userLimit := NewLocalLimiter(Rule{Name: "user", Limit: 2, Window: time.Second, Burst: 2})

	set := NewSet(tenantLimit, userLimit)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if d := set.Allow(ctx, "k"); !d.Allowed {
			t.Fatalf("第 %d 次应放行: %+v", i+1, d)
		}
	}

	d := set.Allow(ctx, "k")
	if d.Allowed {
		t.Fatalf("应被最紧的维度拒绝")
	}
	if d.RejectedBy != "user" {
		t.Fatalf("RejectedBy = %s, want user", d.RejectedBy)
	}
}

func TestRuleNormalization(t *testing.T) {
	r := Rule{}.normalized()
	if r.Limit != 100 || r.Window != time.Second || r.Burst != 100 || r.Name != "default" {
		t.Fatalf("默认值异常: %+v", r)
	}
	if r.ratePerMilli() <= 0 {
		t.Fatalf("ratePerMilli 应为正数")
	}
}
