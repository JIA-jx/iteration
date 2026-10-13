package ratelimit

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type Rule struct {
	Name        string
	Limit       int64
	Window      time.Duration
	Burst       int64
	MaxLocalKey int
}

func (r Rule) normalized() Rule {
	if r.Limit <= 0 {
		r.Limit = 100
	}
	if r.Window <= 0 {
		r.Window = time.Second
	}
	if r.Window < time.Millisecond {
		r.Window = time.Millisecond
	}
	if r.Burst <= 0 {
		r.Burst = r.Limit
	}
	if r.MaxLocalKey <= 0 {
		r.MaxLocalKey = 100000
	}
	if r.Name == "" {
		r.Name = "default"
	}
	return r
}

func (r Rule) ratePerMilli() float64 {
	windowMs := r.Window.Milliseconds()
	if windowMs < 1 {
		windowMs = 1
	}
	return float64(r.Limit) / float64(windowMs)
}

type Result struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
	Degraded   bool
}

type Limiter interface {
	Allow(ctx context.Context, key string) Result
	Name() string
}

var ErrNilClient = errors.New("ratelimit: redis client is nil")

type localBucket struct {
	tokens float64
	ts     time.Time
}

type LocalLimiter struct {
	rule Rule

	mu      sync.Mutex
	buckets map[string]*localBucket
	now     func() time.Time
}

func NewLocalLimiter(rule Rule) *LocalLimiter {
	return &LocalLimiter{
		rule:    rule.normalized(),
		buckets: make(map[string]*localBucket),
		now:     time.Now,
	}
}

func (l *LocalLimiter) Name() string { return l.rule.Name }

func (l *LocalLimiter) Allow(ctx context.Context, key string) Result {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= l.rule.MaxLocalKey {
			l.evictLocked(now)
		}
		b = &localBucket{tokens: float64(l.rule.Burst), ts: now}
		l.buckets[key] = b
	}

	delta := now.Sub(b.ts).Seconds()
	if delta > 0 {
		b.tokens += delta * float64(l.rule.Limit) / l.rule.Window.Seconds()
		if b.tokens > float64(l.rule.Burst) {
			b.tokens = float64(l.rule.Burst)
		}
	}
	b.ts = now

	if b.tokens >= 1 {
		b.tokens--
		return Result{Allowed: true, Remaining: int64(b.tokens)}
	}

	need := 1 - b.tokens
	retry := time.Duration(need / (float64(l.rule.Limit) / l.rule.Window.Seconds()) * float64(time.Second))
	if retry <= 0 {
		retry = time.Millisecond
	}
	return Result{Allowed: false, Remaining: 0, RetryAfter: retry}
}

func (l *LocalLimiter) evictLocked(now time.Time) {
	idle := l.rule.Window * 2
	for k, b := range l.buckets {
		if now.Sub(b.ts) > idle {
			delete(l.buckets, k)
		}
	}
	if len(l.buckets) < l.rule.MaxLocalKey {
		return
	}

	type bucketAge struct {
		key string
		ts  time.Time
	}
	ages := make([]bucketAge, 0, len(l.buckets))
	for k, b := range l.buckets {
		ages = append(ages, bucketAge{key: k, ts: b.ts})
	}
	sort.Slice(ages, func(i, j int) bool { return ages[i].ts.Before(ages[j].ts) })

	target := l.rule.MaxLocalKey / 2
	for i := 0; i < len(ages) && len(l.buckets) > target; i++ {
		delete(l.buckets, ages[i].key)
	}
}

type ResilientLimiter struct {
	primary  Limiter
	fallback Limiter
}

func NewResilientLimiter(primary, fallback Limiter) *ResilientLimiter {
	return &ResilientLimiter{primary: primary, fallback: fallback}
}

func (l *ResilientLimiter) Name() string {
	if l.primary != nil {
		return l.primary.Name()
	}
	if l.fallback != nil {
		return l.fallback.Name()
	}
	return "noop"
}

func (l *ResilientLimiter) Allow(ctx context.Context, key string) Result {
	if l.primary == nil {
		return l.allowFallback(ctx, key)
	}

	res := l.primary.Allow(ctx, key)
	if res.Degraded {
		return l.allowFallback(ctx, key)
	}
	return res
}

func (l *ResilientLimiter) allowFallback(ctx context.Context, key string) Result {
	if l.fallback == nil {
		return Result{Allowed: true, Degraded: true}
	}
	res := l.fallback.Allow(ctx, key)
	res.Degraded = true
	return res
}

type Set struct {
	limiters []Limiter
}

func NewSet(limiters ...Limiter) *Set {
	return &Set{limiters: limiters}
}

func (s *Set) Add(l Limiter) {
	if l != nil {
		s.limiters = append(s.limiters, l)
	}
}

type Decision struct {
	Allowed    bool
	RejectedBy string
	Remaining  int64
	RetryAfter time.Duration
	Degraded   bool
}

func (s *Set) Allow(ctx context.Context, key string) Decision {
	d := Decision{Allowed: true}
	for _, l := range s.limiters {
		res := l.Allow(ctx, key)
		if res.Degraded {
			d.Degraded = true
		}
		if !res.Allowed {
			return Decision{
				Allowed:    false,
				RejectedBy: l.Name(),
				Remaining:  res.Remaining,
				RetryAfter: res.RetryAfter,
				Degraded:   res.Degraded,
			}
		}
		if d.Remaining == 0 || res.Remaining < d.Remaining {
			d.Remaining = res.Remaining
		}
	}
	return d
}
