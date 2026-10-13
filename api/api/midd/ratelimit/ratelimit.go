package ratelimit

import (
	"sync"
	"time"
)

type Limiter interface {
	Allow() (bool, float64)
}

func NewLimiter(rule RuleConfig) Limiter {
	if rule.Algorithm == "leaky_bucket" {
		return NewLeakyBucket(rule.MaxTokens, rule.TokenRate)
	}
	return NewTokenBucket(rule.MaxTokens, rule.TokenRate)
}

func (tb *TokenBucket) Allow() (bool, float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastTime).Seconds()
	tb.tokens = tb.tokens + elapsed*float64(tb.rate)
	if tb.tokens > float64(tb.maxTokens) {
		tb.tokens = float64(tb.maxTokens)
	}
	tb.lastTime = now

	if tb.tokens >= 1 {
		tb.tokens -= 1
		usage := 1 - (tb.tokens / float64(tb.maxTokens))
		return true, usage
	}

	return false, 1.0
}

func (lb *LeakyBucket) Allow() (bool, float64) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(lb.lastTime).Seconds()
	lb.water = lb.water - elapsed*float64(lb.rate)
	if lb.water < 0 {
		lb.water = 0
	}
	lb.lastTime = now

	if lb.water < float64(lb.maxCapacity) {
		lb.water += 1
		usage := lb.water / float64(lb.maxCapacity)
		return true, usage
	}

	return false, 1.0
}

type TokenBucket struct {
	maxTokens int
	rate      int
	tokens    float64
	lastTime  time.Time
	mu        sync.Mutex
}

func NewTokenBucket(maxTokens, rate int) *TokenBucket {
	return &TokenBucket{
		maxTokens: maxTokens,
		rate:      rate,
		tokens:    float64(maxTokens),
		lastTime:  time.Now(),
	}
}

type LeakyBucket struct {
	maxCapacity int
	rate        int
	water       float64
	lastTime    time.Time
	mu          sync.Mutex
}

func NewLeakyBucket(maxCapacity, rate int) *LeakyBucket {
	return &LeakyBucket{
		maxCapacity: maxCapacity,
		rate:        rate,
		water:       0,
		lastTime:    time.Now(),
	}
}
