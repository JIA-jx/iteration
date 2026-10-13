package ratelimit

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

const tokenBucketScript = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local vals = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(vals[1])
local ts = tonumber(vals[2])

if tokens == nil then tokens = burst end
if ts == nil then ts = now end

local delta = now - ts
if delta < 0 then delta = 0 end

tokens = tokens + delta * rate
if tokens > burst then tokens = burst end

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', key, ttl)

local retry = 0
if allowed == 0 then
  if rate > 0 then
    retry = math.ceil((1 - tokens) / rate)
  else
    retry = ttl
  end
end

return {allowed, math.floor(tokens), retry}
`

const keyPrefix = "rl:"

type RedisLimiter struct {
	rdb  *redis.Client
	rule Rule
	now  func() time.Time
}

func NewRedisLimiter(rdb *redis.Client, rule Rule) *RedisLimiter {
	return &RedisLimiter{rdb: rdb, rule: rule.normalized(), now: time.Now}
}

func (l *RedisLimiter) Name() string { return l.rule.Name }

func (l *RedisLimiter) key(key string) string {
	return keyPrefix + l.rule.Name + ":" + key
}

func (l *RedisLimiter) Allow(ctx context.Context, key string) Result {
	if l.rdb == nil {
		return Result{Allowed: true, Degraded: true}
	}

	ttl := l.rule.Window * 2
	if ttl <= 0 {
		ttl = time.Second
	}

	raw, err := l.rdb.Eval(ctx, tokenBucketScript,
		[]string{l.key(key)},
		l.now().UnixMilli(),
		l.rule.ratePerMilli(),
		l.rule.Burst,
		ttl.Milliseconds(),
	).Result()
	if err != nil {
		return Result{Allowed: true, Degraded: true}
	}

	items, ok := raw.([]interface{})
	if !ok || len(items) < 3 {
		return Result{Allowed: true, Degraded: true}
	}

	allowed := toInt64(items[0]) == 1
	remaining := toInt64(items[1])
	retryMs := toInt64(items[2])
	if retryMs < 0 {
		retryMs = 0
	}

	return Result{
		Allowed:    allowed,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryMs) * time.Millisecond,
	}
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}
