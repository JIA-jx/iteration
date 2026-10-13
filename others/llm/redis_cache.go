package llm

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisCache struct {
	rdb *redis.Client
	now func() time.Time
}

func NewRedisCache(rdb *redis.Client) *RedisCache {
	return &RedisCache{rdb: rdb, now: time.Now}
}

func (c *RedisCache) Get(ctx context.Context, key string) (string, bool, error) {
	if c.rdb == nil {
		return "", false, nil
	}
	opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	raw, err := c.rdb.Get(opCtx, key).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return raw, true, nil
}

func (c *RedisCache) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	if c.rdb == nil {
		return nil
	}
	opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.rdb.Set(opCtx, key, value, ttl).Err()
}
