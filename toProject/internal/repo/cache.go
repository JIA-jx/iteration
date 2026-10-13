package repo

import (
	"context"
	"time"
)

type Cache interface {
	Put(ctx context.Context, key string, value any, expire time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	HSet(ctx context.Context, key string, field string, value string)
	HKeys(ctx context.Context, key string) ([]string, error)
	Delete(background context.Context, keys []string)
	HSetMap(ctx context.Context, key string, data map[string]interface{}) error
	Keys(ctx context.Context, key string) ([]string, error)
	Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error)
	SetNX(ctx context.Context, key, value string, expire time.Duration) (bool, error)

	GetMulti(ctx context.Context, keys []string) (map[string]string, error)

	PutMulti(ctx context.Context, items map[string]string, expire time.Duration) error

	Del(ctx context.Context, keys ...string) (int64, error)

	SAdd(ctx context.Context, key string, members ...string) (int64, error)
	SMembers(ctx context.Context, key string) ([]string, error)

	Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error)

	ZAdd(ctx context.Context, key string, score float64, member string) error
	ZRem(ctx context.Context, key string, members ...string) (int64, error)
	ZRangeByScore(ctx context.Context, key string, min, max string, offset, count int64) ([]string, error)
	ZCard(ctx context.Context, key string) (int64, error)
	HGet(ctx context.Context, key, field string) (string, error)
	HDel(ctx context.Context, key string, fields ...string) (int64, error)
}
