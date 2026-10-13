package dao

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

var Rc *RedisCache

type RedisCache struct {
	Rdb *redis.Client
}

func (rc *RedisCache) Put(ctx context.Context, key string, value any, expire time.Duration) error {
	err := rc.Rdb.Set(ctx, key, value, expire).Err()
	return err
}
func (rc *RedisCache) Get(ctx context.Context, key string) (string, error) {
	result, err := rc.Rdb.Get(ctx, key).Result()
	return result, err
}
func (rc *RedisCache) HSet(ctx context.Context, key string, field string, value string) {
	rc.Rdb.HSet(ctx, key, field, value)
}

func (rc *RedisCache) HKeys(ctx context.Context, key string) ([]string, error) {
	result, err := rc.Rdb.HKeys(ctx, key).Result()
	return result, err
}

func (rc *RedisCache) Keys(ctx context.Context, key string) ([]string, error) {
	return rc.Rdb.Keys(ctx, key).Result()
}

func (rc *RedisCache) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return rc.Rdb.Scan(ctx, cursor, match, count).Result()
}

func (rc *RedisCache) Delete(ctx context.Context, keys []string) {
	rc.Rdb.Del(ctx, keys...)
}

func (rc *RedisCache) HSetMap(ctx context.Context, key string, data map[string]interface{}) error {
	if len(data) == 0 {
		return nil
	}
	return rc.Rdb.HSet(ctx, key, data).Err()
}

func (rc *RedisCache) SetNX(ctx context.Context, key, value string, expire time.Duration) (bool, error) {
	return rc.Rdb.SetNX(ctx, key, value, expire).Result()
}

func (rc *RedisCache) GetMulti(ctx context.Context, keys []string) (map[string]string, error) {
	result := make(map[string]string, len(keys))
	if len(keys) == 0 {
		return result, nil
	}

	unique := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		unique = append(unique, k)
	}
	if len(unique) == 0 {
		return result, nil
	}

	values, err := rc.Rdb.MGet(ctx, unique...).Result()
	if err != nil {
		return result, err
	}

	for i, v := range values {
		if i >= len(unique) || v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		result[unique[i]] = s
	}
	return result, nil
}

func (rc *RedisCache) PutMulti(ctx context.Context, items map[string]string, expire time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	pipe := rc.Rdb.Pipeline()
	for k, v := range items {
		if k == "" {
			continue
		}
		pipe.Set(ctx, k, v, expire)
	}
	_, err := pipe.Exec(ctx)

	return err
}

func (rc *RedisCache) Del(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return rc.Rdb.Del(ctx, keys...).Result()
}

func (rc *RedisCache) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	args := make([]interface{}, 0, len(members))
	for _, m := range members {
		args = append(args, m)
	}
	return rc.Rdb.SAdd(ctx, key, args...).Result()
}

func (rc *RedisCache) SMembers(ctx context.Context, key string) ([]string, error) {
	return rc.Rdb.SMembers(ctx, key).Result()
}

func (rc *RedisCache) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return rc.Rdb.Eval(ctx, script, keys, args...).Result()
}

func (rc *RedisCache) ZAdd(ctx context.Context, key string, score float64, member string) error {
	return rc.Rdb.ZAdd(ctx, key, &redis.Z{Score: score, Member: member}).Err()
}

func (rc *RedisCache) ZRem(ctx context.Context, key string, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	args := make([]interface{}, 0, len(members))
	for _, m := range members {
		args = append(args, m)
	}
	return rc.Rdb.ZRem(ctx, key, args...).Result()
}

func (rc *RedisCache) ZRangeByScore(ctx context.Context, key string, min, max string, offset, count int64) ([]string, error) {
	return rc.Rdb.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: offset,
		Count:  count,
	}).Result()
}

func (rc *RedisCache) ZCard(ctx context.Context, key string) (int64, error) {
	return rc.Rdb.ZCard(ctx, key).Result()
}

func (rc *RedisCache) HGet(ctx context.Context, key, field string) (string, error) {
	return rc.Rdb.HGet(ctx, key, field).Result()
}

func (rc *RedisCache) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	return rc.Rdb.HDel(ctx, key, fields...).Result()
}
