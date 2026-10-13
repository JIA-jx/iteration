package dao

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"test.com/toUser/config"
	"time"
)

var Rc *RedisCache
var redSync *redsync.Redsync

type RedisCache struct {
	Rdb *redis.Client
}

func init() {
	rdb := redis.NewClient(config.C.ReadRedisConfig())
	Rc = &RedisCache{
		Rdb: rdb,
	}
	pool := goredis.NewPool(rdb)
	redSync = redsync.New(pool)
}

func (rc *RedisCache) GetClient() *redis.Client {
	return rc.Rdb
}

func (rc *RedisCache) Put(ctx context.Context, key, value string, expire time.Duration) error {
	return rc.Rdb.Set(ctx, key, value, expire).Err()
}
func (rc *RedisCache) Get(ctx context.Context, key string) (string, error) {
	return rc.Rdb.Get(ctx, key).Result()
}

func (rc *RedisCache) Exist(ctx context.Context, key string) bool {
	return rc.Rdb.Exists(ctx, key).Val() > 0
}

func (rc *RedisCache) Del(ctx context.Context, key string) error {
	return rc.Rdb.Del(ctx, key).Err()
}

func (rc *RedisCache) SetExpire(ctx context.Context, key string, expire time.Duration) error {
	return rc.Rdb.Expire(ctx, key, expire).Err()
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
		if s, ok := v.(string); ok {
			result[unique[i]] = s
		}
	}
	return result, nil
}

func (rc *RedisCache) PutMultiMemberAndOrg(
	ctx context.Context,
	members map[string]string, memberExpire time.Duration,
	orgs map[string]string, orgExpire time.Duration,
) error {
	if len(members) == 0 && len(orgs) == 0 {
		return nil
	}

	pipe := rc.Rdb.Pipeline()
	for k, v := range members {
		if k == "" {
			continue
		}
		pipe.Set(ctx, k, v, memberExpire)
	}
	for k, v := range orgs {
		if k == "" {
			continue
		}
		pipe.Set(ctx, k, v, orgExpire)
	}
	_, err := pipe.Exec(ctx)
	return err
}
