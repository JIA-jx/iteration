package config

import (
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"test.com/toProject/internal/dao"
)

var redSync *redsync.Redsync

func (c *Config) ReConnRedis() {
	rdb := redis.NewClient(c.ReadRedisConfig())
	rc := &dao.RedisCache{
		Rdb: rdb,
	}
	dao.Rc = rc

	pool := goredis.NewPool(rdb)
	redSync = redsync.New(pool)
}

func RedSync() *redsync.Redsync {
	return redSync
}
