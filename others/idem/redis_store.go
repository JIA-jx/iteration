package idem

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	redisRecordPrefix = "idem:rec:"
	redisLockPrefix   = "idem:lock:"
	redisOpTimeout    = 2 * time.Second
)

type RedisStore struct {
	rdb *redis.Client
}

func NewRedisStore(rdb *redis.Client) *RedisStore {
	return &RedisStore{rdb: rdb}
}

func recordKey(key string) string { return redisRecordPrefix + key }
func lockKey(key string) string   { return redisLockPrefix + key }

func (s *RedisStore) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, redisOpTimeout)
}

func (s *RedisStore) TryAcquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if s.rdb == nil {
		return false, errors.New("idem: redis client is nil")
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()

	raw, err := s.rdb.Get(c, recordKey(key)).Result()
	if err == nil && raw != "" {
		var rec Record
		if json.Unmarshal([]byte(raw), &rec) == nil {
			if rec.Status == StatusSucceeded || rec.Status == StatusProcessing {
				return false, nil
			}
		}
	} else if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}

	return s.rdb.SetNX(c, lockKey(key), NewToken(), ttl).Result()
}

func (s *RedisStore) Get(ctx context.Context, key string) (*Record, error) {
	if s.rdb == nil {
		return nil, errors.New("idem: redis client is nil")
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()

	raw, err := s.rdb.Get(c, recordKey(key)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var rec Record
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *RedisStore) Complete(ctx context.Context, key, result string, ttl time.Duration) error {
	return s.put(ctx, &Record{
		Key:       key,
		Status:    StatusSucceeded,
		Result:    result,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		ExpireAt:  time.Now().Add(ttl),
	}, ttl)
}

func (s *RedisStore) Fail(ctx context.Context, key, reason string, ttl time.Duration) error {
	return s.put(ctx, &Record{
		Key:       key,
		Status:    StatusFailed,
		Reason:    reason,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		ExpireAt:  time.Now().Add(ttl),
	}, ttl)
}

func (s *RedisStore) put(ctx context.Context, rec *Record, ttl time.Duration) error {
	if s.rdb == nil {
		return errors.New("idem: redis client is nil")
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()

	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	pipe := s.rdb.Pipeline()
	pipe.Set(c, recordKey(rec.Key), body, ttl)
	pipe.Del(c, lockKey(rec.Key))
	_, err = pipe.Exec(c)
	return err
}

func (s *RedisStore) Release(ctx context.Context, key string) error {
	if s.rdb == nil {
		return errors.New("idem: redis client is nil")
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()
	return s.rdb.Del(c, lockKey(key)).Err()
}
