package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"test.com/toProject/internal/repo"
)

type RedisLock struct {
	cache repo.Cache
	key   string
	token string
	ttl   time.Duration
}

func New(cache repo.Cache, key string, ttl time.Duration) *RedisLock {
	if ttl <= 0 {
		ttl = time.Minute
	}
	return &RedisLock{
		cache: cache,
		key:   "lock:job:" + key,
		token: newToken(),
		ttl:   ttl,
	}
}

func (l *RedisLock) TryAcquire(ctx context.Context) (bool, error) {
	if l == nil || l.cache == nil {
		return true, nil
	}
	ok, err := l.cache.SetNX(ctx, l.key, l.token, l.ttl)
	if err != nil {
		return false, err
	}
	return ok, nil
}

const releaseScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

func (l *RedisLock) Release(ctx context.Context) {
	if l == nil || l.cache == nil {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := l.cache.Eval(releaseCtx, releaseScript, []string{l.key}, l.token); err != nil {
		return
	}
}

func newToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return hex.EncodeToString([]byte(time.Now().String()))
	}
	return hex.EncodeToString(b)
}

var ErrLockBusy = errors.New("lock: job is already running on another instance")
