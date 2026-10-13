package repo

import (
	"context"
	"time"
)

type Cache interface {
	Put(ctx context.Context, key, value string, expire time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	Exist(ctx context.Context, key string) bool
	Del(ctx context.Context, key string) error
	SetExpire(ctx context.Context, key string, expire time.Duration) error
	SetNX(ctx context.Context, key, value string, expire time.Duration) (bool, error)

	GetMulti(ctx context.Context, keys []string) (map[string]string, error)

	PutMultiMemberAndOrg(
		ctx context.Context,
		members map[string]string, memberExpire time.Duration,
		orgs map[string]string, orgExpire time.Duration,
	) error
}
