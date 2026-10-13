package cachekv

import (
	"context"
	"time"

	"go.uber.org/zap"
	"test.com/toProject/internal/repo"
)

func InvalidateGroup(ctx context.Context, cache repo.Cache, group string) int {
	if group == "" || cache == nil {
		return 0
	}

	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	idxKey := GroupIndex(group)

	keys, err := cache.SMembers(opCtx, idxKey)
	if err != nil {
		zap.L().Warn("cache index smembers failed", zap.String("group", group), zap.Error(err))
		return 0
	}

	n := 0
	if len(keys) > 0 {
		if deleted, dErr := cache.Del(opCtx, keys...); dErr != nil {
			zap.L().Warn("cache invalidate del failed", zap.String("group", group), zap.Error(dErr))
		} else {
			n = int(deleted)
		}
	}

	if _, dErr := cache.Del(opCtx, idxKey); dErr != nil {
		zap.L().Warn("cache index del failed", zap.String("group", group), zap.Error(dErr))
	}

	zap.L().Info("cache group invalidated", zap.String("group", group), zap.Int("deleted", n))
	return n
}
