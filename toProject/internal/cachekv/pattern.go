package cachekv

import (
	"context"
	"time"

	"go.uber.org/zap"
	"test.com/toProject/internal/repo"
)

func InvalidatePattern(ctx context.Context, cache repo.Cache, patterns ...string) int {
	if cache == nil || len(patterns) == 0 {
		return 0
	}

	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	deleted := 0
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		keys, err := cache.Keys(opCtx, pattern)
		if err != nil {
			zap.L().Warn("cache pattern keys failed", zap.String("pattern", pattern), zap.Error(err))
			continue
		}
		if len(keys) == 0 {
			continue
		}
		n, dErr := cache.Del(opCtx, keys...)
		if dErr != nil {
			zap.L().Warn("cache pattern del failed", zap.String("pattern", pattern), zap.Error(dErr))
			continue
		}
		deleted += int(n)
	}

	return deleted
}
