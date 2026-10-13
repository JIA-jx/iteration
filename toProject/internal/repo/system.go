package repo

import (
	"context"
	"test.com/toProject/internal/data"
)

type SystemRepo interface {
	List(ctx context.Context) ([]*data.SystemNotify, error)
	Query(ctx context.Context, traceId, level string, elkAddr, elkIndex string) ([]*data.SystemLog, error)
	Clear(ctx context.Context) error
}
