package repo

import (
	"context"

	"test.com/toProject/internal/data"
)

type DelayTaskRepo interface {
	Upsert(ctx context.Context, t *data.DelayTask) error

	UpdateStatus(ctx context.Context, taskId, status, lastError string, attempts int) error

	FindByTaskId(ctx context.Context, taskId string) (*data.DelayTask, error)

	List(ctx context.Context, taskType, status string, page, pageSize int64) ([]*data.DelayTask, int64, error)

	ListPendingReminders(ctx context.Context, limit int) ([]*data.Task, error)
}
