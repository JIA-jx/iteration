package repo

import (
	"context"
	"test.com/toProject/internal/data"
)

type TaskWorkTimeRepo interface {
	Save(ctx context.Context, twt *data.TaskWorkTime) error
	FindWorkTimeList(ctx context.Context, taskCode int64) (list []*data.TaskWorkTime, err error)
	TaskWorkTimeStat(ctx context.Context, taskCode int64) (stat *data.WorkTimeStat, err error)
}
