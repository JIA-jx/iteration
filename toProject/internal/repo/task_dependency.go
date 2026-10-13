package repo

import (
	"context"
	"test.com/toProject/internal/data"
)

type TaskDependencyRepo interface {
	Save(ctx context.Context, td *data.TaskDependency) error
	Delete(ctx context.Context, id int64) (int64, error)
	ListByProject(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error)
	ListByTask(ctx context.Context, taskCode int64) ([]*data.TaskDependency, error)
	DeleteByTask(ctx context.Context, taskCode int64) (int64, error)
	DeleteByTaskPair(ctx context.Context, projectCode, taskCode, preTaskCode int64) (int64, error)
}
