package repo

import (
	"context"
	"test.com/toProject/internal/data"
)

type ProjectNodeRepo interface {
	FindAll(ctx context.Context) (list []*data.ProjectNode, err error)
}
