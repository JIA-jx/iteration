package repo

import (
	"context"
	"test.com/toProject/internal/data"
)

type SourceLinkRepo interface {
	Save(ctx context.Context, link *data.SourceLink) error
	FindByTaskCode(ctx context.Context, taskCode int64) (list []*data.SourceLink, err error)
	FindById(ctx context.Context, id int64) (*data.SourceLink, error)
	DeleteById(ctx context.Context, id int64) (int64, error)
}
