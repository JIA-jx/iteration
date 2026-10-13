package repo

import (
	"context"

	"test.com/toProject/internal/data"
)

type TenantRepo interface {
	FindById(ctx context.Context, id int64) (*data.Tenant, error)
	List(ctx context.Context) ([]*data.Tenant, error)
	Save(ctx context.Context, t *data.Tenant) error
	AddAIUsage(ctx context.Context, id int64, tokens int64) error
}
