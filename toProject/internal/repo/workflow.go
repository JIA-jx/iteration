package repo

import (
	"context"

	"test.com/toProject/internal/data"
)

type WorkflowDefinitionRepo interface {
	Save(ctx context.Context, def *data.WorkflowDefinition) error
	FindByKeyVersion(ctx context.Context, tenantId int64, defKey string, version int) (*data.WorkflowDefinition, error)
	FindLatestEnabled(ctx context.Context, tenantId int64, defKey string) (*data.WorkflowDefinition, error)
	List(ctx context.Context, tenantId int64, defKey string, page, pageSize int64) ([]*data.WorkflowDefinition, int64, error)
	NextVersion(ctx context.Context, tenantId int64, defKey string) (int, error)
	DisableAll(ctx context.Context, tenantId int64, defKey string) error
}
