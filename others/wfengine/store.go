package wfengine

import "context"

type Store interface {
	Create(ctx context.Context, inst *Instance) error
	Get(ctx context.Context, id string) (*Instance, error)
	Update(ctx context.Context, id string, expectedVersion int64, mutate func(*Instance) error) error
	ListByBiz(ctx context.Context, tenantId int64, bizId string) ([]*Instance, error)
	ListPendingTasks(ctx context.Context, approverId int64, tenantId int64) ([]*Task, error)
}
