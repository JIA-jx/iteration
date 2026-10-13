package repo

import (
	"context"
	"test.com/toProject/internal/data"
)

type DepartmentRepo interface {
	FindDepartmentById(ctx context.Context, id int64, orgCode int64) (*data.Department, error)
	FindDepartment(ctx context.Context, organizationCode int64, parentDepartmentCode int64, name string) (*data.Department, error)
	Save(c context.Context, dpm *data.Department) error
	ListDepartment(c context.Context, organizationCode int64, parentDepartmentCode int64, page int64, size int64) (list []*data.Department, total int64, err error)
}
