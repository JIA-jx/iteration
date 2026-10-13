package domain

import (
	"context"
	"test.com/others/errs"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
	"time"
)

type DepartmentDomain struct {
	departmentRepo repo.DepartmentRepo
}

func (d *DepartmentDomain) FindDepartmentById(ctx context.Context, id int64, orgCode int64) (*data.Department, *errs.MError) {
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	dp, err := d.departmentRepo.FindDepartmentById(c, id, orgCode)
	if err != nil {
		return nil, model.DBError
	}
	return dp, nil
}

func (d *DepartmentDomain) List(ctx context.Context, organizationCode int64, parentDepartmentCode int64, page int64, size int64) ([]*data.DepartmentDisplay, int64, *errs.MError) {
	list, total, err := d.departmentRepo.ListDepartment(ctx, organizationCode, parentDepartmentCode, page, size)
	if err != nil {
		return nil, 0, model.DBError
	}
	var dList []*data.DepartmentDisplay
	for _, v := range list {
		dList = append(dList, v.ToDisplay())
	}
	return dList, total, nil
}

func (d *DepartmentDomain) Save(ctx context.Context, organizationCode, departmentCode, parentDepartmentCode int64, name string) (*data.DepartmentDisplay, *errs.MError) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	dpm, err := d.departmentRepo.FindDepartment(c, organizationCode, parentDepartmentCode, name)
	if err != nil {
		return nil, model.DBError
	}
	if dpm == nil {
		dpm = &data.Department{
			Name:             name,
			OrganizationCode: organizationCode,
			CreateTime:       time.Now().UnixMilli(),
		}
		if parentDepartmentCode > 0 {
			dpm.Pcode = parentDepartmentCode
		}
		err := d.departmentRepo.Save(c, dpm)
		if err != nil {
			return nil, model.DBError
		}
		return dpm.ToDisplay(), nil
	}
	return dpm.ToDisplay(), nil
}

func NewDepartmentDomain() *DepartmentDomain {
	return &DepartmentDomain{
		departmentRepo: dao.NewDepartmentDao(),
	}
}
