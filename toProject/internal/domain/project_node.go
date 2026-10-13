package domain

import (
	"context"
	"test.com/others/errs"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
)

type ProjectNodeDomain struct {
	projectNodeRepo repo.ProjectNodeRepo
}

func (d *ProjectNodeDomain) TreeList(ctx context.Context) ([]*data.ProjectNodeTree, *errs.MError) {
	list, err := d.projectNodeRepo.FindAll(ctx)
	if err != nil {
		return nil, model.DBError
	}
	return data.ToNodeTreeList(list), nil
}

func (d *ProjectNodeDomain) NodeList(ctx context.Context) ([]*data.ProjectNode, *errs.MError) {
	list, err := d.projectNodeRepo.FindAll(ctx)
	if err != nil {
		return nil, model.DBError
	}
	return list, nil
}

func NewProjectNodeDomain() *ProjectNodeDomain {
	return &ProjectNodeDomain{
		projectNodeRepo: dao.NewProjectNodeDao(),
	}
}
