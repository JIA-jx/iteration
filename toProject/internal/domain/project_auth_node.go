package domain

import (
	"context"
	"test.com/others/errs"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
)

type ProjectAuthNodeDomain struct {
	projectAuthNodeRepo repo.ProjectAuthNodeRepo
}

func NewProjectAuthNodeDomain() *ProjectAuthNodeDomain {
	return &ProjectAuthNodeDomain{
		projectAuthNodeRepo: dao.NewProjectAuthNodeDao(),
	}
}

func (d *ProjectAuthNodeDomain) AuthNodeList(ctx context.Context, authId int64) ([]string, *errs.MError) {
	list, err := d.projectAuthNodeRepo.FindNodeStringList(ctx, authId)
	if err != nil {
		return nil, model.DBError
	}
	return list, nil
}

func (d *ProjectAuthNodeDomain) Save(ctx context.Context, conn database.DbConn, authId int64, nodes []string) *errs.MError {
	err := d.projectAuthNodeRepo.DeleteByAuthId(ctx, conn, authId)
	if err != nil {
		return model.DBError
	}
	err = d.projectAuthNodeRepo.Save(ctx, conn, authId, nodes)
	if err != nil {
		return model.DBError
	}
	return nil
}
