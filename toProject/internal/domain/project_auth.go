package domain

import (
	"context"
	"go.uber.org/zap"
	"strconv"
	"test.com/others/errs"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
	"time"
)

type ProjectAuthDomain struct {
	projectAuthRepo       repo.ProjectAuthRepo
	userRpcDomain         *UserRpcDomain
	projectNodeDomain     *ProjectNodeDomain
	projectAuthNodeDomain *ProjectAuthNodeDomain
	accountDomain         *AccountDomain
}

func (d *ProjectAuthDomain) AuthList(ctx context.Context, orgCode int64) ([]*data.ProjectAuthDisplay, *errs.MError) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	list, err := d.projectAuthRepo.FindAuthList(c, orgCode)
	if err != nil {
		zap.L().Error("[ProjectAuthDomain] FindAuthList error", zap.Error(err))
		return nil, model.DBError
	}

	res := make([]*data.ProjectAuthDisplay, 0, len(list))
	for _, v := range list {
		res = append(res, v.ToDisplay())
	}
	return res, nil
}

func (d *ProjectAuthDomain) AuthListPage(ctx context.Context, orgCode int64, page int64, pageSize int64) ([]*data.ProjectAuthDisplay, int64, *errs.MError) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	list, total, err := d.projectAuthRepo.FindAuthListPage(c, orgCode, page, pageSize)
	if err != nil {
		zap.L().Error("project AuthList projectAuthRepo.FindAuthList error", zap.Error(err))
		return nil, 0, model.DBError
	}
	var pdList []*data.ProjectAuthDisplay
	for _, v := range list {
		display := v.ToDisplay()
		pdList = append(pdList, display)
	}
	return pdList, total, nil
}

func (d *ProjectAuthDomain) AllNodeAndAuth(ctx context.Context, authId int64) ([]*data.ProjectNodeAuthTree, []string, *errs.MError) {
	nodeList, err := d.projectNodeDomain.NodeList(ctx)
	if err != nil {
		return nil, nil, err
	}
	checkedList, err := d.projectAuthNodeDomain.AuthNodeList(ctx, authId)
	if err != nil {
		return nil, nil, err
	}
	list := data.ToAuthNodeTreeList(nodeList, checkedList)
	return list, checkedList, nil
}

func (d *ProjectAuthDomain) Save(ctx context.Context, conn database.DbConn, authId int64, nodes []string) *errs.MError {
	err := d.projectAuthNodeDomain.Save(ctx, conn, authId, nodes)
	if err != nil {
		return err
	}
	return nil
}

func (d *ProjectAuthDomain) AuthNodes(ctx context.Context, emberId int64) ([]string, *errs.MError) {
	authId := int64(0)

	account, err := d.accountDomain.FindAccount(ctx, emberId)
	if err == nil && account != nil {
		if v, pErr := strconv.ParseInt(account.Authorize, 10, 64); pErr == nil && v > 0 {
			authId = v
		}
	}

	if authId == 0 {
		defaultId, dErr := d.projectAuthRepo.FindDefaultAuthId(ctx)
		if dErr != nil || defaultId <= 0 {
			adminId, aErr := d.projectAuthRepo.FindAdminAuthId(ctx)
			if aErr != nil || adminId <= 0 {
				return nil, model.ParamsError
			}
			authId = adminId
		} else {
			authId = defaultId
		}
	}

	authNodeList, dbErr := d.projectAuthNodeDomain.AuthNodeList(ctx, authId)
	if dbErr != nil {
		return nil, model.DBError
	}
	return authNodeList, nil
}

func NewProjectAuthDomain() *ProjectAuthDomain {
	return &ProjectAuthDomain{
		projectAuthRepo:       dao.NewProjectAuthDao(),
		userRpcDomain:         NewUserRpcDomain(),
		projectNodeDomain:     NewProjectNodeDomain(),
		projectAuthNodeDomain: NewProjectAuthNodeDomain(),
		accountDomain:         NewAccountDomain(),
	}
}
