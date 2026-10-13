package account_service_v1

import (
	"context"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
	"test.com/others/encrypts"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/account"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/database/tran"
	"test.com/toProject/internal/domain"
	"test.com/toProject/internal/repo"
)

type AccountService struct {
	account.UnimplementedAccountServiceServer
	cache             repo.Cache
	transaction       tran.Transaction
	accountDomain     *domain.AccountDomain
	projectAuthDomain *domain.ProjectAuthDomain
}

func New() *AccountService {
	return &AccountService{
		cache:             dao.Rc,
		transaction:       dao.NewTransaction(),
		accountDomain:     domain.NewAccountDomain(),
		projectAuthDomain: domain.NewProjectAuthDomain(),
	}
}

func (a *AccountService) Account(ctx context.Context, msg *account.AccountReqMessage) (*account.AccountResponse, error) {
	accountList, total, err := a.accountDomain.AccountList(
		ctx,
		msg.OrganizationCode,
		msg.MemberId,
		msg.Page,
		msg.PageSize,
		msg.DepartmentCode,
		msg.SearchType,
	)
	if err != nil {
		zap.L().Error("[AccountService] AccountList查询失败", zap.Error(err))
		return nil, errs.GrpcError(err)
	}

	authList, err := a.projectAuthDomain.AuthList(
		ctx,
		encrypts.DecryptNoErr(msg.OrganizationCode),
	)
	if err != nil {
		zap.L().Error("[AccountService] AuthList查询失败", zap.Error(err))
		return nil, errs.GrpcError(err)
	}

	var maList []*account.MemberAccount
	_ = copier.Copy(&maList, accountList)
	var prList []*account.ProjectAuth
	_ = copier.Copy(&prList, authList)

	return &account.AccountResponse{
		AccountList: maList,
		AuthList:    prList,
		Total:       total,
	}, nil
}
