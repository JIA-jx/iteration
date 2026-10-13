package domain

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"test.com/others/encrypts"
	"test.com/others/errs"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
)

type AccountDomain struct {
	accountRepo      repo.AccountRepo
	userRpcDomain    *UserRpcDomain
	departmentDomain *DepartmentDomain
}

func NewAccountDomain() *AccountDomain {
	return &AccountDomain{
		accountRepo:      dao.NewMemberAccountDao(),
		userRpcDomain:    NewUserRpcDomain(),
		departmentDomain: NewDepartmentDomain(),
	}
}

func (d *AccountDomain) AccountList(ctx context.Context, organizationCode string, memberId, page, pageSize int64, departmentCode string, searchType int32) ([]*data.MemberAccountDisplay, int64, *errs.MError) {
	orgId := encrypts.DecryptNoErr(organizationCode)
	deptId := encrypts.DecryptNoErr(departmentCode)

	var condition string
	switch searchType {
	case 1:
		condition = "status = 1"
	case 2:
		condition = "department_code IS NULL"
	case 3:
		condition = "status = 0"
	case 4:
		condition = fmt.Sprintf("status = 1 AND department_code = %d", deptId)
	default:
		condition = "status = 1"
	}

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 200 {
		pageSize = 200
	}

	list, total, err := d.accountRepo.FindList(ctx, condition, orgId, deptId, page, pageSize)
	if err != nil {
		zap.L().Error("[AccountDomain] FindList查询失败", zap.Error(err))
		return nil, 0, model.DBError
	}

	memberIds := make([]int64, 0, len(list))
	for _, item := range list {
		if item.MemberCode > 0 {
			memberIds = append(memberIds, item.MemberCode)
		}
	}
	_, memberMap, _ := d.userRpcDomain.MemberList(ctx, memberIds)

	res := make([]*data.MemberAccountDisplay, 0, len(list))
	for _, item := range list {
		display := item.ToDisplay()

		if memberInfo := memberMap[item.MemberCode]; memberInfo != nil {
			display.Avatar = memberInfo.Avatar
		}

		if item.DepartmentCode > 0 {
			dept, err := d.departmentDomain.FindDepartmentById(ctx, item.DepartmentCode, 0)
			if err != nil {
				zap.L().Error("[AccountDomain] FindDepartmentById失败", zap.Error(err))
				return nil, 0, err
			}
			if dept != nil {
				display.Departments = dept.Name
			}
		}

		res = append(res, display)
	}

	return res, total, nil
}

func (d *AccountDomain) FindAccount(ctx context.Context, memberId int64) (*data.MemberAccount, *errs.MError) {
	account, err := d.accountRepo.FindByMemberId(ctx, memberId)
	if err != nil {
		return nil, model.DBError
	}
	return account, nil
}
