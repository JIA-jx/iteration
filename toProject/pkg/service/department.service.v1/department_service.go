package department_service_v1

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redsync/redsync/v4"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
	"test.com/others/encrypts"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/department"
	"test.com/toProject/config"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/database/tran"
	"test.com/toProject/internal/domain"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
)

var departMentDetailLRU, _ = lru.NewARC(10000)

type DepartmentService struct {
	department.UnimplementedDepartmentServiceServer
	cache            repo.Cache
	transaction      tran.Transaction
	departmentDomain *domain.DepartmentDomain
}

func New() *DepartmentService {
	return &DepartmentService{
		cache:            dao.Rc,
		transaction:      dao.NewTransaction(),
		departmentDomain: domain.NewDepartmentDomain(),
	}
}

func (d *DepartmentService) List(ctx context.Context, msg *department.DepartmentReqMessage) (*department.ListDepartmentMessage, error) {
	organizationCode := encrypts.DecryptNoErr(msg.OrganizationCode)
	var parentDepartmentCode int64
	if msg.ParentDepartmentCode != "" {
		parentDepartmentCode = encrypts.DecryptNoErr(msg.ParentDepartmentCode)
	}

	cacheKey := fmt.Sprintf("department:list:%d:%d:%d:%d",
		organizationCode, parentDepartmentCode, msg.Page, msg.PageSize)

	if cacheData, ok := departMentDetailLRU.Get(cacheKey); ok {
		if res, ok := cacheData.(*department.ListDepartmentMessage); ok {
			return res, nil
		}
	}

	dps, total, err := d.departmentDomain.List(ctx, organizationCode, parentDepartmentCode, msg.Page, msg.PageSize)
	if err != nil {
		return nil, errs.GrpcError(err)
	}

	var list []*department.DepartmentMessage
	if err := copier.Copy(&list, dps); err != nil {
		return nil, errs.GrpcError(model.ParamsError)
	}

	res := &department.ListDepartmentMessage{
		List:  list,
		Total: total,
	}

	departMentDetailLRU.Add(cacheKey, res)

	zap.L().Info("DepartmentService.List",
		zap.Int64("organizationCode", organizationCode),
		zap.Int64("parentDepartmentCode", parentDepartmentCode),
		zap.Int64("page", int64(msg.Page)),
	)

	return res, nil
}
func (d *DepartmentService) Save(ctx context.Context, msg *department.DepartmentReqMessage) (*department.DepartmentMessage, error) {
	organizationCode := encrypts.DecryptNoErr(msg.OrganizationCode)
	var departmentCode int64
	if msg.DepartmentCode != "" {
		departmentCode = encrypts.DecryptNoErr(msg.DepartmentCode)
	}
	var parentDepartmentCode int64
	if msg.ParentDepartmentCode != "" {
		parentDepartmentCode = encrypts.DecryptNoErr(msg.ParentDepartmentCode)
	}

	lockKey := fmt.Sprintf("lock:department:save:%s:%s", msg.OrganizationCode, msg.Name)
	mutex := config.RedSync().NewMutex(lockKey,
		redsync.WithExpiry(10*time.Second),
	)
	if err := mutex.Lock(); err != nil {
		return &department.DepartmentMessage{}, errs.GrpcError(model.FrequentErr)
	}
	defer func() {
		_, _ = mutex.Unlock()
	}()

	var dp *data.DepartmentDisplay
	err := d.transaction.ActionWithCtx(ctx, func(txCtx context.Context, conn database.DbConn) error {
		var saveErr error
		dp, saveErr = d.departmentDomain.Save(
			txCtx,
			organizationCode,
			departmentCode,
			parentDepartmentCode,
			msg.Name,
		)
		return saveErr
	})
	if err != nil {
		return &department.DepartmentMessage{}, errs.GrpcError(model.TranErr)
	}

	if departMentDetailLRU != nil {
		departMentDetailLRU.Purge()
	}

	var res = &department.DepartmentMessage{}
	copier.Copy(res, dp)

	return res, nil
}

func (d *DepartmentService) Read(ctx context.Context, msg *department.DepartmentReqMessage) (*department.DepartmentMessage, error) {
	deptId := encrypts.DecryptNoErr(msg.DepartmentCode)

	cacheKey := fmt.Sprintf("dept:detail:%d", deptId)
	if val, ok := departMentDetailLRU.Get(cacheKey); ok {
		return val.(*department.DepartmentMessage), nil
	}

	var dp *data.Department
	err := d.transaction.ActionWithCtx(ctx, func(txCtx context.Context, conn database.DbConn) error {
		var findErr *errs.MError
		dp, findErr = d.departmentDomain.FindDepartmentById(
			txCtx,
			deptId,
			encrypts.DecryptNoErr(msg.OrganizationCode),
		)
		return findErr
	})
	if err != nil {
		return &department.DepartmentMessage{}, errs.GrpcError(model.ParamsError)
	}

	var res = &department.DepartmentMessage{}
	_ = copier.Copy(res, dp)

	departMentDetailLRU.Add(cacheKey, res)

	return res, nil
}
