package auth_service_v1

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
	"test.com/others/encrypts"
	"test.com/others/errs"
	"test.com/others/kk"
	workflow "test.com/others/wkflow"
	"test.com/rpcOrgrpc/auth"
	"test.com/toProject/config"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/database/tran"
	"test.com/toProject/internal/domain"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
)

var authFlow *workflow.AuthApplyFlow
var authFlowOnce sync.Once
var authDetailLRU, _ = lru.NewARC(10000)

func getAuthFlow() *workflow.AuthApplyFlow {
	authFlowOnce.Do(func() {
		authFlow = workflow.NewAuthApplyFlow(config.C.KafkaConfig.Brokers[0])
	})
	return authFlow
}

type AuthService struct {
	auth.UnimplementedAuthServiceServer
	cache             repo.Cache
	transaction       tran.Transaction
	projectAuthDomain *domain.ProjectAuthDomain
	kafka             *kk.KafkaWriter
}

func New() *AuthService {
	return &AuthService{
		cache:             dao.Rc,
		transaction:       dao.NewTransaction(),
		projectAuthDomain: domain.NewProjectAuthDomain(),
		kafka:             kk.GetWriter("127.0.0.1:9092"),
	}
}

func (a *AuthService) AuthList(ctx context.Context, msg *auth.AuthReqMessage) (*auth.ListAuthMessage, error) {
	organizationCode := encrypts.DecryptNoErr(msg.OrganizationCode)

	cacheKey := fmt.Sprintf("auth:list:%d:%d:%d", organizationCode, msg.Page, msg.PageSize)
	if val, ok := authDetailLRU.Get(cacheKey); ok {
		return val.(*auth.ListAuthMessage), nil
	}

	listPage, total, err := a.projectAuthDomain.AuthListPage(
		ctx,
		organizationCode,
		msg.Page,
		msg.PageSize,
	)
	if err != nil {
		return nil, errs.GrpcError(err)
	}

	var prList []*auth.ProjectAuth
	if err := copier.Copy(&prList, listPage); err != nil {
		return nil, errs.GrpcError(model.ParamsError)
	}

	resp := &auth.ListAuthMessage{
		List:  prList,
		Total: total,
	}

	authDetailLRU.Add(cacheKey, resp)

	return resp, nil
}

func (a *AuthService) Apply(ctx context.Context, msg *auth.AuthReqMessage) (*auth.ApplyResponse, error) {
	organizationCode := encrypts.DecryptNoErr(msg.OrganizationCode)
	if organizationCode <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}
	roles, roleErr := a.projectAuthDomain.AuthList(ctx, organizationCode)
	if roleErr != nil {
		zap.L().Error("Apply AuthList error", zap.Int64("org", organizationCode), zap.Error(roleErr))
		return nil, errs.GrpcError(model.DBError)
	}
	inOrg := false
	for _, r := range roles {
		if r != nil && r.Id == msg.AuthId {
			inOrg = true
			break
		}
	}
	if !inOrg {
		zap.L().Warn("Apply rejected: role not in caller organization",
			zap.Int64("auth_id", msg.AuthId), zap.Int64("org", organizationCode))
		return nil, errs.GrpcError(model.BusinessError)
	}

	if msg.Action == "getnode" {
		cacheKey := fmt.Sprintf("auth:node:%d", msg.AuthId)
		if cached, err := a.cache.Get(ctx, cacheKey); err == nil && cached != "" {
			var resp auth.ApplyResponse
			if json.Unmarshal([]byte(cached), &resp) == nil {
				return &resp, nil
			}
		}

		list, checkedList, err := a.projectAuthDomain.AllNodeAndAuth(ctx, msg.AuthId)
		if err != nil {
			return nil, errs.GrpcError(err)
		}

		var prList []*auth.ProjectNodeMessage
		copier.Copy(&prList, list)
		resp := &auth.ApplyResponse{List: prList, CheckedList: checkedList}
		if body, mErr := json.Marshal(resp); mErr == nil {
			_ = a.cache.Put(ctx, cacheKey, string(body), 30*time.Second)
		}

		return resp, nil
	}

	if msg.Action == "save" {
		lockKey := fmt.Sprintf("lock:auth:save:%d", msg.AuthId)
		mutex := config.RedSync().NewMutex(lockKey, redsync.WithExpiry(30*time.Second))
		if err := mutex.Lock(); err != nil {
			return nil, errs.GrpcError(model.FrequentErr)
		}
		defer mutex.Unlock()

		authid := strconv.Itoa(int(msg.AuthId))
		if err := getAuthFlow().Submit(ctx, authid, msg.Nodes); err != nil {
			return nil, errs.GrpcError(model.BusinessError)
		}

		err := getAuthFlow().Approve(ctx, authid, msg.Nodes, func(ctx context.Context) error {
			return a.transaction.Action(func(conn database.DbConn) error {
				return a.projectAuthDomain.Save(ctx, conn, msg.AuthId, msg.Nodes)
			})
		})
		if err != nil {
			return nil, errs.GrpcError(model.BusinessError)
		}

		authDetailLRU.Purge()
		_, _ = a.cache.Del(ctx, fmt.Sprintf("auth:node:%d", msg.AuthId))

		return &auth.ApplyResponse{}, nil
	}
	return &auth.ApplyResponse{}, nil
}

func (a *AuthService) AuthNodesByMemberId(ctx context.Context, msg *auth.AuthReqMessage) (*auth.AuthNodesResponse, error) {
	list, err := a.projectAuthDomain.AuthNodes(ctx, msg.MemberId)
	if err != nil {
		return nil, errs.GrpcError(err)
	}
	return &auth.AuthNodesResponse{List: list}, nil
}
