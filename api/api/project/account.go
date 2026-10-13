package project

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
	"net/http"
	"test.com/api/api/rpc"
	"test.com/api/pkg/model"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/account"
	"time"
)

type HandlerAccount struct {
}

func (a *HandlerAccount) account(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	result := &common.Result{}
	var req model.AccountReq
	if err := c.ShouldBind(&req); err != nil {
		zap.L().Error("[Account] 参数绑定失败", zap.Error(err))
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 || req.PageSize > 100 {
		req.PageSize = 20
	}

	memberId := c.GetInt64("memberId")
	orgCode := OrganizationCode(c)

	msg := &account.AccountReqMessage{
		MemberId:         memberId,
		OrganizationCode: orgCode,
		Page:             int64(req.Page),
		PageSize:         int64(req.PageSize),
		SearchType:       int32(req.SearchType),
		DepartmentCode:   req.DepartmentCode,
	}

	response, err := rpc.AccountServiceClient.Account(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		zap.L().Error("[Account] gRPC调用失败")
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var list []*model.MemberAccount
	_ = copier.CopyWithOption(&list, response.AccountList, copier.Option{IgnoreEmpty: true, DeepCopy: true})
	if list == nil {
		list = make([]*model.MemberAccount, 0)
	}

	var authList []*model.ProjectAuth
	_ = copier.CopyWithOption(&authList, response.AuthList, copier.Option{IgnoreEmpty: true, DeepCopy: true})
	if authList == nil {
		authList = make([]*model.ProjectAuth, 0)
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"total":    response.Total,
		"page":     req.Page,
		"list":     list,
		"authList": authList,
	}))
}

func NewAccount() *HandlerAccount {
	return &HandlerAccount{}
}
