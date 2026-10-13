package project

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"
	"net/http"
	"test.com/api/api/rpc"
	"test.com/api/pkg/model"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/auth"
	"time"
)

type HandlerAuth struct {
}

func (a *HandlerAuth) authList(c *gin.Context) {
	result := &common.Result{}
	organizationCode := OrganizationCode(c)

	var page = &model.Page{}
	page.Bind(c)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &auth.AuthReqMessage{
		OrganizationCode: organizationCode,
		Page:             page.Page,
		PageSize:         page.PageSize,
	}

	response, err := rpc.AuthServiceClient.AuthList(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var authList []*model.ProjectAuth
	err = copier.Copy(&authList, response.List)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "数据转换失败"))
		return
	}
	if authList == nil {
		authList = []*model.ProjectAuth{}
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"total": response.Total,
		"list":  authList,
		"page":  page.Page,
	}))
}

func (a *HandlerAuth) apply(c *gin.Context) {
	result := &common.Result{}

	var req *model.ProjectAuthReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "参数错误"))
		return
	}

	if !CallerHasNode(c, "project/auth/apply") {
		c.JSON(http.StatusOK, result.Fail(403, "无权限查看或修改角色权限节点"))
		return
	}

	var nodes []string
	if req.Nodes != "" {
		if err := json.Unmarshal([]byte(req.Nodes), &nodes); err != nil {
			c.JSON(http.StatusOK, result.Fail(400, "nodes格式错误"))
			return
		}
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &auth.AuthReqMessage{
		Action:           req.Action,
		AuthId:           req.Id,
		Nodes:            nodes,
		MemberId:         c.GetInt64("memberId"),
		OrganizationCode: OrganizationCode(c),
	}

	applyResponse, err := rpc.AuthServiceClient.Apply(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var list []*model.ProjectNodeAuthTree
	copier.Copy(&list, applyResponse.List)
	var checkedList []string
	copier.Copy(&checkedList, applyResponse.CheckedList)

	PurgeNodeCache(c.GetInt64("memberId"))

	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":        list,
		"checkedList": checkedList,
	}))
}

func (a *HandlerAuth) GetAuthNodes(c *gin.Context) ([]string, error) {
	memberId := c.GetInt64("memberId")
	msg := &auth.AuthReqMessage{
		MemberId: memberId,
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	response, err := rpc.AuthServiceClient.AuthNodesByMemberId(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		return nil, errs.NewError(errs.ErrorCode(code), msg)
	}
	return response.List, err
}

func NewAuth() *HandlerAuth {
	return &HandlerAuth{}
}
