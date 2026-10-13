package project

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"
	"net/http"
	"test.com/api/api/rpc"
	"test.com/api/pkg/model"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/department"
	"time"
)

type HandlerTeam struct {
}

func (d *HandlerTeam) department(c *gin.Context) {
	result := &common.Result{}

	var req *model.DepartmentReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "参数错误: "+err.Error()))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &department.DepartmentReqMessage{
		Page:                 req.Page,
		PageSize:             req.PageSize,
		ParentDepartmentCode: req.Pcode,
		OrganizationCode:     OrganizationCode(c),
	}

	listDepartmentMessage, err := rpc.DepartmentServiceClient.List(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var list []*model.Department
	copier.Copy(&list, listDepartmentMessage.List)
	if list == nil {
		list = []*model.Department{}
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"total": listDepartmentMessage.Total,
		"page":  req.Page,
		"list":  list,
	}))
}

func (d *HandlerTeam) save(c *gin.Context) {
	result := &common.Result{}

	var req *model.DepartmentReq
	if err := c.ShouldBind(&req); err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &department.DepartmentReqMessage{
		Name:                 req.Name,
		DepartmentCode:       req.DepartmentCode,
		ParentDepartmentCode: req.ParentDepartmentCode,
		OrganizationCode:     OrganizationCode(c),
	}

	departmentMessage, err := rpc.DepartmentServiceClient.Save(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var res = new(model.Department)
	if err := copier.Copy(res, departmentMessage); err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(res))
}

func (d *HandlerTeam) read(c *gin.Context) {
	result := &common.Result{}

	departmentCode := c.PostForm("departmentCode")
	if departmentCode == "" {
		c.JSON(http.StatusOK, result.Fail(400, "departmentCode 不能为空"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &department.DepartmentReqMessage{
		DepartmentCode:   departmentCode,
		OrganizationCode: OrganizationCode(c),
	}

	departmentMessage, err := rpc.DepartmentServiceClient.Read(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var res = &model.Department{}
	if err := copier.Copy(res, departmentMessage); err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "数据转换失败"))
		return
	}

	c.JSON(http.StatusOK, result.Success(res))
}

func NewTeam() *HandlerTeam {
	return &HandlerTeam{}
}
