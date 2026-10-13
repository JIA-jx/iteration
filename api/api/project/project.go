package project

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strconv"
	"test.com/api/api/rpc"
	"test.com/api/pkg/model"
	"test.com/api/pkg/model/pro"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/project"
	"time"
)

type HandlerProject struct {
}

func (p *HandlerProject) index(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &project.IndexMessage{}
	indexResponse, err := rpc.ProjectServiceClient.Index(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	menus := indexResponse.Menus
	var ms []*model.Menu
	copier.Copy(&ms, menus)
	c.JSON(http.StatusOK, result.Success(ms))
}

func (p *HandlerProject) myProjectList(c *gin.Context) {
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	memberId := c.GetInt64("memberId")
	memberName := c.GetString("memberName")

	page := &model.Page{}
	page.Bind(c)
	selectBy := c.PostForm("selectBy")

	msg := &project.ProjectRpcMessage{
		MemberId:   memberId,
		MemberName: memberName,
		SelectBy:   selectBy,
		Page:       page.Page,
		PageSize:   page.PageSize,
	}

	myProjectResponse, err := rpc.ProjectServiceClient.FindProjectByMemId(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var pms []*pro.ProjectAndMember
	copier.Copy(&pms, myProjectResponse.Pm)
	if pms == nil {
		pms = []*pro.ProjectAndMember{}
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  pms,
		"total": myProjectResponse.Total,
	}))
}

func (p *HandlerProject) projectTemplate(c *gin.Context) {
	result := &common.Result{}
	memberId := c.GetInt64("memberId")
	memberName := c.GetString("memberName")
	organizationCode := OrganizationCode(c)

	page := &model.Page{}
	page.Bind(c)

	viewTypeStr := c.PostForm("viewType")
	viewType, err := strconv.ParseInt(viewTypeStr, 10, 64)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &project.ProjectRpcMessage{
		MemberId:         memberId,
		MemberName:       memberName,
		ViewType:         int32(viewType),
		Page:             page.Page,
		PageSize:         page.PageSize,
		OrganizationCode: organizationCode,
	}
	templateResp, err := rpc.ProjectServiceClient.FindProjectTemplate(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var projectTemplates []*pro.ProjectTemplate
	_ = copier.Copy(&projectTemplates, templateResp.Ptm)

	if projectTemplates == nil {
		projectTemplates = []*pro.ProjectTemplate{}
	}
	for _, item := range projectTemplates {
		if item.TaskStages == nil {
			item.TaskStages = []*pro.TaskStagesOnlyName{}
		}
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  projectTemplates,
		"total": templateResp.Total,
	}))
}

func (p *HandlerProject) projectSave(c *gin.Context) {
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	memberId := c.GetInt64("memberId")
	organizationCode := OrganizationCode(c)
	if memberId <= 0 || organizationCode == "" {
		err := errors.New("解析数据错误，用户或组织信息异常")
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var req *pro.SaveProjectRequest
	if err := c.ShouldBind(&req); err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	msg := &project.ProjectRpcMessage{
		MemberId:         memberId,
		OrganizationCode: organizationCode,
		TemplateCode:     req.TemplateCode,
		Name:             req.Name,
		Id:               int64(req.Id),
		Description:      req.Description,
	}
	saveProject, err := rpc.ProjectServiceClient.SaveProject(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	var rsp *pro.SaveProject
	copier.Copy(&rsp, saveProject)
	c.JSON(http.StatusOK, result.Success(rsp))
}

func (p *HandlerProject) readProject(c *gin.Context) {
	result := &common.Result{}

	projectCode := c.PostForm("projectCode")
	memberId := c.GetInt64("memberId")

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	detail, err := rpc.ProjectServiceClient.FindProjectDetail(ctx, &project.ProjectRpcMessage{
		ProjectCode: projectCode,
		MemberId:    memberId,
	})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	pd := &pro.ProjectDetail{}
	copier.Copy(pd, detail)
	c.JSON(http.StatusOK, result.Success(pd))
}

func (p *HandlerProject) recycleProject(c *gin.Context) {
	result := &common.Result{}
	projectCode := c.PostForm("projectCode")
	memberId := c.GetInt64("memberId")

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	_, err := rpc.ProjectServiceClient.UpdateDeletedProject(ctx, &project.ProjectRpcMessage{
		ProjectCode: projectCode,
		MemberId:    memberId,
		Deleted:     true,
	})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success([]int{}))
}

func (p *HandlerProject) recoveryProject(c *gin.Context) {
	result := &common.Result{}
	projectCode := c.PostForm("projectCode")
	memberId := c.GetInt64("memberId")

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	_, err := rpc.ProjectServiceClient.UpdateDeletedProject(ctx, &project.ProjectRpcMessage{
		ProjectCode: projectCode,
		MemberId:    memberId,
		Deleted:     false,
	})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	c.JSON(http.StatusOK, result.Success([]int{}))
}

func (p *HandlerProject) collectProject(c *gin.Context) {
	result := &common.Result{}
	projectCode := c.PostForm("projectCode")
	collectType := c.PostForm("type")
	memberId := c.GetInt64("memberId")

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	_, err := rpc.ProjectServiceClient.UpdateCollectProject(ctx, &project.ProjectRpcMessage{
		ProjectCode: projectCode,
		CollectType: collectType,
		MemberId:    memberId,
	})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	c.JSON(http.StatusOK, result.Success([]int{}))
}

func (p *HandlerProject) editProject(c *gin.Context) {
	result := &common.Result{}
	var req pro.ProjectReq

	if err := c.ShouldBind(&req); err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	memberId := c.GetInt64("memberId")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &project.UpdateProjectMessage{}
	copier.Copy(msg, &req)
	msg.MemberId = memberId

	_, err := rpc.ProjectServiceClient.UpdateProject(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	c.JSON(http.StatusOK, result.Success([]int{}))
}

func (p *HandlerProject) getLogBySelfProject(c *gin.Context) {
	result := &common.Result{}
	var page = &model.Page{}
	page.Bind(c)

	msg := &project.ProjectRpcMessage{
		MemberId: c.GetInt64("memberId"),
		Page:     page.Page,
		PageSize: page.PageSize,
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	projectLogResponse, err := rpc.ProjectServiceClient.GetLogBySelfProject(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var list []*model.ProjectLog
	if projectLogResponse.List != nil {
		_ = copier.Copy(&list, projectLogResponse.List)
	}
	if list == nil {
		list = []*model.ProjectLog{}
	}

	c.JSON(http.StatusOK, result.Success(list))
}

func (p *HandlerProject) nodeList(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	response, err := rpc.ProjectServiceClient.NodeList(ctx, &project.ProjectRpcMessage{})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	var list []*model.ProjectNodeTree
	copier.Copy(&list, response.Nodes)
	c.JSON(http.StatusOK, result.Success(gin.H{
		"nodes": list,
	}))
}

func (p *HandlerProject) uploadCover(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	memberId := c.GetInt64("memberId")

	fileHeader, err := c.FormFile("cover")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "请上传封面图片"))
		return
	}
	if fileHeader.Size > 5<<20 {
		c.JSON(http.StatusOK, result.Fail(400, "图片不能超过5MB"))
		return
	}

	file, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "文件打开失败"))
		return
	}
	defer file.Close()

	fileData, err := io.ReadAll(io.LimitReader(file, 5<<20+1))
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "文件读取失败"))
		return
	}

	if len(fileData) > 5<<20 {
		c.JSON(http.StatusOK, result.Fail(400, "图片不能超过5MB"))
		return
	}

	req := &project.CoverUploadRequest{
		MemberId: memberId,
		FileData: fileData,
		FileName: fileHeader.Filename,
	}

	resp, err := rpc.ProjectServiceClient.UploadCover(ctx, req)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"coverKey": resp.CoverKey,
	}))
}

func (p *HandlerProject) saveTemplate(c *gin.Context) {
	result := &common.Result{}

	memberId := c.GetInt64("memberId")

	name := c.PostForm("name")
	description := c.PostForm("description")
	coverKey := c.PostForm("coverKey")
	if name == "" || description == "" || coverKey == "" {
		c.JSON(http.StatusOK, result.Fail(400, "参数不完整"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	if name == "" || description == "" || coverKey == "" {
		c.JSON(http.StatusOK, result.Fail(400, "参数不完整"))
		return
	}

	msg := &project.SaveTemplateRequest{
		CoverKey:    coverKey,
		Name:        name,
		Description: description,
		MemberId:    memberId,
	}

	resp, err := rpc.ProjectServiceClient.SaveTemplate(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(resp))
}

func (p *HandlerProject) deleteTemplate(c *gin.Context) {
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	var req model.DeleteTemplateReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "参数错误: "+err.Error()))
		return
	}

	msg := &project.DeleteTemplateRequest{
		Code: req.Code,
	}
	resp, err := rpc.ProjectServiceClient.DeleteTemplate(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(resp))
}

func (p *HandlerProject) recycleList(c *gin.Context) {
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	var req project.RecycleBinListRequest
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "参数错误: "+err.Error()))
		return
	}

	msg := &project.RecycleBinListRequest{
		Page:     req.Page,
		PageSize: req.PageSize,
	}

	response, err := rpc.ProjectServiceClient.GetRecycleBinList(ctx, msg)
	if err != nil {
		code, ms := errs.ParseGrpcError(err)
		zap.L().Error("回收站列表获取失败", zap.Error(err))
		c.JSON(http.StatusOK, result.Fail(code, ms))
		return
	}

	var resp []*pro.ProjectResp
	copier.Copy(&resp, response.List)
	if resp == nil {
		resp = []*pro.ProjectResp{}
	}

	zap.L().Info("回收站列表获取成功", zap.Int64("total", response.Total))
	c.JSON(http.StatusOK, result.Success(resp))
}

func New() *HandlerProject {
	return &HandlerProject{}
}
