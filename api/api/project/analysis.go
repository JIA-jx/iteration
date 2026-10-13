package project

import (
	"context"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"net/http"
	"test.com/api/api/rpc"
	"test.com/api/pkg/model"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/analysis"
	"time"
)

type HandlerAnalysis struct{}

func (als *HandlerAnalysis) aiRecommend(c *gin.Context) {
	result := &common.Result{}
	req := model.AnalysisReq{}
	c.ShouldBind(&req)

	if req.TaskCode == "" {
		zap.L().Warn("aiRecommend param error: taskCode is empty")
		c.JSON(http.StatusOK, result.Fail(-999, "taskCode不能为空"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	resp, err := rpc.AnalysisServiceClient.AIRecommend(ctx, &analysis.AnalysisReq{
		TaskCode: req.TaskCode,
		MemberId: c.GetInt64("memberId"),
	})
	if err != nil {
		zap.L().Error("project analysis aiRecommend AnalysisServiceClient.AIRecommend error", zap.Error(err))
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(resp))
}

func (als *HandlerAnalysis) delivery(c *gin.Context) {
	result := &common.Result{}
	req := model.AnalysisReq{}
	c.ShouldBind(&req)

	if req.ProjectCode == "" {
		zap.L().Warn("delivery param error: projectCode is empty")
		c.JSON(http.StatusOK, result.Fail(-999, "projectCode不能为空"))
		return
	}
	if req.BeginTime == 0 || req.EndTime == 0 || req.BeginTime > req.EndTime {
		zap.L().Warn("delivery param error: beginTime or endTime is invalid")
		c.JSON(http.StatusOK, result.Fail(-999, "时间参数无效"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	resp, err := rpc.AnalysisServiceClient.DeliveryRate(ctx, &analysis.AnalysisReq{
		ProjectCode: req.ProjectCode,
		BeginTime:   req.BeginTime,
		EndTime:     req.EndTime,
		MemberId:    c.GetInt64("memberId"),
	})
	if err != nil {
		zap.L().Error("project analysis delivery AnalysisServiceClient.DeliveryRate error", zap.Error(err))
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(resp))
}

func (als *HandlerAnalysis) burndown(c *gin.Context) {
	result := &common.Result{}
	req := model.AnalysisReq{}
	c.ShouldBind(&req)

	if req.ProjectCode == "" {
		zap.L().Warn("burndown param error: projectCode is empty")
		c.JSON(http.StatusOK, result.Fail(-999, "projectCode不能为空"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	resp, err := rpc.AnalysisServiceClient.Burndown(ctx, &analysis.AnalysisReq{
		ProjectCode: req.ProjectCode,
		MemberId:    c.GetInt64("memberId"),
		BeginTime:   req.BeginTime,
		EndTime:     req.EndTime,
	})
	if err != nil {
		zap.L().Error("project analysis burndown AnalysisServiceClient.Burndown error", zap.Error(err))
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(resp))
}

func (als *HandlerAnalysis) teamLoad(c *gin.Context) {
	result := &common.Result{}
	req := model.AnalysisReq{}
	c.ShouldBind(&req)

	if req.ProjectCode == "" {
		zap.L().Warn("teamLoad param error: projectCode is empty")
		c.JSON(http.StatusOK, result.Fail(-999, "projectCode不能为空"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	resp, err := rpc.AnalysisServiceClient.TeamLoad(ctx, &analysis.AnalysisReq{
		ProjectCode: req.ProjectCode,
		MemberId:    c.GetInt64("memberId"),
		BeginTime:   req.BeginTime,
		EndTime:     req.EndTime,
	})
	if err != nil {
		zap.L().Error("project analysis teamLoad AnalysisServiceClient.TeamLoad error", zap.Error(err))
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(resp))
}

func NewAnalysis() *HandlerAnalysis {
	return &HandlerAnalysis{}
}
