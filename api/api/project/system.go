package project

import (
	"context"
	"github.com/gin-gonic/gin"
	"net/http"
	"test.com/api/api/rpc"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/system"
	"time"
)

type HandlerSystem struct{}

func NewSystem() *HandlerSystem {
	return &HandlerSystem{}
}

func (s *HandlerSystem) list(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	resp, err := rpc.SystemServiceClient.NotifyList(ctx, &system.NotifyListRequest{})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	c.JSON(http.StatusOK, result.Success(resp.List))
}

func (s *HandlerSystem) logQuery(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	req := &system.LogQueryRequest{}
	if err := c.ShouldBindJSON(req); err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "参数格式有误"))
		return
	}

	resp, err := rpc.SystemServiceClient.LogQuery(ctx, req)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	c.JSON(http.StatusOK, result.Success(resp.Logs))
}

func (s *HandlerSystem) reload(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	resp, err := rpc.SystemServiceClient.ConfigReload(ctx, &system.ConfigReloadRequest{})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	c.JSON(http.StatusOK, result.Success(resp.Msg))
}

func (s *HandlerSystem) clearCache(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	resp, err := rpc.SystemServiceClient.CacheClear(ctx, &system.CacheClearRequest{})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	c.JSON(http.StatusOK, result.Success(resp.Msg))
}
