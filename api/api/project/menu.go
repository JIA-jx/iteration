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
	"test.com/rpcOrgrpc/menu"
	"time"
)

type HandlerMenu struct {
}

func (m HandlerMenu) menuList(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	res, err := rpc.MenuServiceClient.MenuList(ctx, &menu.MenuReqMessage{})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var list []*model.Menu
	copier.Copy(&list, res.List)

	if list == nil {
		list = []*model.Menu{}
	}

	c.JSON(http.StatusOK, result.Success(list))
}

func NewMenu() *HandlerMenu {
	return &HandlerMenu{}
}
