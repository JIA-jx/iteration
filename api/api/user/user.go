package user

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
	"net/http"
	"test.com/api/api/midd"
	"test.com/api/api/rpc"
	"test.com/api/pkg/model/user"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/others/jwts"
	"test.com/others/prom"
	"test.com/rpcOrgrpc/user/login"
	login_service_v1 "test.com/toUser/pkg/service/login.service.v1"
	"time"
)

type HandlerUser struct {
}

func NewHandler() *HandlerUser {
	return &HandlerUser{}
}

func (*HandlerUser) getCaptcha(ctx *gin.Context) {
	result := &common.Result{}
	mobile := ctx.PostForm("mobile")

	if err := login_service_v1.CaptchaLimit(ctx, mobile); err != nil {
		ctx.JSON(http.StatusOK, result.Fail(429, "获取过于频繁，请稍后再试"))
		return
	}

	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	rsp, err := rpc.LoginServiceClient.GetCaptcha(c, &login.CaptchaMessage{Mobile: mobile})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		ctx.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	ctx.JSON(http.StatusOK, result.Success(rsp.Code))
}

func (u *HandlerUser) register(c *gin.Context) {
	ctx := c.Request.Context()

	result := &common.Result{}
	var req user.RegisterReq
	err := c.ShouldBind(&req)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "参数格式有误"))
		return
	}

	if err := req.Verify(); err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, err.Error()))
		return
	}

	midd.AttachTenantCode(c, req.TenantCode)
	ctx = c.Request.Context()

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	msg := &login.RegisterMessage{}
	err = copier.Copy(msg, req)
	if err != nil {
		zap.L().Error("register copy error", zap.Error(err))
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "copy有误"))
		return
	}

	prom.ApiCounter.WithLabelValues("register", "total").Inc()

	_, err = rpc.LoginServiceClient.Register(ctx, msg)
	if err != nil {
		prom.ApiCounter.WithLabelValues("register", "fail").Inc()
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	prom.ApiCounter.WithLabelValues("register", "success").Inc()

	c.JSON(http.StatusOK, result.Success(""))
}

func GetIp(c *gin.Context) string {
	ip := c.ClientIP()
	if ip == "::1" {
		ip = "127.0.0.1"
	}
	return ip
}

func (u *HandlerUser) login(c *gin.Context) {
	result := &common.Result{}
	var req user.LoginReq
	err := c.ShouldBind(&req)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "参数格式有误"))
		return
	}

	ctx, cancel := context.WithTimeout(c, 2*time.Second)
	defer cancel()
	msg := &login.LoginMessage{}
	err = copier.Copy(msg, req)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "copy有误"))
		return
	}

	msg.Ip = GetIp(c)
	loginRsp, err := rpc.LoginServiceClient.Login(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	if loginRsp.Member == nil || loginRsp.Member.Id <= 0 {
		c.JSON(http.StatusOK, result.Fail(http.StatusInternalServerError, "登录信息异常"))
		return
	}

	accessToken := ""
	refreshToken := ""
	if loginRsp.TokenList != nil {
		accessToken = loginRsp.TokenList.AccessToken
		refreshToken = loginRsp.TokenList.RefreshToken
	}
	if accessToken == "" || refreshToken == "" {
		c.JSON(http.StatusOK, result.Fail(http.StatusInternalServerError, "登录信息异常"))
		return
	}

	rsp := &user.LoginRsp{}
	err = copier.Copy(rsp, loginRsp)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "copy有误"))
		return
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"user":          rsp,
		"access_token":  accessToken,
		"refresh_token": refreshToken,
	}))
}

func (u *HandlerUser) myOrgList(c *gin.Context) {
	result := &common.Result{}

	memberId := c.GetInt64("memberId")
	if memberId <= 0 {
		c.JSON(http.StatusOK, result.Fail(http.StatusUnauthorized, "未登录"))
		return
	}

	list, err2 := rpc.LoginServiceClient.MyOrgList(c.Request.Context(), &login.UserMessage{MemId: memberId})
	if err2 != nil {
		code, msg := errs.ParseGrpcError(err2)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	var orgs []*user.OrganizationList
	if list.OrganizationList != nil {
		_ = copier.Copy(&orgs, list.OrganizationList)
	}
	c.JSON(http.StatusOK, result.Success(orgs))
}

func (u *HandlerUser) refreshToken(c *gin.Context) {
	result := &common.Result{}

	refreshToken := c.GetHeader("Refresh-Token")
	if refreshToken == "" {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "缺少 refresh token"))
		return
	}

	publicKey, err := jwts.LoadRSAPublicKey("./conf/rsa/rsa_public_key.pem")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusInternalServerError, "公钥加载失败"))
		return
	}

	userId, tenantId, err := jwts.ParseRefreshTokenWithTenant(refreshToken, publicKey)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusUnauthorized, "refresh token 无效"))
		return
	}

	privateKey, err := jwts.LoadRSAPrivateKey("./conf/rsa/rsa_private_key.pem")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusInternalServerError, "生成token失败"))
		return
	}

	ip := GetIp(c)

	newJwtToken := jwts.CreateTokenWithTenant(
		userId,
		tenantId,
		2*time.Hour,
		privateKey,
		7*24*time.Hour,
		privateKey,
		ip,
	)
	if newJwtToken == nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusInternalServerError, "生成token失败"))
		return
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"access_token":  newJwtToken.AccessToken,
		"refresh_token": newJwtToken.RefreshToken,
	}))
}
