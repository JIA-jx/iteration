package model

import (
	"test.com/others/errs"
)

var (
	ParamsError        = errs.NewError(998, "参数校验错误")
	RedisError         = errs.NewError(999, "redis错误")
	DBError            = errs.NewError(998, "db错误")
	NoLogin            = errs.NewError(997, "未登录")
	TokenGenerateError = errs.NewError(996, "生成 TOKEN 失败")
	NoLegalMobile      = errs.NewError(10102001, "手机号不合法")
	CaptchaNotExist    = errs.NewError(10102002, "验证码不存在或者已过期")
	CaptchaError       = errs.NewError(10102003, "验证码错误")
	EmailExist         = errs.NewError(10102004, "邮箱已经存在了")
	AccountExist       = errs.NewError(10102005, "账号已经存在了")
	MobileExist        = errs.NewError(10102006, "手机号已经存在了")
	AccountAndPwdError = errs.NewError(10102007, "账号密码不正确")
	CaptchaLimit       = errs.NewError(10102008, "获取验证码操作频繁")
	InterfaceLimit     = errs.NewError(10102009, "接口请求频繁")
	RegisterRepeat     = errs.NewError(10102010, "重复提交注册")
	LoginFrequentError = errs.NewError(10102011, "登录频繁")
	TenantQuotaError   = errs.NewError(10102012, "租户配额已用尽")
)
