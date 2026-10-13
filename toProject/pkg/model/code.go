package model

import (
	"test.com/others/errs"
)

var (
	DBError               = errs.NewError(998, "db错误")
	LogError              = errs.NewError(997, "log错误")
	CacheClearError       = errs.NewError(995, "cacheClear错误")
	RedisError            = errs.NewError(996, "redis错误")
	ParamsError           = errs.NewError(401, "参数错误")
	FailToDel             = errs.NewError(2000, "删除失败")
	TaskNameNotNull       = errs.NewError(2001, "任务标题不能为空")
	TaskStagesNotNull     = errs.NewError(2002, "任务步骤不存在")
	ProjectAlreadyDeleted = errs.NewError(2003, "项目已经删除了")
	BusinessError         = errs.NewError(2004, "业务错误")
	NotFoundError         = errs.NewError(2005, "缓存未命中")
	OnlyImage             = errs.NewError(2006, "只支持图片类型")
	UploadImageFail       = errs.NewError(2007, "图片上传失败")
	FrequentErr           = errs.NewError(2008, "操作频繁")
	GetOrgErr             = errs.NewError(2009, "获取组织失败")
	SaveTempErr           = errs.NewError(2010, "报错模板失败")
	TempEmpCode           = errs.NewError(2011, "模板编码不能为空")
	TranErr               = errs.NewError(2012, "事务错误")
	OptimisticLockError   = errs.NewError(2013, "乐观锁错误")
	TaskNotFound          = errs.NewError(2014, "查询任务失败")
	AIErr                 = errs.NewError(2015, "AI分析异常")
	UploadChunkFail       = errs.NewError(2016, "分片上传失败")
	ChunkMissing          = errs.NewError(2017, "分片缺失，无法合并")
)
