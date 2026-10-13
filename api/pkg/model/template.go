package model

type CoverUploadReq struct {
	MemberId int64 `form:"memberId" binding:"required"`
}

type SaveTemplateReq struct {
	CoverKey    string `form:"coverKey" binding:"required"`
	Name        string `form:"name" binding:"required"`
	Description string `form:"description" binding:"required"`
}

type DeleteTemplateReq struct {
	Code string `form:"id" binding:"required"`
}
