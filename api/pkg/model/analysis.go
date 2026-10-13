package model

type AnalysisReq struct {
	ProjectCode string `json:"projectCode" form:"projectCode"`
	TaskCode    string `json:"taskCode" form:"taskCode"`
	BeginTime   int64  `json:"beginTime" form:"beginTime"`
	EndTime     int64  `json:"endTime" form:"endTime"`
}
