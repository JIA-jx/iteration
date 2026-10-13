package data

type TaskTag struct {
	Id          int64  `json:"id"`
	ProjectCode int64  `json:"project_code"`
	Name        string `json:"name"`
	Color       string `json:"color"`
	Sort        int    `json:"sort"`
	CreateBy    int64  `json:"create_by"`
	CreateTime  int64  `json:"create_time"`
	TenantId    int64  `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
}

func (*TaskTag) TableName() string {
	return "iteration_task_tag"
}

type TaskTagDisplay struct {
	Id         int64  `json:"id"`
	Code       string `json:"code"`
	Name       string `json:"name"`
	Color      string `json:"color"`
	Sort       int    `json:"sort"`
	CreateTime string `json:"create_time"`
}
