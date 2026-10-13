package data

type TaskStages struct {
	Id          int    `json:"id"`
	Name        string `json:"name"`
	ProjectCode int64  `json:"project_code"`
	Sort        int    `json:"sort"`
	Description string `json:"description"`
	CreateTime  int64  `json:"create_time"`
	Deleted     int    `json:"deleted"`
	TenantId    int64  `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
}

func (*TaskStages) TableName() string {
	return "iteration_task_stages"
}

func ToTaskStagesMap(tsList []*TaskStages) map[int]*TaskStages {
	m := make(map[int]*TaskStages)
	for _, v := range tsList {
		m[v.Id] = v
	}
	return m
}
