package data

import "time"

const (
	DependencyTypeFS = "FS"
	DependencyTypeSS = "SS"
	DependencyTypeFF = "FF"
	DependencyTypeSF = "SF"
)

type TaskDependency struct {
	Id          int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	ProjectCode int64     `gorm:"column:project_code;not null;default:0;index" json:"project_code"`
	TaskCode    int64     `gorm:"column:task_code;not null;default:0;index" json:"task_code"`
	PreTaskCode int64     `gorm:"column:pre_task_code;not null;default:0;index" json:"pre_task_code"`
	Type        string    `gorm:"column:type;size:8;not null;default:'FS'" json:"type"`
	Lag         int64     `gorm:"column:lag;not null;default:0" json:"lag"`
	TenantId    int64     `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
	CreatedAt   time.Time `gorm:"column:created_at;not null" json:"created_at"`
}

func (TaskDependency) TableName() string {
	return "task_dependency"
}
