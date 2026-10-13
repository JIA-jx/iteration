package data

import "time"

type DelayTask struct {
	Id          int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	TaskId      string    `gorm:"column:task_id;size:64;not null;uniqueIndex" json:"task_id"`
	Type        string    `gorm:"column:type;size:64;not null;index" json:"type"`
	Payload     string    `gorm:"column:payload;type:text" json:"payload"`
	RunAt       int64     `gorm:"column:run_at;not null;index" json:"run_at"`
	Status      string    `gorm:"column:status;size:16;not null;default:'pending';index" json:"status"`
	Attempts    int       `gorm:"column:attempts;not null;default:0" json:"attempts"`
	MemberId    int64     `gorm:"column:member_id;not null;default:0;index" json:"member_id"`
	ProjectCode int64     `gorm:"column:project_code;not null;default:0;index" json:"project_code"`
	LastError   string    `gorm:"column:last_error;size:512" json:"last_error"`
	CreatedAt   time.Time `gorm:"column:created_at;not null" json:"created_at"`
	UpdatedAt   time.Time `gorm:"column:updated_at;not null" json:"updated_at"`
}

func (DelayTask) TableName() string {
	return "delay_task"
}
