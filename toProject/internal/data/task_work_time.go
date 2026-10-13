package data

import (
	"github.com/jinzhu/copier"
	"test.com/others/encrypts"
	"test.com/others/tms"
)

type TaskWorkTime struct {
	Id         int64
	TaskCode   int64
	MemberCode int64
	CreateTime int64
	Content    string
	BeginTime  int64
	Num        int
	TenantId   int64 `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
}

type WorkTimeStat struct {
	TotalWorkTime int64 `gorm:"column:total_work_time"`
	AvgWorkTime   int64 `gorm:"column:avg_work_time"`
	MaxWorkTime   int64 `gorm:"column:max_work_time"`
	MinWorkTime   int64 `gorm:"column:min_work_time"`
}

func (*TaskWorkTime) TableName() string {
	return "iteration_task_work_time"
}

type TaskWorkTimeDisplay struct {
	Id         int64
	TaskCode   string
	MemberCode string
	CreateTime string
	Content    string
	BeginTime  string
	Num        int
	Member     Member
}

func (t *TaskWorkTime) ToDisplay() *TaskWorkTimeDisplay {
	td := &TaskWorkTimeDisplay{}
	copier.Copy(td, t)
	td.MemberCode = encrypts.EncryptNoErr(t.MemberCode)
	td.TaskCode = encrypts.EncryptNoErr(t.TaskCode)
	td.CreateTime = tms.FormatByMilli(t.CreateTime)
	td.BeginTime = tms.FormatByMilli(t.BeginTime)
	return td
}
