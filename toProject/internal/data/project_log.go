package data

import (
	"github.com/jinzhu/copier"
	"test.com/others/encrypts"
	"test.com/others/tms"
)

type ProjectLog struct {
	Id           int64
	MemberCode   int64
	Content      string
	Remark       string
	Type         string
	CreateTime   int64
	SourceCode   int64
	ActionType   string
	ToMemberCode int64
	IsComment    int
	ProjectCode  int64
	Icon         string
	IsRobot      int
	TenantId     int64 `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
}

func (*ProjectLog) TableName() string {
	return "iteration_log"
}

type ProjectLogDisplay struct {
	Id           int64
	MemberCode   string
	Content      string
	Remark       string
	Type         string
	CreateTime   string
	SourceCode   string
	ActionType   string
	ToMemberCode string
	IsComment    int
	ProjectCode  string
	Icon         string
	IsRobot      int
	Member       Member
}

func (l *ProjectLog) ToDisplay() *ProjectLogDisplay {
	pd := &ProjectLogDisplay{}
	copier.Copy(pd, l)
	pd.MemberCode = encrypts.EncryptNoErr(l.MemberCode)
	pd.ToMemberCode = encrypts.EncryptNoErr(l.ToMemberCode)
	pd.ProjectCode = encrypts.EncryptNoErr(l.ProjectCode)
	pd.CreateTime = tms.FormatByMilli(l.CreateTime)
	pd.SourceCode = encrypts.EncryptNoErr(l.SourceCode)
	return pd
}

type IndexProjectLogDisplay struct {
	Content      string
	Remark       string
	CreateTime   string
	SourceCode   string
	IsComment    int
	ProjectCode  string
	MemberAvatar string
	MemberName   string
	ProjectName  string
	TaskName     string
}

func (l *ProjectLog) ToIndexDisplay() *IndexProjectLogDisplay {
	pd := &IndexProjectLogDisplay{}
	copier.Copy(pd, l)
	pd.ProjectCode = encrypts.EncryptNoErr(l.ProjectCode)
	pd.CreateTime = tms.FormatByMilli(l.CreateTime)
	pd.SourceCode = encrypts.EncryptNoErr(l.SourceCode)
	return pd
}
