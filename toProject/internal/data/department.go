package data

import (
	"github.com/jinzhu/copier"
	"test.com/others/encrypts"
	"test.com/others/tms"
)

type Department struct {
	Id               int64
	OrganizationCode int64
	Name             string
	Sort             int
	Pcode            int64
	Icon             string
	CreateTime       int64
	Path             string
	TenantId         int64 `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
}

func (*Department) TableName() string {
	return "iteration_department"
}

type DepartmentDisplay struct {
	Id               int64
	Code             string
	OrganizationCode string
	Name             string
	Sort             int
	Pcode            string
	Icon             string
	CreateTime       string
	Path             string
}

func (d *Department) ToDisplay() *DepartmentDisplay {
	dp := &DepartmentDisplay{}
	copier.Copy(dp, d)
	dp.Code = encrypts.EncryptNoErr(d.Id)
	dp.CreateTime = tms.FormatByMilli(d.CreateTime)
	dp.OrganizationCode = encrypts.EncryptNoErr(d.OrganizationCode)
	if d.Pcode > 0 {
		dp.Pcode = encrypts.EncryptNoErr(d.Pcode)
	} else {
		dp.Pcode = ""
	}
	return dp
}
