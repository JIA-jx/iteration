package data

import (
	"github.com/jinzhu/copier"
	"test.com/others/encrypts"
	"test.com/others/tms"
)

type ProjectAuth struct {
	Id               int64  `json:"id"`
	OrganizationCode int64  `json:"organization_code"`
	Title            string `json:"title"`
	CreateAt         int64  `json:"create_at"`
	Sort             int    `json:"sort"`
	Status           int    `json:"status"`
	Desc             string `json:"desc"`
	CreateBy         int64  `json:"create_by"`
	IsDefault        int    `json:"is_default"`
	Type             string `json:"type"`
	TenantId         int64  `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
}

func (*ProjectAuth) TableName() string {
	return "iteration_auth"
}

func (a *ProjectAuth) ToDisplay() *ProjectAuthDisplay {
	p := &ProjectAuthDisplay{}
	copier.Copy(p, a)
	p.OrganizationCode = encrypts.EncryptNoErr(a.OrganizationCode)
	p.CreateAt = tms.FormatByMilli(a.CreateAt)
	if a.Type == "admin" || a.Type == "member" {
		p.CanDelete = 0
	} else {
		p.CanDelete = 1
	}
	return p
}

type ProjectAuthDisplay struct {
	Id               int64  `json:"id"`
	OrganizationCode string `json:"organization_code"`
	Title            string `json:"title"`
	CreateAt         string `json:"create_at"`
	Sort             int    `json:"sort"`
	Status           int    `json:"status"`
	Desc             string `json:"desc"`
	CreateBy         int64  `json:"create_by"`
	IsDefault        int    `json:"is_default"`
	Type             string `json:"type"`
	CanDelete        int    `json:"canDelete"`
}
