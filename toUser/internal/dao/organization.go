package dao

import (
	"context"
	"test.com/toUser/internal/data/organization"
	"test.com/toUser/internal/database"
	"test.com/toUser/internal/database/gorms"
)

type OrganizationDao struct {
	conn *gorms.GormConn
}

func (o *OrganizationDao) FindOrganizationByMemId(ctx context.Context, memId int64) ([]*organization.Organization, error) {
	var orgs []*organization.Organization
	err := o.conn.Slave().WithContext(ctx).
		Where("member_id = ?", memId).
		Find(&orgs).Error

	return orgs, err
}

func NewOrganizationDao() *OrganizationDao {
	return &OrganizationDao{
		conn: gorms.New(),
	}
}

func (o *OrganizationDao) SaveOrganization(conn database.DbConn, ctx context.Context, org *organization.Organization) error {
	return conn.(*gorms.GormConn).Tx(ctx).Create(org).Error
}

func (o *OrganizationDao) FindOrgListByMemberId(ctx context.Context, memberId int64) ([]*organization.Organization, error) {
	var list []*organization.Organization

	err := o.conn.Slave().WithContext(ctx).
		Model(&organization.Organization{}).
		Where("member_id = ?", memberId).
		Find(&list).Error

	return list, err
}
