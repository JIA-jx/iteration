package dao

import (
	"context"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type ProjectAuthDao struct {
	conn *gorms.GormConn
}

func (p *ProjectAuthDao) FindAuthListPage(ctx context.Context, orgCode int64, page int64, pageSize int64) (list []*data.ProjectAuth, total int64, err error) {
	session := p.conn.Slave().WithContext(ctx)
	err = session.Model(&data.ProjectAuth{}).
		Where("organization_code=?", orgCode).
		Limit(int(pageSize)).
		Offset(int((page - 1) * pageSize)).
		Find(&list).Error
	err = session.Model(&data.ProjectAuth{}).
		Where("organization_code=?", orgCode).
		Count(&total).Error
	return
}

func (p *ProjectAuthDao) FindAuthList(ctx context.Context, orgCode int64) (list []*data.ProjectAuth, err error) {
	session := p.conn.Slave().WithContext(ctx)
	err = session.Model(&data.ProjectAuth{}).Where("organization_code=?", orgCode).Find(&list).Error
	return
}

func (p *ProjectAuthDao) FindDefaultAuthId(ctx context.Context) (int64, error) {
	var auth data.ProjectAuth
	err := p.conn.Slave().WithContext(ctx).
		Model(&data.ProjectAuth{}).
		Where("is_default = ? AND status = ?", 1, 1).
		Order("id ASC").
		First(&auth).Error
	if err != nil {
		return 0, err
	}
	return auth.Id, nil
}

func (p *ProjectAuthDao) FindAdminAuthId(ctx context.Context) (int64, error) {
	var auth data.ProjectAuth
	err := p.conn.Slave().WithContext(ctx).
		Model(&data.ProjectAuth{}).
		Where("type = ? AND status = ?", "admin", 1).
		Order("id ASC").
		First(&auth).Error
	if err != nil {
		return 0, err
	}
	return auth.Id, nil
}

func NewProjectAuthDao() *ProjectAuthDao {
	return &ProjectAuthDao{
		conn: gorms.New(),
	}
}
