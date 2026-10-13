package dao

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"test.com/toUser/internal/data/tenant"
	"test.com/toUser/internal/database/gorms"
)

type TenantDao struct {
	conn *gorms.GormConn
}

func (t *TenantDao) FindByCode(ctx context.Context, code string) (*tenant.Tenant, error) {
	var row tenant.Tenant
	err := t.conn.Slave().WithContext(ctx).
		Where("code = ?", code).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func NewTenantDao() *TenantDao {
	return &TenantDao{conn: gorms.New()}
}
