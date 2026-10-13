package dao

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
	"test.com/toProject/internal/repo"
)

type WorkflowDefinitionDao struct {
	conn *gorms.GormConn
}

func NewWorkflowDefinitionDao() repo.WorkflowDefinitionRepo {
	return &WorkflowDefinitionDao{conn: gorms.New()}
}

func (d *WorkflowDefinitionDao) Save(ctx context.Context, def *data.WorkflowDefinition) error {
	return d.conn.Master().WithContext(ctx).Create(def).Error
}

func (d *WorkflowDefinitionDao) FindByKeyVersion(ctx context.Context, tenantId int64, defKey string, version int) (*data.WorkflowDefinition, error) {
	var def data.WorkflowDefinition
	err := d.conn.Master().WithContext(ctx).
		Where("tenant_id = ? AND def_key = ? AND version = ?", tenantId, defKey, version).
		First(&def).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &def, nil
}

func (d *WorkflowDefinitionDao) FindLatestEnabled(ctx context.Context, tenantId int64, defKey string) (*data.WorkflowDefinition, error) {
	var def data.WorkflowDefinition
	err := d.conn.Master().WithContext(ctx).
		Where("tenant_id = ? AND def_key = ? AND status = ?", tenantId, defKey, data.WorkflowDefinitionEnabled).
		Order("version DESC").
		First(&def).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &def, nil
}

func (d *WorkflowDefinitionDao) List(ctx context.Context, tenantId int64, defKey string, page, pageSize int64) ([]*data.WorkflowDefinition, int64, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 20
	}

	tx := d.conn.Master().WithContext(ctx).Model(&data.WorkflowDefinition{}).Where("tenant_id = ?", tenantId)
	if defKey != "" {
		tx = tx.Where("def_key = ?", defKey)
	}

	var total int64
	if err := tx.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var list []*data.WorkflowDefinition
	err := tx.Order("def_key ASC, version DESC").
		Offset(int((page - 1) * pageSize)).
		Limit(int(pageSize)).
		Find(&list).Error
	if err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (d *WorkflowDefinitionDao) NextVersion(ctx context.Context, tenantId int64, defKey string) (int, error) {
	var maxVersion *int
	err := d.conn.Master().WithContext(ctx).
		Model(&data.WorkflowDefinition{}).
		Where("tenant_id = ? AND def_key = ?", tenantId, defKey).
		Select("MAX(version)").
		Scan(&maxVersion).Error
	if err != nil {
		return 0, err
	}
	if maxVersion == nil {
		return 1, nil
	}
	return *maxVersion + 1, nil
}

func (d *WorkflowDefinitionDao) DisableAll(ctx context.Context, tenantId int64, defKey string) error {
	return d.conn.Master().WithContext(ctx).
		Model(&data.WorkflowDefinition{}).
		Where("tenant_id = ? AND def_key = ?", tenantId, defKey).
		Update("status", data.WorkflowDefinitionDisabled).Error
}
