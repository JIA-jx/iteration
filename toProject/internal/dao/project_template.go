package dao

import (
	"context"
	"gorm.io/gorm"
	"strconv"
	"test.com/api/pkg/model/pro"
	"test.com/others/tenant"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type ProjectTemplateDao struct {
	conn *gorms.GormConn
}

func (p *ProjectTemplateDao) FindProjectTemplateSystem(ctx context.Context, page int64, size int64) (pts []data.ProjectTemplate, total int64, err error) {
	session := p.conn.Slave().WithContext(ctx)
	err = session.
		Model(&data.ProjectTemplate{}).
		Where("is_system=?", 1).
		Limit(int(size)).
		Offset(int((page - 1) * size)).
		Find(&pts).Error
	if err != nil {
		return pts, total, err
	}
	err = session.Model(&data.ProjectTemplate{}).Where("is_system=?", 1).Count(&total).Error
	return pts, total, err
}

func (p *ProjectTemplateDao) FindProjectTemplateCustom(ctx context.Context, memId int64, organizationCode int64, page int64, size int64) (pts []data.ProjectTemplate, total int64, err error) {
	session := p.conn.Slave().WithContext(ctx)
	err = session.
		Model(&data.ProjectTemplate{}).
		Where("is_system=? and member_code=? and organization_code=?", 0, memId, organizationCode).
		Limit(int(size)).
		Offset(int((page - 1) * size)).
		Find(&pts).Error
	if err != nil {
		return pts, total, err
	}
	err = session.Model(&data.ProjectTemplate{}).Where("is_system=? and member_code=? and organization_code=?", 0, memId, organizationCode).Count(&total).Error
	return pts, total, err
}

func (p *ProjectTemplateDao) FindProjectTemplateAll(ctx context.Context, organizationCode int64, page int64, size int64) (pts []data.ProjectTemplate, total int64, err error) {
	session := p.conn.Slave().WithContext(ctx)
	err = session.
		Model(&data.ProjectTemplate{}).
		Where("organization_code=?", organizationCode).
		Limit(int(size)).
		Offset(int((page - 1) * size)).
		Find(&pts).Error
	if err != nil {
		return pts, total, err
	}
	err = session.Model(&data.ProjectTemplate{}).Where("organization_code=?", organizationCode).Count(&total).Error
	return pts, total, err
}

func (p *ProjectTemplateDao) FindPersonalOrg(ctx context.Context, id int64) (string, error) {
	session := p.conn.Reader(ctx).WithContext(ctx)

	var orgID int64
	err := session.Table("iteration_organization").
		Where("member_id = ?", id).
		Where("tenant_id = ?", tenant.IdFromContext(ctx)).
		Select("id").
		Take(&orgID).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return "", err
		}
	}

	return strconv.FormatInt(orgID, 10), nil
}

func (p *ProjectTemplateDao) SaveTemplate(ctx context.Context, template *pro.ProjectTemplate) error {
	session := p.conn.Session(ctx)
	createTime, _ := strconv.ParseInt(template.CreateTime, 10, 64)
	orgCode, _ := strconv.ParseInt(template.OrganizationCode, 10, 64)
	memberCode, _ := strconv.ParseInt(template.MemberCode, 10, 64)
	dbTemplate := map[string]interface{}{
		"name":              template.Name,
		"description":       template.Description,
		"sort":              template.Sort,
		"create_time":       createTime,
		"organization_code": orgCode,
		"cover":             template.Cover,
		"member_code":       memberCode,
		"is_system":         template.IsSystem,
		"tenant_id":         tenant.IdFromContext(ctx),
	}
	err := session.Model(&data.ProjectTemplate{}).Create(dbTemplate).Error
	if err != nil {
		return err
	}
	return nil
}

func (p *ProjectTemplateDao) DeleteTemplate(ctx context.Context, code string) error {
	id, _ := strconv.ParseInt(code, 10, 64)
	session := p.conn.Session(ctx)
	err := session.Model(&data.ProjectTemplate{}).Where("id = ?", id).Delete(&data.ProjectTemplate{}).Error
	if err != nil {
		return err
	}
	return nil
}

func NewProjectTemplateDao() *ProjectTemplateDao {
	return &ProjectTemplateDao{
		conn: gorms.New(),
	}
}
