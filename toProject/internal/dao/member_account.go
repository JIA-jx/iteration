package dao

import (
	"context"
	"gorm.io/gorm"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type MemberAccountDao struct {
	conn *gorms.GormConn
}

func (m *MemberAccountDao) FindByMemberId(ctx context.Context, memberId int64) (ma *data.MemberAccount, err error) {
	session := m.conn.Slave().WithContext(ctx)
	err = session.Where("member_code=?", memberId).Take(&ma).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return
}

func (m *MemberAccountDao) FindList(ctx context.Context, condition string, organizationCode int64, departmentCode int64, page int64, pageSize int64) (list []*data.MemberAccount, total int64, err error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 200 {
		pageSize = 200
	}

	session := m.conn.Slave().WithContext(ctx)
	offset := (page - 1) * pageSize
	err = session.Model(&data.MemberAccount{}).
		Where("organization_code=?", organizationCode).
		Where(condition).Limit(int(pageSize)).Offset(int(offset)).Find(&list).Error
	if err != nil {
		return nil, 0, err
	}
	err = m.conn.Slave().WithContext(ctx).
		Model(&data.MemberAccount{}).
		Where("organization_code=?", organizationCode).
		Where(condition).Count(&total).Error
	return
}

func NewMemberAccountDao() *MemberAccountDao {
	return &MemberAccountDao{
		conn: gorms.New(),
	}
}
