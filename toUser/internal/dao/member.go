package dao

import (
	"context"
	"errors"
	"test.com/toUser/internal/data/member"
	"test.com/toUser/internal/database"
	"test.com/toUser/internal/database/gorms"

	"gorm.io/gorm"
	"test.com/others/tenant"
)

type MemberDao struct {
	conn *gorms.GormConn
}

func (m *MemberDao) FindMemberByIds(ctx context.Context, ids []int64) ([]*member.Member, error) {
	var list []*member.Member
	if len(ids) == 0 {
		return list, nil
	}

	err := m.conn.Slave().WithContext(ctx).
		Where("id IN (?)", ids).
		Find(&list).Error

	return list, err
}

func (m *MemberDao) FindMemberById(ctx context.Context, id int64) (*member.Member, error) {
	var mem member.Member
	err := m.conn.Slave().WithContext(ctx).
		Where("id = ?", id).
		First(&mem).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &mem, nil
}

func (m *MemberDao) FindMember(ctx context.Context, account string, pwd string) (*member.Member, error) {
	var mem member.Member
	err := m.conn.Slave().WithContext(ctx).
		Where("(account = ? OR email = ?) AND password = ?", account, account, pwd).
		First(&mem).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &mem, nil
}

func (m *MemberDao) CountByTenant(ctx context.Context, tenantId int64) (int64, error) {
	var count int64
	err := m.conn.Slave().WithContext(tenant.ScopeAll(ctx)).
		Model(&member.Member{}).
		Where("tenant_id = ?", tenantId).
		Count(&count).Error
	return count, err
}

func (m *MemberDao) UpdatePassword(ctx context.Context, id int64, pwd string) error {
	if id <= 0 || pwd == "" {
		return nil
	}
	return m.conn.Session(ctx).
		Model(&member.Member{}).
		Where("id = ?", id).
		Update("password", pwd).Error
}

func NewMemberDao() *MemberDao {
	return &MemberDao{
		conn: gorms.New(),
	}
}

func (m *MemberDao) SaveMember(conn database.DbConn, ctx context.Context, mem *member.Member) error {
	return conn.(*gorms.GormConn).Tx(ctx).Create(mem).Error
}

func (m *MemberDao) GetMemberByEmail(ctx context.Context, email string) (bool, error) {
	var count int64
	err := m.conn.Slave().WithContext(ctx).
		Model(&member.Member{}).
		Where("email = ?", email).
		Count(&count).Error

	return count > 0, err
}

func (m *MemberDao) GetMemberByAccount(ctx context.Context, account string) (bool, error) {
	var count int64
	err := m.conn.Slave().WithContext(ctx).
		Model(&member.Member{}).
		Where("account = ?", account).
		Count(&count).Error

	return count > 0, err
}

func (m *MemberDao) GetMemberByMobile(ctx context.Context, mobile string) (bool, error) {
	var count int64
	err := m.conn.Slave().WithContext(ctx).
		Model(&member.Member{}).
		Where("mobile = ?", mobile).
		Count(&count).Error

	return count > 0, err
}
