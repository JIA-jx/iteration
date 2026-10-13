package dao

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type SourceLinkDao struct {
	conn *gorms.GormConn
}

func (s *SourceLinkDao) Save(ctx context.Context, link *data.SourceLink) error {
	return s.conn.Session(ctx).Save(&link).Error
}

func (s *SourceLinkDao) FindByTaskCode(ctx context.Context, taskCode int64) (list []*data.SourceLink, err error) {
	session := s.conn.Slave().WithContext(ctx)
	err = session.Model(&data.SourceLink{}).Where("link_type=? and link_code=?", "task", taskCode).Find(&list).Error
	return
}

func NewSourceLinkDao() *SourceLinkDao {
	return &SourceLinkDao{
		conn: gorms.New(),
	}
}
func (s *SourceLinkDao) DeleteById(ctx context.Context, id int64) (int64, error) {
	res := s.conn.Session(ctx).
		Model(&data.SourceLink{}).
		Where("id = ?", id).
		Delete(&data.SourceLink{})
	return res.RowsAffected, res.Error
}

func (s *SourceLinkDao) FindById(ctx context.Context, id int64) (*data.SourceLink, error) {
	var link data.SourceLink
	err := s.conn.Reader(ctx).WithContext(ctx).Where("id = ?", id).First(&link).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &link, nil
}
