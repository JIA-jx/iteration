package dao

import (
	"context"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type ProjectNodeDao struct {
	conn *gorms.GormConn
}

func (m *ProjectNodeDao) FindAll(ctx context.Context) (pms []*data.ProjectNode, err error) {
	session := m.conn.Slave().WithContext(ctx)
	err = session.Model(&data.ProjectNode{}).Find(&pms).Error
	return
}

func NewProjectNodeDao() *ProjectNodeDao {
	return &ProjectNodeDao{
		conn: gorms.New(),
	}
}
