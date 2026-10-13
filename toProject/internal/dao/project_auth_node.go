package dao

import (
	"context"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/database/gorms"
)

type ProjectAuthNodeDao struct {
	conn *gorms.GormConn
}

func (p *ProjectAuthNodeDao) FindNodeStringList(ctx context.Context, authId int64) (list []string, err error) {
	session := p.conn.Slave().WithContext(ctx)
	err = session.Model(&data.ProjectAuthNode{}).Where("auth=?", authId).Select("node").Find(&list).Error
	return
}

func (p *ProjectAuthNodeDao) DeleteByAuthId(ctx context.Context, conn database.DbConn, authId int64) error {
	tx := conn.(*gorms.GormConn).Tx(ctx)
	err := tx.Where("auth=?", authId).Delete(&data.ProjectAuthNode{}).Error
	return err
}

func (p *ProjectAuthNodeDao) Save(ctx context.Context, conn database.DbConn, authId int64, nodes []string) error {
	tx := conn.(*gorms.GormConn).Tx(ctx)
	var list []*data.ProjectAuthNode
	for _, v := range nodes {
		pn := &data.ProjectAuthNode{
			Auth: authId,
			Node: v,
		}
		list = append(list, pn)
	}
	err := tx.Create(list).Error
	return err
}

func NewProjectAuthNodeDao() *ProjectAuthNodeDao {
	return &ProjectAuthNodeDao{
		conn: gorms.New(),
	}
}
