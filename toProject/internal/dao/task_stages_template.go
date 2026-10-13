package dao

import (
	"context"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type TaskStagesTemplateDao struct {
	conn *gorms.GormConn
}

func (t *TaskStagesTemplateDao) FindByProjectTemplateId(ctx context.Context, projectTemplateCode int) (list []*data.MsTaskStagesTemplate, err error) {
	session := t.conn.Slave().WithContext(ctx)
	err = session.
		Model(&data.MsTaskStagesTemplate{}).
		Where("project_template_code=?", projectTemplateCode).
		Order("sort desc,id asc").
		Find(&list).
		Error
	return
}

func (t *TaskStagesTemplateDao) FindInProTemIds(ctx context.Context, ids []int) ([]data.MsTaskStagesTemplate, error) {
	var tsts []data.MsTaskStagesTemplate
	session := t.conn.Session(ctx)
	err := session.Model(&data.MsTaskStagesTemplate{}).Where("project_template_code in ?", ids).Find(&tsts).Error
	return tsts, err
}

func NewTaskStagesTemplateDao() *TaskStagesTemplateDao {
	return &TaskStagesTemplateDao{
		conn: gorms.New(),
	}
}
