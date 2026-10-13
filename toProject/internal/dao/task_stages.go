package dao

import (
	"context"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/database/gorms"
)

type TaskStagesDao struct {
	conn *gorms.GormConn
}

func (t *TaskStagesDao) FindById(ctx context.Context, id int) (ts *data.TaskStages, err error) {
	var stage data.TaskStages
	err = t.conn.Slave().WithContext(ctx).Where("id=?", id).Find(&stage).Error
	if err != nil {
		return nil, err
	}
	if stage.Id == 0 {
		return nil, nil
	}
	return &stage, nil
}

func (t *TaskStagesDao) FindStagesByProjectId(ctx context.Context, projectCode int64, page int64, pageSize int64) (list []*data.TaskStages, total int64, err error) {
	session := t.conn.Slave().WithContext(ctx)
	err = session.Model(&data.TaskStages{}).
		Where("project_code=?", projectCode).
		Order("sort asc").
		Limit(int(pageSize)).Offset(int((page - 1) * pageSize)).
		Find(&list).Error
	err = session.Model(&data.TaskStages{}).
		Where("project_code=?", projectCode).
		Count(&total).Error
	return
}

func (t *TaskStagesDao) SaveTaskStages(ctx context.Context, conn database.DbConn, ts *data.TaskStages) error {
	return conn.(*gorms.GormConn).Tx(ctx).Save(&ts).Error
}

func NewTaskStagesDao() *TaskStagesDao {
	return &TaskStagesDao{
		conn: gorms.New(),
	}
}
