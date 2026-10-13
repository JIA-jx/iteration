package dao

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm/clause"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type TaskDependencyDao struct {
	conn *gorms.GormConn
}

func NewTaskDependencyDao() *TaskDependencyDao {
	return &TaskDependencyDao{conn: gorms.New()}
}

func (d *TaskDependencyDao) Save(ctx context.Context, td *data.TaskDependency) error {
	if td == nil {
		return errors.New("TaskDependencyDao.Save: dependency is nil")
	}
	if td.ProjectCode <= 0 || td.TaskCode <= 0 || td.PreTaskCode <= 0 {
		return errors.New("TaskDependencyDao.Save: projectCode/taskCode/preTaskCode is required")
	}
	if td.Type == "" {
		td.Type = data.DependencyTypeFS
	}
	if td.CreatedAt.IsZero() {
		td.CreatedAt = time.Now()
	}

	return d.conn.Session(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "project_code"},
				{Name: "task_code"},
				{Name: "pre_task_code"},
				{Name: "type"},
			},
			DoUpdates: clause.AssignmentColumns([]string{"lag"}),
		}).
		Create(td).Error
}

func (d *TaskDependencyDao) Delete(ctx context.Context, id int64) (int64, error) {
	if id <= 0 {
		return 0, errors.New("TaskDependencyDao.Delete: id is required")
	}
	res := d.conn.Session(ctx).
		Where("id = ?", id).
		Delete(&data.TaskDependency{})
	return res.RowsAffected, res.Error
}

func (d *TaskDependencyDao) ListByProject(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error) {
	list := make([]*data.TaskDependency, 0)
	err := d.conn.Slave().WithContext(ctx).
		Model(&data.TaskDependency{}).
		Where("project_code = ?", projectCode).
		Order("id ASC").
		Find(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (d *TaskDependencyDao) ListByTask(ctx context.Context, taskCode int64) ([]*data.TaskDependency, error) {
	list := make([]*data.TaskDependency, 0)
	err := d.conn.Slave().WithContext(ctx).
		Model(&data.TaskDependency{}).
		Where("task_code = ? OR pre_task_code = ?", taskCode, taskCode).
		Order("id ASC").
		Find(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (d *TaskDependencyDao) DeleteByTask(ctx context.Context, taskCode int64) (int64, error) {
	if taskCode <= 0 {
		return 0, errors.New("TaskDependencyDao.DeleteByTask: taskCode is required")
	}
	res := d.conn.Session(ctx).
		Where("task_code = ? OR pre_task_code = ?", taskCode, taskCode).
		Delete(&data.TaskDependency{})
	return res.RowsAffected, res.Error
}

func (d *TaskDependencyDao) DeleteByTaskPair(ctx context.Context, projectCode, taskCode, preTaskCode int64) (int64, error) {
	if taskCode <= 0 || preTaskCode <= 0 {
		return 0, errors.New("TaskDependencyDao.DeleteByTaskPair: taskCode/preTaskCode is required")
	}
	session := d.conn.Session(ctx).
		Where("task_code = ?", taskCode).
		Where("pre_task_code = ?", preTaskCode)
	if projectCode > 0 {
		session = session.Where("project_code = ?", projectCode)
	}
	res := session.Delete(&data.TaskDependency{})
	return res.RowsAffected, res.Error
}
