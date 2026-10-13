package dao

import (
	"context"
	"test.com/toProject/internal/data"
)

func (t *TaskDao) ListTaskByProject(ctx context.Context, projectCode int64) (list []*data.Task, err error) {
	err = t.conn.Slave().WithContext(ctx).
		Model(&data.Task{}).
		Where("project_code = ?", projectCode).
		Where("deleted = 0").
		Order("sort ASC, id ASC").
		Find(&list).Error
	return
}

func (t *TaskDao) UpdateTaskSchedule(ctx context.Context, taskCode int64, beginTime, endTime int64) error {
	return t.conn.Session(ctx).
		Model(&data.Task{}).
		Where("id = ?", taskCode).
		UpdateColumns(map[string]interface{}{
			"begin_time": beginTime,
			"end_time":   endTime,
		}).Error
}
