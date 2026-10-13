package dao

import (
	"context"
	"test.com/toProject/internal/data"
)

func (t *TaskWorkTimeDao) ListWorkTimeByTaskIds(ctx context.Context, taskIds []int64) (list []*data.TaskWorkTime, err error) {
	if len(taskIds) == 0 {
		return nil, nil
	}
	err = t.conn.Slave().WithContext(ctx).
		Model(&data.TaskWorkTime{}).
		Where("task_code IN (?)", taskIds).
		Find(&list).Error
	return
}
