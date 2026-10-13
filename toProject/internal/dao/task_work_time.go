package dao

import (
	"context"
	"test.com/others/tenant"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type TaskWorkTimeDao struct {
	conn *gorms.GormConn
}

func (t *TaskWorkTimeDao) Save(ctx context.Context, twt *data.TaskWorkTime) error {
	session := t.conn.Session(ctx)
	err := session.Save(&twt).Error
	return err
}

func (t *TaskWorkTimeDao) FindWorkTimeList(ctx context.Context, taskCode int64) (list []*data.TaskWorkTime, err error) {
	session := t.conn.Slave().WithContext(ctx)
	err = session.Model(&data.TaskWorkTime{}).Where("task_code=?", taskCode).Find(&list).Error
	return
}

func (t *TaskWorkTimeDao) TaskWorkTimeStat(ctx context.Context, taskCode int64) (stat *data.WorkTimeStat, err error) {
	session := t.conn.Slave().WithContext(ctx)
	err = session.Model(&data.TaskWorkTime{}).
		Where("task_code = ? AND tenant_id = ?", taskCode, tenant.IdFromContext(ctx)).
		Select(`
			COALESCE(SUM(num), 0) AS total_work_time,
			CAST(ROUND(AVG(num)) AS SIGNED) AS avg_work_time,
			COALESCE(MAX(num), 0) AS max_work_time,
			COALESCE(MIN(num), 0) AS min_work_time
		`).
		Scan(&stat).Error

	if err != nil {
		return nil, err
	}
	return stat, nil
}

func NewTaskWorkTimeDao() *TaskWorkTimeDao {
	return &TaskWorkTimeDao{
		conn: gorms.New(),
	}
}
