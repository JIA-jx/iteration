package dao

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type DelayTaskDao struct {
	conn *gorms.GormConn
}

func NewDelayTaskDao() *DelayTaskDao {
	return &DelayTaskDao{conn: gorms.New()}
}

func (d *DelayTaskDao) Upsert(ctx context.Context, t *data.DelayTask) error {
	if t == nil || t.TaskId == "" {
		return errors.New("DelayTaskDao.Upsert: task_id is empty")
	}
	now := time.Now()
	t.CreatedAt = now
	t.UpdatedAt = now

	return d.conn.Master().WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "task_id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"type", "payload", "run_at", "status", "attempts",
				"member_id", "project_code", "last_error", "updated_at",
			}),
		}).
		Create(t).Error
}

func (d *DelayTaskDao) UpdateStatus(ctx context.Context, taskId, status, lastError string, attempts int) error {
	if taskId == "" {
		return errors.New("DelayTaskDao.UpdateStatus: task_id is empty")
	}
	updates := map[string]interface{}{
		"status":     status,
		"attempts":   attempts,
		"updated_at": time.Now(),
	}
	if lastError != "" {
		updates["last_error"] = truncateUTF8(lastError, 500)
	}

	return d.conn.Master().WithContext(ctx).
		Model(&data.DelayTask{}).
		Where("task_id = ?", taskId).
		Updates(updates).Error
}

func (d *DelayTaskDao) FindByTaskId(ctx context.Context, taskId string) (*data.DelayTask, error) {
	var t data.DelayTask
	err := d.conn.Slave().WithContext(ctx).
		Where("task_id = ?", taskId).
		Take(&t).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (d *DelayTaskDao) List(ctx context.Context, taskType, status string, page, pageSize int64) ([]*data.DelayTask, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 200 {
		pageSize = 200
	}

	session := d.conn.Slave().WithContext(ctx).Model(&data.DelayTask{})
	if taskType != "" {
		session = session.Where("type = ?", taskType)
	}
	if status != "" {
		session = session.Where("status = ?", status)
	}

	var total int64
	if err := session.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var list []*data.DelayTask
	err := session.Order("id DESC").
		Limit(int(pageSize)).
		Offset(int((page - 1) * pageSize)).
		Find(&list).Error
	return list, total, err
}

func (d *DelayTaskDao) ListPendingReminders(ctx context.Context, limit int) ([]*data.Task, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	var list []*data.Task
	err := d.conn.Slave().WithContext(ctx).
		Model(&data.Task{}).
		Where("done = ?", 0).
		Where("deleted = ?", 0).
		Where("assign_to > 0").
		Where("end_time > 0").
		Order("id ASC").
		Limit(limit).
		Find(&list).Error
	return list, err
}
