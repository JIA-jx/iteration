package dao

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type TaskActionDao struct {
	conn *gorms.GormConn
}

func NewTaskActionDao() *TaskActionDao {
	return &TaskActionDao{conn: gorms.New()}
}

func (d *TaskActionDao) BatchUpdateTasks(ctx context.Context, taskIds []int64, fields map[string]interface{}) (int64, error) {
	if len(taskIds) == 0 || len(fields) == 0 {
		return 0, nil
	}
	res := d.conn.Session(ctx).
		Model(&data.Task{}).
		Where("id IN (?)", taskIds).
		Updates(fields)
	return res.RowsAffected, res.Error
}

func (d *TaskActionDao) ListMemberAccounts(ctx context.Context, keyword string, limit int) ([]*data.MemberAccount, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	session := d.conn.Reader(ctx).WithContext(ctx).Model(&data.MemberAccount{})
	if keyword != "" {
		like := "%" + keyword + "%"
		session = session.Where("name LIKE ? OR mobile LIKE ? OR email LIKE ?", like, like, like)
	}
	var list []*data.MemberAccount
	err := session.Order("id ASC").Limit(limit).Find(&list).Error
	return list, err
}

func (d *TaskActionDao) AddTaskMembers(ctx context.Context, taskCode int64, memberCodes []int64) (int64, error) {
	var added int64
	now := time.Now().UnixMilli()
	err := d.conn.Session(ctx).Transaction(func(tx *gorm.DB) error {
		for _, code := range memberCodes {
			if code <= 0 {
				continue
			}
			var exist int64
			if err := tx.WithContext(ctx).
				Model(&data.TaskMember{}).
				Where("task_code = ? AND member_code = ?", taskCode, code).
				Count(&exist).Error; err != nil {
				return err
			}
			if exist > 0 {
				continue
			}
			tm := &data.TaskMember{
				TaskCode:   taskCode,
				MemberCode: code,
				JoinTime:   now,
			}
			if err := tx.WithContext(ctx).Create(tm).Error; err != nil {
				return err
			}
			added++
		}
		return nil
	})
	return added, err
}

func (d *TaskActionDao) RemoveTaskMembers(ctx context.Context, taskCode int64, memberCodes []int64) (int64, error) {
	if len(memberCodes) == 0 {
		return 0, nil
	}
	res := d.conn.Session(ctx).
		Where("task_code = ? AND member_code IN (?)", taskCode, memberCodes).
		Delete(&data.TaskMember{})
	return res.RowsAffected, res.Error
}

func (d *TaskActionDao) SaveStage(ctx context.Context, stage *data.TaskStages) error {
	return d.conn.Session(ctx).Create(stage).Error
}

func (d *TaskActionDao) UpdateStage(ctx context.Context, projectCode int64, id int64, fields map[string]interface{}) (int64, error) {
	res := d.conn.Session(ctx).
		Model(&data.TaskStages{}).
		Where("id = ? AND project_code = ?", id, projectCode).
		Updates(fields)
	return res.RowsAffected, res.Error
}

func (d *TaskActionDao) DeleteStage(ctx context.Context, projectCode int64, id int64) (int64, error) {
	res := d.conn.Session(ctx).
		Model(&data.TaskStages{}).
		Where("id = ? AND project_code = ?", id, projectCode).
		Update("deleted", 1)
	return res.RowsAffected, res.Error
}

func (d *TaskActionDao) ListStages(ctx context.Context, projectCode int64) ([]*data.TaskStages, error) {
	var list []*data.TaskStages
	err := d.conn.Reader(ctx).WithContext(ctx).
		Model(&data.TaskStages{}).
		Where("project_code = ? AND deleted = 0", projectCode).
		Order("sort ASC").
		Find(&list).Error
	return list, err
}

func (d *TaskActionDao) SaveTemplateStages(ctx context.Context, templateCode int64, names []string) (int64, error) {
	var created int64
	now := time.Now().UnixMilli()
	err := d.conn.Session(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(ctx).
			Where("project_template_code = ?", templateCode).
			Delete(&data.MsTaskStagesTemplate{}).Error; err != nil {
			return err
		}
		for i, name := range names {
			if name == "" {
				continue
			}
			st := &data.MsTaskStagesTemplate{
				Name:                name,
				ProjectTemplateCode: int(templateCode),
				CreateTime:          now,
				Sort:                (i + 1) * 65536,
			}
			if err := tx.WithContext(ctx).Create(st).Error; err != nil {
				return err
			}
			created++
		}
		return nil
	})
	return created, err
}

func (d *TaskActionDao) ListTemplateStages(ctx context.Context, templateCode int64) ([]*data.MsTaskStagesTemplate, error) {
	var list []*data.MsTaskStagesTemplate
	err := d.conn.Reader(ctx).WithContext(ctx).
		Model(&data.MsTaskStagesTemplate{}).
		Where("project_template_code = ?", templateCode).
		Order("sort ASC").
		Find(&list).Error
	return list, err
}

func (d *TaskActionDao) FindTemplateStageById(ctx context.Context, id int64) (*data.MsTaskStagesTemplate, error) {
	var row data.MsTaskStagesTemplate
	err := d.conn.Reader(ctx).WithContext(ctx).Where("id = ?", id).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (d *TaskActionDao) UpdateTemplateStage(ctx context.Context, id int64, name string) (int64, error) {
	res := d.conn.Session(ctx).
		Model(&data.MsTaskStagesTemplate{}).
		Where("id = ?", id).
		Update("name", name)
	return res.RowsAffected, res.Error
}

func (d *TaskActionDao) DeleteTemplateStage(ctx context.Context, id int64) (int64, error) {
	res := d.conn.Session(ctx).Where("id = ?", id).Delete(&data.MsTaskStagesTemplate{})
	return res.RowsAffected, res.Error
}

func (d *TaskActionDao) SaveTag(ctx context.Context, tag *data.TaskTag) error {
	return d.conn.Session(ctx).Create(tag).Error
}

func (d *TaskActionDao) UpdateTag(ctx context.Context, projectCode int64, id int64, fields map[string]interface{}) (int64, error) {
	res := d.conn.Session(ctx).
		Model(&data.TaskTag{}).
		Where("id = ? AND project_code = ?", id, projectCode).
		Updates(fields)
	return res.RowsAffected, res.Error
}

func (d *TaskActionDao) DeleteTag(ctx context.Context, projectCode int64, id int64) (int64, error) {
	res := d.conn.Session(ctx).
		Where("id = ? AND project_code = ?", id, projectCode).
		Delete(&data.TaskTag{})
	return res.RowsAffected, res.Error
}

func (d *TaskActionDao) FindTagById(ctx context.Context, id int64) (*data.TaskTag, error) {
	var tag data.TaskTag
	err := d.conn.Reader(ctx).WithContext(ctx).Where("id = ?", id).First(&tag).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &tag, nil
}

func (d *TaskActionDao) ListTags(ctx context.Context, projectCode int64) ([]*data.TaskTag, error) {
	var list []*data.TaskTag
	session := d.conn.Reader(ctx).WithContext(ctx).Model(&data.TaskTag{})
	if projectCode > 0 {
		session = session.Where("project_code = ?", projectCode)
	}
	err := session.Order("sort ASC, id ASC").Limit(200).Find(&list).Error
	return list, err
}

func (d *TaskActionDao) ListTasksByTag(ctx context.Context, projectCode int64, tag string) ([]*data.Task, error) {
	var list []*data.Task
	session := d.conn.Reader(ctx).WithContext(ctx).
		Model(&data.Task{}).
		Where("task_tag = ? AND deleted = 0", tag)
	if projectCode > 0 {
		session = session.Where("project_code = ?", projectCode)
	}
	err := session.Order("sort ASC, id ASC").Limit(200).Find(&list).Error
	return list, err
}

func (d *TaskActionDao) UpdateWorkTime(ctx context.Context, taskCode int64, id int64, fields map[string]interface{}) (int64, error) {
	res := d.conn.Session(ctx).
		Model(&data.TaskWorkTime{}).
		Where("id = ? AND task_code = ?", id, taskCode).
		Updates(fields)
	return res.RowsAffected, res.Error
}

func (d *TaskActionDao) DeleteWorkTime(ctx context.Context, taskCode int64, id int64) (int64, error) {
	res := d.conn.Session(ctx).
		Where("id = ? AND task_code = ?", id, taskCode).
		Delete(&data.TaskWorkTime{})
	return res.RowsAffected, res.Error
}

func (d *TaskActionDao) ListWorkTime(ctx context.Context, taskCode int64, page, pageSize int64) ([]*data.TaskWorkTime, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 200 {
		pageSize = 20
	}
	var list []*data.TaskWorkTime
	err := d.conn.Reader(ctx).WithContext(ctx).
		Model(&data.TaskWorkTime{}).
		Where("task_code = ?", taskCode).
		Order("id DESC").
		Limit(int(pageSize)).
		Offset(int((page - 1) * pageSize)).
		Find(&list).Error
	if err != nil {
		return nil, 0, err
	}
	var total int64
	err = d.conn.Reader(ctx).WithContext(ctx).
		Model(&data.TaskWorkTime{}).
		Where("task_code = ?", taskCode).
		Count(&total).Error
	return list, total, err
}
