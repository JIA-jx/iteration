package dao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"test.com/others/tenant"
	"test.com/others/wfengine"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type WorkflowStore struct {
	conn *gorms.GormConn
}

func NewWorkflowStore() *WorkflowStore {
	return &WorkflowStore{conn: gorms.New()}
}

func (s *WorkflowStore) Create(ctx context.Context, inst *wfengine.Instance) error {
	if inst == nil || inst.Id == "" {
		return fmt.Errorf("%w: instance is nil or has empty id", wfengine.ErrInvalidRequest)
	}

	row, err := toInstanceRow(inst)
	if err != nil {
		return err
	}

	return s.conn.Master().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(row).Error; err != nil {
			if isDuplicateRow(err) {
				return fmt.Errorf("%w: instance %s already exists", wfengine.ErrInvalidState, inst.Id)
			}
			return err
		}
		return syncTasks(tx, inst)
	})
}

func (s *WorkflowStore) Get(ctx context.Context, id string) (*wfengine.Instance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: empty instance id", wfengine.ErrInvalidRequest)
	}

	var row data.WorkflowInstance
	err := s.conn.Master().WithContext(ctx).
		Where("id = ? AND tenant_id = ?", id, tenant.IdFromContext(ctx)).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("%w: %s", wfengine.ErrInstanceNotFound, id)
	}
	if err != nil {
		return nil, err
	}

	return fromInstanceRow(&row)
}

func (s *WorkflowStore) Update(ctx context.Context, id string, expectedVersion int64, mutate func(*wfengine.Instance) error) error {
	if id == "" {
		return fmt.Errorf("%w: empty instance id", wfengine.ErrInvalidRequest)
	}
	if mutate == nil {
		return fmt.Errorf("%w: mutate func is nil", wfengine.ErrInvalidRequest)
	}

	return s.conn.Master().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row data.WorkflowInstance
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND tenant_id = ?", id, tenant.IdFromContext(ctx)).
			First(&row).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: %s", wfengine.ErrInstanceNotFound, id)
		}
		if err != nil {
			return err
		}

		if row.Version != expectedVersion {
			return fmt.Errorf("%w: instance %s expected version %d but stored version is %d",
				wfengine.ErrVersionConflict, id, expectedVersion, row.Version)
		}

		inst, err := fromInstanceRow(&row)
		if err != nil {
			return err
		}

		if err := mutate(inst); err != nil {
			return err
		}

		inst.Version = expectedVersion + 1

		body, err := json.Marshal(inst)
		if err != nil {
			return fmt.Errorf("workflow: marshal instance failed: %w", err)
		}

		res := tx.Model(&data.WorkflowInstance{}).
			Where("id = ? AND version = ? AND tenant_id = ?", id, expectedVersion, tenant.IdFromContext(ctx)).
			Updates(map[string]interface{}{
				"instance_json": string(body),
				"status":        string(inst.Status),
				"current_key":   inst.CurrentKey,
				"finished_at":   inst.FinishedAt,
				"version":       inst.Version,
				"updated_at":    time.Now(),
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("%w: instance %s lost the version race at %d", wfengine.ErrVersionConflict, id, expectedVersion)
		}

		return syncTasks(tx, inst)
	})
}

func (s *WorkflowStore) ListAll(ctx context.Context) ([]*wfengine.Instance, error) {
	var rows []*data.WorkflowInstance
	err := s.conn.Master().WithContext(ctx).
		Model(&data.WorkflowInstance{}).
		Where("status = ?", string(wfengine.InstanceRunning)).
		Order("updated_at ASC").
		Limit(2000).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}

	out := make([]*wfengine.Instance, 0, len(rows))
	for _, row := range rows {
		inst, err := fromInstanceRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, nil
}

func (s *WorkflowStore) ListByBiz(ctx context.Context, tenantId int64, bizId string) ([]*wfengine.Instance, error) {
	if tenantId <= 0 && bizId == "" {
		return nil, nil
	}

	tx := s.conn.Master().WithContext(ctx).Model(&data.WorkflowInstance{})
	if tenantId > 0 {
		tx = tx.Where("tenant_id = ?", tenantId)
	}
	if bizId != "" {
		tx = tx.Where("biz_id = ?", bizId)
	}

	var rows []*data.WorkflowInstance
	if err := tx.Order("created_at DESC").Limit(500).Find(&rows).Error; err != nil {
		return nil, err
	}

	out := make([]*wfengine.Instance, 0, len(rows))
	for _, row := range rows {
		inst, err := fromInstanceRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, nil
}

func (s *WorkflowStore) ListPendingTasks(ctx context.Context, approverId int64, tenantId int64) ([]*wfengine.Task, error) {
	if approverId <= 0 {
		return nil, nil
	}

	tx := s.conn.Master().WithContext(ctx).
		Model(&data.WorkflowTask{}).
		Where("approver_id = ? AND status = ?", approverId, string(wfengine.TaskPending))
	if tenantId > 0 {
		tx = tx.Where("tenant_id = ?", tenantId)
	}

	var rows []*data.WorkflowTask
	if err := tx.Order("created_at ASC").Limit(500).Find(&rows).Error; err != nil {
		return nil, err
	}

	out := make([]*wfengine.Task, 0, len(rows))
	for _, row := range rows {
		out = append(out, &wfengine.Task{
			Id:         row.Id,
			InstanceId: row.InstanceId,
			NodeKey:    row.NodeKey,
			NodeName:   row.NodeName,
			ApproverId: row.ApproverId,
			Status:     wfengine.TaskStatus(row.Status),
			Comment:    row.Comment,
			CreatedAt:  row.CreatedAt,
			HandledAt:  row.HandledAt,
			TimeoutAt:  row.TimeoutAt,
		})
	}
	return out, nil
}

func toInstanceRow(inst *wfengine.Instance) (*data.WorkflowInstance, error) {
	body, err := json.Marshal(inst)
	if err != nil {
		return nil, fmt.Errorf("workflow: marshal instance failed: %w", err)
	}

	return &data.WorkflowInstance{
		Id:                inst.Id,
		TenantId:          inst.TenantId,
		DefinitionKey:     inst.DefinitionKey,
		DefinitionVersion: inst.DefinitionVersion,
		BizId:             inst.BizId,
		SubmitterId:       inst.SubmitterId,
		Status:            string(inst.Status),
		CurrentKey:        inst.CurrentKey,
		InstanceJson:      string(body),
		Version:           1,
		CreatedAt:         inst.CreatedAt,
		UpdatedAt:         inst.UpdatedAt,
		FinishedAt:        inst.FinishedAt,
	}, nil
}

func fromInstanceRow(row *data.WorkflowInstance) (*wfengine.Instance, error) {
	if row == nil {
		return nil, fmt.Errorf("%w: nil instance row", wfengine.ErrInstanceNotFound)
	}

	var inst wfengine.Instance
	if err := json.Unmarshal([]byte(row.InstanceJson), &inst); err != nil {
		return nil, fmt.Errorf("workflow: unmarshal instance %s failed: %w", row.Id, err)
	}

	if inst.Id == "" {
		inst.Id = row.Id
	}
	if inst.Version == 0 {
		inst.Version = row.Version
	}
	if inst.TenantId == 0 {
		inst.TenantId = row.TenantId
	}

	return &inst, nil
}

func syncTasks(tx *gorm.DB, inst *wfengine.Instance) error {
	if err := tx.Where("instance_id = ?", inst.Id).Delete(&data.WorkflowTask{}).Error; err != nil {
		return err
	}

	if len(inst.Tasks) == 0 {
		return nil
	}

	rows := make([]*data.WorkflowTask, 0, len(inst.Tasks))
	for _, t := range inst.Tasks {
		if t == nil {
			continue
		}
		rows = append(rows, &data.WorkflowTask{
			Id:         t.Id,
			InstanceId: inst.Id,
			TenantId:   inst.TenantId,
			NodeKey:    t.NodeKey,
			NodeName:   t.NodeName,
			ApproverId: t.ApproverId,
			Status:     string(t.Status),
			Comment:    t.Comment,
			CreatedAt:  t.CreatedAt,
			HandledAt:  t.HandledAt,
			TimeoutAt:  t.TimeoutAt,
		})
	}
	if len(rows) == 0 {
		return nil
	}

	return tx.CreateInBatches(rows, 100).Error
}

func isDuplicateRow(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Duplicate entry") ||
		strings.Contains(msg, "Error 1062") ||
		strings.Contains(msg, "UNIQUE constraint failed")
}
