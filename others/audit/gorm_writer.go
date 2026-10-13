package audit

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

type GormWriter struct {
	db  *gorm.DB
	now func() time.Time
}

func NewGormWriter(db *gorm.DB) *GormWriter {
	return &GormWriter{db: db, now: time.Now}
}

func (w *GormWriter) Write(ctx context.Context, events ...*Event) error {
	if w.db == nil {
		return errors.New("audit: db is nil")
	}
	if len(events) == 0 {
		return nil
	}

	list := make([]*Event, 0, len(events))
	for _, e := range events {
		if e == nil {
			continue
		}
		if e.CreatedAt.IsZero() {
			e.CreatedAt = w.now()
		}
		list = append(list, e)
	}
	if len(list) == 0 {
		return nil
	}

	return w.db.WithContext(ctx).CreateInBatches(list, 100).Error
}

func (w *GormWriter) List(ctx context.Context, q Query) ([]*Event, int64, error) {
	if w.db == nil {
		return nil, 0, errors.New("audit: db is nil")
	}

	if q.Page <= 0 {
		q.Page = 1
	}
	if q.PageSize <= 0 {
		q.PageSize = 20
	}
	if q.PageSize > 200 {
		q.PageSize = 200
	}

	tx := w.db.WithContext(ctx).Model(&Event{}).
		Where("tenant_id = ?", q.TenantId)
	if q.Action != "" {
		tx = tx.Where("action = ?", q.Action)
	}
	if q.Resource != "" {
		tx = tx.Where("resource = ?", q.Resource)
	}
	if q.ResourceId != "" {
		tx = tx.Where("resource_id = ?", q.ResourceId)
	}
	if q.ActorId > 0 {
		tx = tx.Where("actor_id = ?", q.ActorId)
	}
	if q.TraceId != "" {
		tx = tx.Where("trace_id = ?", q.TraceId)
	}
	if !q.BeginTime.IsZero() {
		tx = tx.Where("created_at >= ?", q.BeginTime)
	}
	if !q.EndTime.IsZero() {
		tx = tx.Where("created_at <= ?", q.EndTime)
	}

	var total int64
	if err := tx.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var list []*Event
	err := tx.Order("id DESC").
		Offset((q.Page - 1) * q.PageSize).
		Limit(q.PageSize).
		Find(&list).Error
	if err != nil {
		return nil, 0, err
	}
	return list, total, nil
}
