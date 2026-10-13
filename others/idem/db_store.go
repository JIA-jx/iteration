package idem

import (
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

type DBRecord struct {
	Id        int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	IdemKey   string    `gorm:"column:idem_key;size:191;not null;uniqueIndex:uk_idem_key" json:"idem_key"`
	Status    string    `gorm:"column:status;size:16;not null" json:"status"`
	Result    string    `gorm:"column:result;type:text" json:"result"`
	Reason    string    `gorm:"column:reason;size:512" json:"reason"`
	ExpireAt  time.Time `gorm:"column:expire_at;not null;index:idx_expire_at" json:"expire_at"`
	CreatedAt time.Time `gorm:"column:created_at;not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null" json:"updated_at"`
}

func (DBRecord) TableName() string {
	return "idempotent_record"
}

type DBStore struct {
	db  *gorm.DB
	now func() time.Time
}

func NewDBStore(db *gorm.DB) *DBStore {
	return &DBStore{db: db, now: time.Now}
}

func (s *DBStore) TryAcquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if s.db == nil {
		return false, errors.New("idem: db is nil")
	}

	now := s.now()

	if err := s.db.WithContext(ctx).
		Where("idem_key = ? AND expire_at <= ?", key, now).
		Delete(&DBRecord{}).Error; err != nil {
		return false, err
	}

	rec := &DBRecord{
		IdemKey:   key,
		Status:    StatusProcessing,
		ExpireAt:  now.Add(ttl),
		CreatedAt: now,
		UpdatedAt: now,
	}
	err := s.db.WithContext(ctx).Create(rec).Error
	if err == nil {
		return true, nil
	}
	if IsDuplicateErr(err) {
		return false, nil
	}
	return false, err
}

func (s *DBStore) Get(ctx context.Context, key string) (*Record, error) {
	if s.db == nil {
		return nil, errors.New("idem: db is nil")
	}

	var rec DBRecord
	err := s.db.WithContext(ctx).
		Where("idem_key = ? AND expire_at > ?", key, s.now()).
		First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &Record{
		Key:       rec.IdemKey,
		Status:    rec.Status,
		Result:    rec.Result,
		Reason:    rec.Reason,
		CreatedAt: rec.CreatedAt,
		UpdatedAt: rec.UpdatedAt,
		ExpireAt:  rec.ExpireAt,
	}, nil
}

func (s *DBStore) Complete(ctx context.Context, key, result string, ttl time.Duration) error {
	return s.update(ctx, key, StatusSucceeded, result, "", ttl)
}

func (s *DBStore) Fail(ctx context.Context, key, reason string, ttl time.Duration) error {
	return s.update(ctx, key, StatusFailed, "", reason, ttl)
}

func (s *DBStore) update(ctx context.Context, key, status, result, reason string, ttl time.Duration) error {
	if s.db == nil {
		return errors.New("idem: db is nil")
	}
	return s.db.WithContext(ctx).
		Model(&DBRecord{}).
		Where("idem_key = ?", key).
		Updates(map[string]interface{}{
			"status":     status,
			"result":     result,
			"reason":     reason,
			"expire_at":  s.now().Add(ttl),
			"updated_at": s.now(),
		}).Error
}

func (s *DBStore) Release(ctx context.Context, key string) error {
	if s.db == nil {
		return errors.New("idem: db is nil")
	}
	return s.db.WithContext(ctx).
		Where("idem_key = ? AND status = ?", key, StatusProcessing).
		Delete(&DBRecord{}).Error
}

func IsDuplicateErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "Duplicate entry") ||
		strings.Contains(msg, "Error 1062") ||
		strings.Contains(msg, "UNIQUE constraint failed")
}
