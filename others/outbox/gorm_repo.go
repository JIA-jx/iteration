package outbox

import (
	"context"
	"time"

	"gorm.io/gorm"
)

type GormRepo struct {
	db *gorm.DB
}

func NewGormRepo(db *gorm.DB) *GormRepo {
	return &GormRepo{db: db}
}

func (d *GormRepo) Save(ctx context.Context, tx *gorm.DB, e *Event) error {
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	e.Status = StatusPending
	e.RetryCnt = 0
	return tx.WithContext(ctx).Create(e).Error
}

func (d *GormRepo) ClaimPending(ctx context.Context, lockToken string, limit int, maxRetry int) ([]*Event, error) {
	if limit <= 0 {
		limit = 50
	}

	db := d.db.WithContext(ctx)
	now := time.Now()

	var candidates []int64
	err := db.Model(&Event{}).
		Where("status = ? AND retry_cnt < ?", StatusPending, maxRetry).
		Where("next_retry_at IS NULL OR next_retry_at <= ?", now).
		Order("id ASC").
		Limit(limit).
		Pluck("id", &candidates).Error
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	if err := db.Model(&Event{}).
		Where("id IN ? AND status = ?", candidates, StatusPending).
		Updates(map[string]interface{}{
			"status":    StatusProcessing,
			"locked_by": lockToken,
			"locked_at": now,
		}).Error; err != nil {
		return nil, err
	}

	var list []*Event
	if err := db.
		Where("locked_by = ? AND status = ?", lockToken, StatusProcessing).
		Order("id ASC").
		Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (d *GormRepo) MarkSent(ctx context.Context, id int64, lockToken string) error {
	now := time.Now()
	res := d.db.WithContext(ctx).
		Model(&Event{}).
		Where("id = ? AND locked_by = ?", id, lockToken).
		Updates(map[string]interface{}{
			"status":     StatusSent,
			"sent_at":    now,
			"locked_by":  "",
			"locked_at":  nil,
			"last_error": "",
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrLockLost
	}
	return nil
}

const markRetryStmt = `UPDATE outbox_event
	SET status = IF(retry_cnt + 1 >= ?, ?, ?),
	    next_retry_at = IF(retry_cnt + 1 >= ?, NULL, ?),
	    retry_cnt = retry_cnt + 1,
	    locked_by = '',
	    locked_at = NULL,
	    last_error = ?
	WHERE id = ? AND locked_by = ?`

func (d *GormRepo) MarkRetry(ctx context.Context, id int64, lockToken string, maxRetry int, backoffSeconds int64, reason string) error {
	if maxRetry <= 0 {
		maxRetry = 1
	}
	if backoffSeconds < 1 {
		backoffSeconds = 1
	}

	nextRetryAt := time.Now().Add(time.Duration(backoffSeconds) * time.Second)

	res := d.db.WithContext(ctx).Exec(markRetryStmt,
		maxRetry, StatusExhausted, StatusPending,
		maxRetry, nextRetryAt,
		TruncateUTF8(reason, 500),
		id, lockToken,
	)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrLockLost
	}
	return nil
}

func (d *GormRepo) MarkExhausted(ctx context.Context, id int64, lockToken string, reason string) error {
	res := d.db.WithContext(ctx).
		Model(&Event{}).
		Where("id = ? AND locked_by = ?", id, lockToken).
		Updates(map[string]interface{}{
			"status":     StatusExhausted,
			"locked_by":  "",
			"locked_at":  nil,
			"last_error": TruncateUTF8(reason, 500),
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrLockLost
	}
	return nil
}

func (d *GormRepo) RecoverStale(ctx context.Context, lockTimeoutSeconds int64, maxRetry int) (int64, error) {
	if lockTimeoutSeconds <= 0 {
		lockTimeoutSeconds = 60
	}
	if maxRetry <= 0 {
		maxRetry = 1
	}
	threshold := time.Now().Add(-time.Duration(lockTimeoutSeconds) * time.Second)

	db := d.db.WithContext(ctx)

	if err := db.Model(&Event{}).
		Where("status = ? AND locked_at IS NOT NULL AND locked_at < ? AND retry_cnt + 1 >= ?",
			StatusProcessing, threshold, maxRetry).
		Updates(map[string]interface{}{
			"status":     StatusExhausted,
			"retry_cnt":  gorm.Expr("retry_cnt + 1"),
			"locked_by":  "",
			"locked_at":  nil,
			"last_error": "lock timeout, retry exhausted",
		}).Error; err != nil {
		return 0, err
	}

	res := db.Model(&Event{}).
		Where("status = ? AND locked_at IS NOT NULL AND locked_at < ?",
			StatusProcessing, threshold).
		Updates(map[string]interface{}{
			"status":     StatusPending,
			"retry_cnt":  gorm.Expr("retry_cnt + 1"),
			"locked_by":  "",
			"locked_at":  nil,
			"last_error": "lock timeout, requeued",
		})
	if res.Error != nil {
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

type statusCount struct {
	Status int8  `gorm:"column:status"`
	Cnt    int64 `gorm:"column:cnt"`
}

func (d *GormRepo) Stats(ctx context.Context) (map[int8]int64, error) {
	var rows []statusCount
	err := d.db.WithContext(ctx).
		Model(&Event{}).
		Select("status, count(*) AS cnt").
		Group("status").
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}

	result := map[int8]int64{
		StatusPending:    0,
		StatusSent:       0,
		StatusExhausted:  0,
		StatusProcessing: 0,
	}
	for _, r := range rows {
		result[r.Status] = r.Cnt
	}
	return result, nil
}

func TruncateUTF8(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) <= max {
		return s
	}

	cut := s[:max]
	for len(cut) > 0 && !validTail(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}

func validTail(s string) bool {
	if s == "" {
		return true
	}
	for i := len(s) - 1; i >= 0 && i > len(s)-4; i-- {
		b := s[i]
		if b < 0x80 {
			return true
		}
		if b&0xC0 == 0xC0 {
			need := 0
			switch {
			case b&0xE0 == 0xC0:
				need = 2
			case b&0xF0 == 0xE0:
				need = 3
			case b&0xF8 == 0xF0:
				need = 4
			default:
				return false
			}
			return len(s)-i == need
		}
	}
	return true
}
