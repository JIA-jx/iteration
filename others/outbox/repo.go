package outbox

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

var ErrLockLost = errors.New("outbox: lock lost")

type Repo interface {
	Save(ctx context.Context, tx *gorm.DB, e *Event) error
	ClaimPending(ctx context.Context, lockToken string, limit int, maxRetry int) ([]*Event, error)
	MarkSent(ctx context.Context, id int64, lockToken string) error
	MarkRetry(ctx context.Context, id int64, lockToken string, maxRetry int, backoffSeconds int64, reason string) error
	MarkExhausted(ctx context.Context, id int64, lockToken string, reason string) error
	RecoverStale(ctx context.Context, lockTimeoutSeconds int64, maxRetry int) (int64, error)
	Stats(ctx context.Context) (map[int8]int64, error)
}
