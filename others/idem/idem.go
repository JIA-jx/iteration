package idem

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

const (
	StatusProcessing = "processing"
	StatusSucceeded  = "succeeded"
	StatusFailed     = "failed"
)

const DefaultTTL = 24 * time.Hour

var (
	ErrKeyEmpty     = errors.New("idem: key is empty")
	ErrInProgress   = errors.New("idem: operation is still in progress")
	ErrRecordMissed = errors.New("idem: lock acquired but record missing")
)

type Record struct {
	Key       string    `json:"key"`
	Status    string    `json:"status"`
	Result    string    `json:"result"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	ExpireAt  time.Time `json:"expire_at"`
}

type Store interface {
	TryAcquire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Get(ctx context.Context, key string) (*Record, error)
	Complete(ctx context.Context, key, result string, ttl time.Duration) error
	Fail(ctx context.Context, key, reason string, ttl time.Duration) error
	Release(ctx context.Context, key string) error
}

type Executor func(ctx context.Context) (string, error)

type Outcome struct {
	Executed bool
	Replayed bool
	Result   string
}

type Guard struct {
	store Store
	ttl   time.Duration
}

func NewGuard(store Store, ttl time.Duration) *Guard {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Guard{store: store, ttl: ttl}
}

func (g *Guard) TTL() time.Duration {
	return g.ttl
}

func (g *Guard) Do(ctx context.Context, key string, fn Executor) (*Outcome, error) {
	return g.DoWithTTL(ctx, key, g.ttl, fn)
}

func (g *Guard) DoWithTTL(ctx context.Context, key string, ttl time.Duration, fn Executor) (*Outcome, error) {
	if key == "" {
		return nil, ErrKeyEmpty
	}
	if fn == nil {
		return nil, errors.New("idem: executor is nil")
	}
	if ttl <= 0 {
		ttl = g.ttl
	}

	acquired, err := g.store.TryAcquire(ctx, key, ttl)
	if err != nil {
		return nil, err
	}

	if acquired {
		return g.execute(ctx, key, ttl, fn)
	}

	rec, err := g.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, ErrRecordMissed
	}

	switch rec.Status {
	case StatusSucceeded:
		idemReplayed.Inc()
		return &Outcome{Replayed: true, Result: rec.Result}, nil
	case StatusFailed:
		idemConflict.Inc()
		return nil, ErrInProgress
	default:
		idemConflict.Inc()
		return nil, ErrInProgress
	}
}

func (g *Guard) execute(ctx context.Context, key string, ttl time.Duration, fn Executor) (*Outcome, error) {
	result, execErr := fn(ctx)

	bg, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if execErr != nil {
		idemExecuted.WithLabelValues("failed").Inc()
		_ = g.store.Release(bg, key)
		return nil, execErr
	}

	idemExecuted.WithLabelValues("succeeded").Inc()
	if err := g.store.Complete(bg, key, result, ttl); err != nil {
		return &Outcome{Executed: true, Result: result}, err
	}
	return &Outcome{Executed: true, Result: result}, nil
}

func Key(scope, bizId string) string {
	if scope == "" {
		return bizId
	}
	if bizId == "" {
		return scope
	}
	return scope + ":" + bizId
}

func NewToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return hex.EncodeToString([]byte(time.Now().String()))
	}
	return hex.EncodeToString(b)
}
