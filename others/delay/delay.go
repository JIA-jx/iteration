package delay

import "context"

const (
	StatusPending   = "pending"
	StatusInflight  = "inflight"
	StatusDone      = "done"
	StatusCanceled  = "canceled"
	StatusFailed    = "failed"
	StatusRecovered = "recovered"
)

type Task struct {
	Id        string `json:"id"`
	Type      string `json:"type"`
	Payload   []byte `json:"payload"`
	RunAt     int64  `json:"run_at"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
	LastError string `json:"last_error,omitempty"`
}

type Due struct {
	Id       string
	Type     string
	Payload  []byte
	RunAt    int64
	Attempts int

	Owner string
}

type TaskType string

type Store interface {
	Enqueue(ctx context.Context, t *Task) error

	Cancel(ctx context.Context, id string) (bool, error)

	Claim(ctx context.Context, now int64, limit int, visibilityTimeout int64) ([]Due, error)

	Complete(ctx context.Context, id string, owner string) error

	Fail(ctx context.Context, id string, owner string, reason string, delayMs int64) error

	Release(ctx context.Context, id string, owner string) error

	Abandon(ctx context.Context, id string, owner string, reason string) error

	Recover(ctx context.Context, now int64, limit int) (int, error)

	Pending(ctx context.Context, id string) (string, error)

	PendingCount(ctx context.Context) (int64, error)

	InflightCount(ctx context.Context) (int64, error)
}

type Handler func(ctx context.Context, task Due) error

type HandlerFunc = Handler
