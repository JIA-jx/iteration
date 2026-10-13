package outbox

import "time"

const (
	StatusPending    int8 = 0
	StatusSent       int8 = 1
	StatusExhausted  int8 = 2
	StatusProcessing int8 = 3
)

type Event struct {
	Id          int64      `gorm:"primaryKey;autoIncrement" json:"id"`
	Topic       string     `gorm:"column:topic;size:64;not null" json:"topic"`
	MsgKey      string     `gorm:"column:msg_key;size:128" json:"msg_key"`
	Payload     []byte     `gorm:"column:payload;type:mediumblob;not null" json:"payload"`
	Status      int8       `gorm:"column:status;not null;default:0" json:"status"`
	RetryCnt    int        `gorm:"column:retry_cnt;not null;default:0" json:"retry_cnt"`
	CreatedAt   time.Time  `gorm:"column:created_at;not null" json:"created_at"`
	SentAt      *time.Time `gorm:"column:sent_at" json:"sent_at"`
	LockedBy    string     `gorm:"column:locked_by;size:64" json:"locked_by"`
	LockedAt    *time.Time `gorm:"column:locked_at" json:"locked_at"`
	NextRetryAt *time.Time `gorm:"column:next_retry_at" json:"next_retry_at"`
	LastError   string     `gorm:"column:last_error;size:512" json:"last_error"`
}

func (Event) TableName() string {
	return "outbox_event"
}
