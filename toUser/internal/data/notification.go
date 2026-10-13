package data

import "time"

type Notification struct {
	Id        int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	Type      string    `gorm:"column:type;size:32;not null;index" json:"type"`
	Topic     string    `gorm:"column:topic;size:64;not null" json:"topic"`
	MsgKey    string    `gorm:"column:msg_key;size:128" json:"msg_key"`
	Payload   string    `gorm:"column:payload;type:text" json:"payload"`
	Status    int8      `gorm:"column:status;not null;default:0" json:"status"`
	CreatedAt time.Time `gorm:"column:created_at;not null" json:"created_at"`
}

func (Notification) TableName() string {
	return "notification"
}

type DeadLetter struct {
	Id        int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	Topic     string    `gorm:"column:topic;size:64;not null" json:"topic"`
	Reason    string    `gorm:"column:reason;size:512" json:"reason"`
	Payload   string    `gorm:"column:payload;type:text" json:"payload"`
	CreatedAt time.Time `gorm:"column:created_at;not null" json:"created_at"`
}

func (DeadLetter) TableName() string {
	return "dead_letter"
}
