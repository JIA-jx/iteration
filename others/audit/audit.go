package audit

import (
	"context"
	"encoding/json"
	"time"
)

type Event struct {
	Id         int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	TenantId   int64     `gorm:"column:tenant_id;not null;index:idx_tenant_created" json:"tenant_id"`
	ActorId    int64     `gorm:"column:actor_id;not null" json:"actor_id"`
	ActorName  string    `gorm:"column:actor_name;size:64" json:"actor_name"`
	Action     string    `gorm:"column:action;size:64;not null;index:idx_action" json:"action"`
	Resource   string    `gorm:"column:resource;size:64;not null" json:"resource"`
	ResourceId string    `gorm:"column:resource_id;size:128;not null;index:idx_resource" json:"resource_id"`
	Before     string    `gorm:"column:before_data;type:mediumtext" json:"before"`
	After      string    `gorm:"column:after_data;type:mediumtext" json:"after"`
	TraceId    string    `gorm:"column:trace_id;size:64;index:idx_trace" json:"trace_id"`
	Ip         string    `gorm:"column:ip;size:64" json:"ip"`
	Remark     string    `gorm:"column:remark;size:512" json:"remark"`
	CreatedAt  time.Time `gorm:"column:created_at;not null;index:idx_tenant_created" json:"created_at"`
}

func (Event) TableName() string {
	return "audit_event"
}

type Writer interface {
	Write(ctx context.Context, events ...*Event) error
}

type Query struct {
	TenantId   int64
	Action     string
	Resource   string
	ResourceId string
	ActorId    int64
	TraceId    string
	BeginTime  time.Time
	EndTime    time.Time
	Page       int
	PageSize   int
}

type Reader interface {
	List(ctx context.Context, q Query) ([]*Event, int64, error)
}

func Diff(before, after interface{}) (string, string) {
	return mustJSON(before), mustJSON(after)
}

func mustJSON(v interface{}) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	if string(b) == "null" {
		return ""
	}
	return string(b)
}
