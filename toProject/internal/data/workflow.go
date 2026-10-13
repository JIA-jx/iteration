package data

import "time"

const (
	WorkflowDefinitionDisabled int8 = 0
	WorkflowDefinitionEnabled  int8 = 1
)

type WorkflowDefinition struct {
	Id             int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	TenantId       int64     `gorm:"column:tenant_id;not null;index:idx_tenant_key" json:"tenant_id"`
	DefKey         string    `gorm:"column:def_key;size:64;not null;index:idx_tenant_key" json:"def_key"`
	Name           string    `gorm:"column:name;size:128;not null" json:"name"`
	Version        int       `gorm:"column:version;not null;index:idx_tenant_key" json:"version"`
	DefinitionJson string    `gorm:"column:definition_json;type:mediumtext;not null" json:"definition_json"`
	Status         int8      `gorm:"column:status;not null;default:1" json:"status"`
	Remark         string    `gorm:"column:remark;size:512" json:"remark"`
	CreatedAt      time.Time `gorm:"column:created_at;not null" json:"created_at"`
	UpdatedAt      time.Time `gorm:"column:updated_at;not null" json:"updated_at"`
}

func (WorkflowDefinition) TableName() string {
	return "workflow_definition"
}

type WorkflowInstance struct {
	Id                string     `gorm:"column:id;size:64;primaryKey" json:"id"`
	TenantId          int64      `gorm:"column:tenant_id;not null;index:idx_tenant_biz" json:"tenant_id"`
	DefinitionKey     string     `gorm:"column:definition_key;size:64;not null" json:"definition_key"`
	DefinitionVersion int        `gorm:"column:definition_version;not null" json:"definition_version"`
	BizId             string     `gorm:"column:biz_id;size:128;not null;index:idx_tenant_biz" json:"biz_id"`
	SubmitterId       int64      `gorm:"column:submitter_id;not null;index:idx_submitter" json:"submitter_id"`
	Status            string     `gorm:"column:status;size:16;not null;index:idx_status" json:"status"`
	CurrentKey        string     `gorm:"column:current_key;size:64" json:"current_key"`
	InstanceJson      string     `gorm:"column:instance_json;type:mediumtext;not null" json:"instance_json"`
	Version           int64      `gorm:"column:version;not null;default:1" json:"version"`
	CreatedAt         time.Time  `gorm:"column:created_at;not null" json:"created_at"`
	UpdatedAt         time.Time  `gorm:"column:updated_at;not null" json:"updated_at"`
	FinishedAt        *time.Time `gorm:"column:finished_at" json:"finished_at"`
}

func (WorkflowInstance) TableName() string {
	return "workflow_instance"
}

type WorkflowTask struct {
	Id         string     `gorm:"column:id;size:64;primaryKey" json:"id"`
	InstanceId string     `gorm:"column:instance_id;size:64;not null;index:idx_instance" json:"instance_id"`
	TenantId   int64      `gorm:"column:tenant_id;not null;index:idx_approver" json:"tenant_id"`
	NodeKey    string     `gorm:"column:node_key;size:64;not null" json:"node_key"`
	NodeName   string     `gorm:"column:node_name;size:128" json:"node_name"`
	ApproverId int64      `gorm:"column:approver_id;not null;index:idx_approver" json:"approver_id"`
	Status     string     `gorm:"column:status;size:16;not null;index:idx_approver" json:"status"`
	Comment    string     `gorm:"column:comment;size:512" json:"comment"`
	CreatedAt  time.Time  `gorm:"column:created_at;not null" json:"created_at"`
	HandledAt  *time.Time `gorm:"column:handled_at" json:"handled_at"`
	TimeoutAt  *time.Time `gorm:"column:timeout_at" json:"timeout_at"`
}

func (WorkflowTask) TableName() string {
	return "workflow_task"
}
