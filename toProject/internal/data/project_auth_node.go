package data

type ProjectAuthNode struct {
	Id       int64
	Auth     int64
	Node     string
	TenantId int64 `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
}

func (*ProjectAuthNode) TableName() string {
	return "iteration_auth_node"
}
