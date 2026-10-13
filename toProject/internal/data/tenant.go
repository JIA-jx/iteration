package data

import "time"

type Tenant struct {
	Id             int64      `gorm:"primaryKey;autoIncrement" json:"id"`
	Code           string     `gorm:"column:code;size:64;not null" json:"code"`
	Name           string     `gorm:"column:name;size:128;not null" json:"name"`
	Status         int8       `gorm:"column:status;not null;default:1" json:"status"`
	MemberQuota    int        `gorm:"column:member_quota;not null;default:0" json:"member_quota"`
	ProjectQuota   int        `gorm:"column:project_quota;not null;default:0" json:"project_quota"`
	StorageQuotaMb int64      `gorm:"column:storage_quota_mb;not null;default:0" json:"storage_quota_mb"`
	AIQuotaTokens  int64      `gorm:"column:ai_quota_tokens;not null;default:0" json:"ai_quota_tokens"`
	AIUsedTokens   int64      `gorm:"column:ai_used_tokens;not null;default:0" json:"ai_used_tokens"`
	ExpireAt       *time.Time `gorm:"column:expire_at" json:"expire_at"`
	CreatedAt      time.Time  `gorm:"column:created_at;not null" json:"created_at"`
	UpdatedAt      time.Time  `gorm:"column:updated_at;not null" json:"updated_at"`
}

func (Tenant) TableName() string {
	return "tenant"
}
