package data

import (
	"gorm.io/gorm"
	"time"
)

type MinioFile struct {
	ID        int64          `gorm:"column:id;primaryKey"`
	Filename  string         `gorm:"column:filename"`
	FilePath  string         `gorm:"column:file_path"`
	Md5       string         `gorm:"column:md5"`
	Size      int64          `gorm:"column:size"`
	CreatedAt time.Time      `gorm:"column:created_at"`
	UpdatedAt time.Time      `gorm:"column:update_at"`
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at"`
	TenantId  int64          `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
}

func (*MinioFile) TableName() string {
	return "iteration_minio_file"
}
