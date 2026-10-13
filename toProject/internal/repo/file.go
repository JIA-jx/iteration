package repo

import (
	"context"
	"test.com/toProject/internal/data"
)

type FileRepo interface {
	Save(ctx context.Context, file *data.File) error
	FindByIds(background context.Context, ids []int64) (list []*data.File, err error)
	CheckMd5Exist(ctx context.Context, md5 string) bool
	SaveFileRecord(ctx context.Context, filename, md5 string, filesize int64) error
	GetFileList(ctx context.Context) ([]*data.MinioFile, error)
	GetFileByID(ctx context.Context, fileId int64) (*data.MinioFile, error)
	SumSizeByTenant(ctx context.Context) (int64, error)
	FindByMd5(ctx context.Context, md5 string) (*data.MinioFile, error)
}
