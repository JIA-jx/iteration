package dao

import (
	"context"
	"errors"
	"test.com/others/tenant"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"

	"gorm.io/gorm"
)

type FileDao struct {
	conn *gorms.GormConn
}

func (f *FileDao) GetFileList(ctx context.Context) ([]*data.MinioFile, error) {
	var list []*data.MinioFile
	err := f.conn.Slave().WithContext(ctx).
		Table("iteration_minio_file").
		Order("id desc").
		Find(&list).Error
	return list, err
}

func (f *FileDao) CheckMd5Exist(ctx context.Context, md5 string) bool {
	var count int64
	err := f.conn.Slave().WithContext(ctx).
		Model(&data.MinioFile{}).
		Where("md5 = ?", md5).
		Count(&count).Error
	if err != nil {
		return false
	}
	return count > 0
}

func (f *FileDao) SaveFileRecord(ctx context.Context, filename, md5 string, filesize int64) error {
	session := f.conn.Session(ctx)
	file := &data.MinioFile{
		Filename: filename,
		FilePath: filename,
		Md5:      md5,
		Size:     filesize,
	}
	return session.Model(&data.MinioFile{}).Create(file).Error
}

func (f *FileDao) FindByMd5(ctx context.Context, md5 string) (*data.MinioFile, error) {
	if md5 == "" {
		return nil, nil
	}
	var row data.MinioFile
	err := f.conn.Slave().WithContext(ctx).
		Model(&data.MinioFile{}).
		Where("md5 = ?", md5).
		Order("id ASC").
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (f *FileDao) SumSizeByTenant(ctx context.Context) (int64, error) {
	var total int64
	err := f.conn.Slave().WithContext(ctx).
		Model(&data.MinioFile{}).
		Where("tenant_id = ?", tenant.IdFromContext(ctx)).
		Select("COALESCE(SUM(size), 0)").
		Scan(&total).Error
	return total, err
}

func (f *FileDao) FindByIds(ctx context.Context, ids []int64) (list []*data.File, err error) {
	session := f.conn.Slave().WithContext(ctx)
	err = session.Model(&data.File{}).Where("id in (?)", ids).Find(&list).Error
	return
}

func (f *FileDao) Save(ctx context.Context, file *data.File) error {
	err := f.conn.Session(ctx).Save(&file).Error
	return err
}

func (f *FileDao) GetFileByID(ctx context.Context, fileId int64) (*data.MinioFile, error) {
	var info data.MinioFile
	err := f.conn.Slave().WithContext(ctx).
		Where("id = ?", fileId).
		First(&info).Error
	return &info, err
}

func NewFileDao() *FileDao {
	return &FileDao{
		conn: gorms.New(),
	}
}
