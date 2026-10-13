package dao

import (
	"context"
	"errors"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type ProjectLogDao struct {
	conn *gorms.GormConn
}

func (p *ProjectLogDao) FindLogByMemberCode(ctx context.Context, memberId int64, page int64, size int64) (list []*data.ProjectLog, total int64, err error) {
	if memberId <= 0 || page <= 0 || size <= 0 {
		return nil, 0, errors.New("invalid params")
	}

	session := p.conn.Slave().WithContext(ctx)
	offset := (page - 1) * size

	err = session.Model(&data.ProjectLog{}).
		Where("member_code = ?", memberId).
		Order("create_time DESC").
		Limit(int(size)).
		Offset(int(offset)).
		Find(&list).Error
	if err != nil {
		return nil, 0, err
	}

	err = session.Model(&data.ProjectLog{}).
		Where("member_code = ?", memberId).
		Count(&total).Error

	return list, total, err
}

func (p *ProjectLogDao) SaveProjectLog(ctx context.Context, pl *data.ProjectLog) error {
	return p.conn.Session(ctx).Save(&pl).Error
}

func (p *ProjectLogDao) FindLogByTaskCode(ctx context.Context, taskCode int64, comment int) (list []*data.ProjectLog, total int64, err error) {
	if taskCode <= 0 {
		return nil, 0, errors.New("invalid task code")
	}

	session := p.conn.Slave().WithContext(ctx)
	model := session.Model(&data.ProjectLog{})

	if comment == 1 {
		model = model.Where("source_code = ? AND is_comment = ?", taskCode, comment)
	} else {
		model = model.Where("source_code = ?", taskCode)
	}

	err = model.Find(&list).Error
	if err != nil {
		return nil, 0, err
	}

	err = model.Count(&total).Error
	return list, total, err
}

func (p *ProjectLogDao) FindLogByTaskCodePage(ctx context.Context, taskCode int64, comment int, page int, pageSize int) (list []*data.ProjectLog, total int64, err error) {
	if taskCode <= 0 || page <= 0 || pageSize <= 0 {
		return nil, 0, errors.New("invalid params")
	}

	session := p.conn.Slave().WithContext(ctx)
	model := session.Model(&data.ProjectLog{})
	offset := (page - 1) * pageSize

	if comment == 1 {
		model = model.Where("source_code = ? AND is_comment = ?", taskCode, comment)
	} else {
		model = model.Where("source_code = ?", taskCode)
	}

	err = model.
		Limit(pageSize).
		Offset(offset).
		Find(&list).Error
	if err != nil {
		return nil, 0, err
	}

	countSession := p.conn.Slave().WithContext(ctx).Model(&data.ProjectLog{})
	if comment == 1 {
		countSession = countSession.Where("source_code = ? AND is_comment = ?", taskCode, comment)
	} else {
		countSession = countSession.Where("source_code = ?", taskCode)
	}
	err = countSession.Count(&total).Error
	return list, total, err
}

func NewProjectLogDao() *ProjectLogDao {
	return &ProjectLogDao{
		conn: gorms.New(),
	}
}
