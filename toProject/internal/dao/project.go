package dao

import (
	"context"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"test.com/others/tenant"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/database/gorms"
	"test.com/toProject/pkg/model"
	"time"
)

type ProjectDao struct {
	conn *gorms.GormConn
}

func (p *ProjectDao) FindProjectByIds(ctx context.Context, pids []int64) (list []*data.Project, err error) {
	session := p.conn.Reader(ctx).WithContext(ctx)
	err = session.Model(&data.Project{}).
		Where("id in (?)", pids).
		Find(&list).Error
	return
}

func (p *ProjectDao) FindProjectById(ctx context.Context, projectCode int64) (pj *data.Project, err error) {
	pj = new(data.Project)
	err = p.conn.Reader(ctx).WithContext(ctx).
		Where("id = ?", projectCode).
		First(pj).
		Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return
}

func (p *ProjectDao) FindProjectMemberByPid(ctx context.Context, projectCode int64) (list []*data.ProjectMember, total int64, err error) {
	session := p.conn.Reader(ctx).WithContext(ctx)
	err = session.Model(&data.ProjectMember{}).
		Where("project_code=?", projectCode).
		Find(&list).
		Error
	if err != nil {
		return
	}
	err = p.conn.Reader(ctx).WithContext(ctx).
		Model(&data.ProjectMember{}).
		Where("project_code=?", projectCode).
		Count(&total).
		Error
	return
}

func (p *ProjectDao) UpdateProject(ctx context.Context, proj *data.Project) error {
	return p.conn.Session(ctx).
		Model(&data.Project{}).
		Where("id = ?", proj.Id).
		Select("name", "description", "cover", "private", "open_prefix", "open_begin_time",
			"open_task_private", "auto_update_schedule", "schedule", "template_code",
			"access_control_type", "white_list", "prefix", "archive", "begin_time", "end_time").
		Updates(proj).
		Error
}

func (p *ProjectDao) DeleteProjectCollect(ctx context.Context, memId int64, projectCode int64) error {
	return p.conn.Session(ctx).
		Where("member_code=? and project_code=?", memId, projectCode).
		Delete(&data.ProjectCollection{}).
		Error
}

func (p *ProjectDao) SaveProjectCollect(ctx context.Context, pc *data.ProjectCollection) error {
	return p.conn.Session(ctx).
		Save(&pc).
		Error
}

func (p *ProjectDao) UpdateDeletedProject(ctx context.Context, code int64, deleted bool) error {
	var val int64
	deletedTime := ""
	if deleted {
		val = 1
		deletedTime = time.Now().Format("2006-01-02 15:04:05")
	}

	return p.conn.Session(ctx).
		Model(&data.Project{}).
		Where("id = ?", code).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Updates(map[string]interface{}{
			"deleted":      val,
			"deleted_time": deletedTime,
		}).
		Error
}

func (p *ProjectDao) FindProjectByPIdAndMemId(ctx context.Context, projectCode int64, memberId int64) (*data.ProjectAndMember, error) {
	var pms *data.ProjectAndMember
	sql := `select a.*, b.project_code, b.member_code, b.join_time, b.is_owner, b.authorize
			from iteration a, iteration_project_member b
			where a.id = b.project_code and a.tenant_id = ? and b.member_code = ? and b.project_code = ? limit 1`
	err := p.conn.Reader(ctx).WithContext(ctx).
		Raw(sql, tenant.IdFromContext(ctx), memberId, projectCode).
		Scan(&pms).Error
	return pms, err
}

func (p *ProjectDao) FindCollectByPidAndMemId(ctx context.Context, projectCode int64, memberId int64) (bool, error) {
	var count int64
	err := p.conn.Reader(ctx).WithContext(ctx).
		Model(&data.ProjectCollection{}).
		Where("member_code = ? and project_code = ?", memberId, projectCode).
		Count(&count).Error
	return count > 0, err
}

func (p *ProjectDao) SaveProject(conn database.DbConn, ctx context.Context, pr *data.Project) error {
	return conn.(*gorms.GormConn).Tx(ctx).Save(&pr).Error
}

func (p *ProjectDao) SaveProjectMember(conn database.DbConn, ctx context.Context, pm *data.ProjectMember) error {
	return conn.(*gorms.GormConn).Tx(ctx).Save(&pm).Error
}

func (p ProjectDao) FindCollectProjectByMemId(ctx context.Context, memberId int64, page int64, size int64) ([]*data.ProjectAndMember, int64, error) {
	var pms []*data.ProjectAndMember
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	}
	index := (page - 1) * size

	sql := `select a.*, b.project_code, b.member_code, b.join_time, b.is_owner, b.authorize
			from iteration a
			join iteration_collection c on c.project_code = a.id
			left join iteration_project_member b on b.project_code = a.id and b.member_code = ?
			where a.tenant_id = ? and c.member_code = ? and a.deleted = 0
			order by a.sort limit ?,?`
	err := p.conn.Reader(ctx).WithContext(ctx).
		Raw(sql, memberId, tenant.IdFromContext(ctx), memberId, index, size).
		Scan(&pms).Error
	if err != nil {
		return nil, 0, err
	}

	var total int64
	err = p.conn.Reader(ctx).WithContext(ctx).
		Model(&data.ProjectCollection{}).
		Where("member_code = ?", memberId).
		Count(&total).Error

	return pms, total, err
}

func (p ProjectDao) FindProjectByMemId(ctx context.Context, memId int64, condition string, page int64, size int64) ([]*data.ProjectAndMember, int64, error) {
	var pms []*data.ProjectAndMember
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	}
	index := (page - 1) * size
	tenantId := tenant.IdFromContext(ctx)

	sql := fmt.Sprintf(`select a.*, b.project_code, b.member_code, b.join_time, b.is_owner, b.authorize
			from iteration a
			join iteration_project_member b on a.id = b.project_code
			where a.tenant_id = ? and b.member_code = ? %s order by a.sort limit ?,?`, condition)
	err := p.conn.Reader(ctx).WithContext(ctx).
		Raw(sql, tenantId, memId, index, size).
		Scan(&pms).Error
	if err != nil {
		return nil, 0, err
	}

	var total int64
	countSQL := fmt.Sprintf(`select count(*) from iteration a
			join iteration_project_member b on a.id = b.project_code
			where a.tenant_id = ? and b.member_code = ? %s`, condition)
	err = p.conn.Reader(ctx).WithContext(ctx).
		Raw(countSQL, tenantId, memId).
		Scan(&total).Error
	return pms, total, err
}

func (p ProjectDao) CountByTenant(ctx context.Context) (int64, error) {
	var total int64
	err := p.conn.Reader(ctx).WithContext(ctx).
		Model(&data.Project{}).
		Where("deleted = ?", 0).
		Count(&total).Error
	return total, err
}

func (p ProjectDao) FindRecycleBinList(ctx context.Context, page, pageSize int64) ([]*data.Project, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	offset := (page - 1) * pageSize

	var total int64
	if err := p.conn.Reader(ctx).WithContext(ctx).
		Model(&data.Project{}).
		Where("deleted = ?", model.Deleted).
		Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var list []*data.Project
	err := p.conn.Reader(ctx).WithContext(ctx).
		Model(&data.Project{}).
		Where("deleted = ?", model.Deleted).
		Order("deleted_time DESC, id DESC").
		Limit(int(pageSize)).
		Offset(int(offset)).
		Find(&list).Error
	if err != nil {
		return nil, 0, err
	}

	return list, total, nil
}

func NewProjectDao() *ProjectDao {
	return &ProjectDao{
		conn: gorms.New(),
	}
}
