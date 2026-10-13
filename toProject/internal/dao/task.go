package dao

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
	"test.com/others/encrypts"
	"test.com/others/tenant"
	"test.com/others/tms"
	"test.com/rpcOrgrpc/analysis"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/database/gorms"
)

func ParseTaskCode(code string) int64 {
	if v, err := strconv.ParseInt(strings.TrimSpace(code), 10, 64); err == nil && v > 0 {
		return v
	}
	return encrypts.DecryptNoErr(code)
}

type TaskDao struct {
	conn *gorms.GormConn
}

func (t *TaskDao) BurndownDailyDone(ctx context.Context, projectCode string, beginTime, endTime int64) ([]*data.BurndownDailyDoneStat, error) {
	var stats []*data.BurndownDailyDoneStat

	err := t.conn.Reader(ctx).WithContext(ctx).
		Table("iteration_task").
		Where("project_code = ?", projectCode).
		Where("tenant_id = ?", tenant.IdFromContext(ctx)).
		Where("done_time BETWEEN ? AND ?", beginTime, endTime).
		Where("done = ?", 1).
		Where("deleted = ?", 0).
		Select(`
			UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(done_time / 1000))) AS day_unix,
			COUNT(*) AS count
		`).
		Group("day_unix").
		Scan(&stats).Error

	if err != nil {
		return nil, err
	}

	return stats, nil
}

func (t *TaskDao) GetByCode(ctx context.Context, taskCode string) (*data.Task, error) {
	id := ParseTaskCode(taskCode)
	if id <= 0 {
		return nil, nil
	}

	var task data.Task
	err := t.conn.Reader(ctx).WithContext(ctx).
		Model(&data.Task{}).
		Where("id = ?", id).
		First(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (t *TaskDao) ListMemberByTaskCode(ctx context.Context, taskCode string) ([]*data.TaskMember, error) {
	id := ParseTaskCode(taskCode)
	if id <= 0 {
		return nil, nil
	}

	var members []*data.TaskMember
	err := t.conn.Reader(ctx).WithContext(ctx).
		Model(&data.TaskMember{}).
		Where("task_code = ?", id).
		Find(&members).Error
	if err != nil {
		return nil, err
	}
	return members, nil
}

func (t *TaskDao) ListLogByTaskCode(ctx context.Context, taskCode string) ([]*data.TaskLog, error) {
	id := ParseTaskCode(taskCode)
	if id <= 0 {
		return nil, nil
	}

	var rows []*data.ProjectLog
	if err := t.conn.Reader(ctx).WithContext(ctx).
		Model(&data.ProjectLog{}).
		Where("source_code = ?", id).
		Order("create_time asc").
		Find(&rows).Error; err != nil {
		return nil, err
	}

	logs := make([]*data.TaskLog, 0, len(rows))
	for _, r := range rows {
		logs = append(logs, &data.TaskLog{
			Id:           r.Id,
			TaskCode:     strconv.FormatInt(r.SourceCode, 10),
			MemberCode:   strconv.FormatInt(r.MemberCode, 10),
			Content:      r.Content,
			Remark:       r.Remark,
			Type:         r.Type,
			CreateTime:   tms.FormatByMilli(r.CreateTime),
			ActionType:   r.ActionType,
			ToMemberCode: strconv.FormatInt(r.ToMemberCode, 10),
			ProjectCode:  strconv.FormatInt(r.ProjectCode, 10),
			Icon:         r.Icon,
			IsComment:    r.IsComment,
			IsRobot:      r.IsRobot,
		})
	}
	return logs, nil
}

func (t *TaskDao) TeamMemberTaskStat(ctx context.Context, projectCode string, beginTime, endTime int64) ([]*data.MemberTaskStat, error) {
	var stats []*data.MemberTaskStat

	err := t.conn.Reader(ctx).WithContext(ctx).
		Model(&data.Task{}).
		Select(`
			iteration_task.assign_to AS member_code,
			COALESCE(m.name, '') AS name,
			COALESCE(m.avatar, '') AS avatar,
			COUNT(*) AS task_count,
			COALESCE(SUM(iteration_task.schedule), 0) AS work_hour
		`).
		Joins("LEFT JOIN iteration_member m ON m.id = iteration_task.assign_to").
		Where("iteration_task.project_code = ?", projectCode).
		Where("iteration_task.create_time BETWEEN ? AND ?", beginTime, endTime).
		Where("iteration_task.assign_to > 0").
		Where("iteration_task.deleted = ?", 0).
		Group("iteration_task.assign_to, m.name, m.avatar").
		Scan(&stats).Error

	if err != nil {
		return nil, err
	}

	return stats, nil
}

func (t *TaskDao) FindTaskByIdWithVersion(ctx context.Context, id int64) (*data.Task, error) {
	var task data.Task
	err := t.conn.Reader(ctx).WithContext(ctx).Where("id = ?", id).First(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (t *TaskDao) UpdateTaskSortWithVersion(ctx context.Context, conn database.DbConn, task *data.Task) (rowsAffected int64, err error) {
	session := t.conn.Session(ctx)
	if conn != nil {
		session = conn.(*gorms.GormConn).Tx(ctx)
	}

	result := session.Model(&data.Task{}).
		Where("id = ? AND version_code = ?", task.Id, task.VersionCode).
		UpdateColumns(map[string]interface{}{
			"sort":         task.Sort,
			"stage_code":   task.StageCode,
			"version_code": task.VersionCode + 1,
		})
	return result.RowsAffected, result.Error
}

func (t *TaskDao) FindTaskByIds(ctx context.Context, taskIdList []int64) (list []*data.Task, err error) {
	session := t.conn.Reader(ctx).WithContext(ctx)
	err = session.Model(&data.Task{}).Where("id in (?)", taskIdList).Find(&list).Error
	return
}

func (t *TaskDao) CountByProjectAndTime(ctx context.Context, projectCode string, beginTime int64, endTime int64) (int64, error) {
	var count int64
	err := t.conn.Reader(ctx).WithContext(ctx).
		Model(&data.Task{}).
		Where("project_code = ?", projectCode).
		Where("create_time >= ? AND create_time <= ?", beginTime, endTime).
		Where("deleted = ?", 0).
		Count(&count).Error

	if err != nil {
		return 0, err
	}

	return count, nil
}

func (t *TaskDao) CountTaskStatusGroup(ctx context.Context, projectCode string, beginTime, endTime int64) (*data.DeliveryResult, error) {
	var stat data.DeliveryResult

	err := t.conn.Reader(ctx).WithContext(ctx).
		Model(&data.Task{}).
		Where("project_code = ?", projectCode).
		Where("create_time >= ? AND create_time <= ?", beginTime, endTime).
		Where("deleted = ?", 0).
		Select(`
			COUNT(*) AS total,
			COALESCE(SUM(CASE WHEN done = ? THEN 1 ELSE 0 END), 0) AS completed,
			COALESCE(SUM(CASE WHEN done = ? AND execute_status = ? THEN 1 ELSE 0 END), 0) AS doing,
			COALESCE(SUM(CASE WHEN done = ? AND end_time > 0 AND end_time < ? THEN 1 ELSE 0 END), 0) AS overdue
		`, 1, 0, data.Doing, 0, time.Now().UnixMilli()).
		Scan(&stat).Error

	if err != nil {
		return nil, err
	}

	return &stat, nil
}

func (t *TaskDao) BurndownByDay(ctx context.Context, projectCode string) ([]*analysis.BurndownPoint, error) {
	var points []*analysis.BurndownPoint

	err := t.conn.Reader(ctx).WithContext(ctx).
		Table("iteration_task").
		Where("project_code = ?", projectCode).
		Where("deleted = ?", 0).
		Select(`
			DATE_FORMAT(FROM_UNIXTIME(create_time / 1000), '%Y-%m-%d') AS date,
			COUNT(*) AS total,
			SUM(CASE WHEN done != 1 THEN 1 ELSE 0 END) AS remain
		`).
		Group("date").
		Order("date ASC").
		Scan(&points).Error

	if err != nil {
		return nil, err
	}

	return points, nil
}

func (t *TaskDao) FindTaskMemberPage(ctx context.Context, taskCode int64, page int64, size int64) (list []*data.TaskMember, total int64, err error) {
	session := t.conn.Reader(ctx).WithContext(ctx)
	offset := (page - 1) * size
	err = session.Model(&data.TaskMember{}).
		Where("task_code=?", taskCode).
		Limit(int(size)).Offset(int(offset)).
		Find(&list).Error
	err = session.Model(&data.TaskMember{}).
		Where("task_code=?", taskCode).
		Count(&total).Error
	return
}

func (t *TaskDao) FindTaskByAssignTo(ctx context.Context, memberId int64, done int, page int64, size int64) (tsList []*data.Task, total int64, err error) {
	session := t.conn.Reader(ctx).WithContext(ctx)
	offset := (page - 1) * size
	err = session.Model(&data.Task{}).Where("assign_to=? and deleted=0 and done=?", memberId, done).Limit(int(size)).Offset(int(offset)).Find(&tsList).Error
	err = session.Model(&data.Task{}).Where("assign_to=? and deleted=0 and done=?", memberId, done).Count(&total).Error
	return
}

func (t *TaskDao) FindTaskByMemberCode(ctx context.Context, memberId int64, done int, page int64, size int64) (tList []*data.Task, total int64, err error) {
	session := t.conn.Reader(ctx).WithContext(ctx)
	offset := (page - 1) * size

	base := func() *gorm.DB {
		return session.Model(&data.Task{}).
			Joins("JOIN iteration_task_member b ON b.task_code = iteration_task.id").
			Where("b.member_code = ? AND iteration_task.deleted = 0 AND iteration_task.done = ?", memberId, done)
	}

	err = base().Limit(int(size)).Offset(int(offset)).Find(&tList).Error
	if err != nil {
		return nil, 0, err
	}
	err = base().Count(&total).Error
	return
}

func (t *TaskDao) FindTaskByCreateBy(ctx context.Context, memberId int64, done int, page int64, size int64) (tList []*data.Task, total int64, err error) {
	session := t.conn.Reader(ctx).WithContext(ctx)
	offset := (page - 1) * size
	err = session.Model(&data.Task{}).Where("create_by=? and deleted=0 and done=?", memberId, done).Limit(int(size)).Offset(int(offset)).Find(&tList).Error
	err = session.Model(&data.Task{}).Where("create_by=? and deleted=0 and done=?", memberId, done).Count(&total).Error
	return
}

func (t *TaskDao) FindTaskByStageCodeLtSort(ctx context.Context, stageCode int, sort int) (ts *data.Task, err error) {
	var task data.Task
	session := t.conn.Reader(ctx).WithContext(ctx)
	err = session.Where("stage_code=? and sort < ? and deleted = 0", stageCode, sort).Order("sort desc").Limit(1).Find(&task).Error
	if err != nil {
		return nil, err
	}
	if task.Id == 0 {
		return nil, nil
	}
	return &task, nil
}

func (t *TaskDao) UpdateTaskSort(ctx context.Context, conn database.DbConn, ts *data.Task) error {
	session := t.conn.Session(ctx)
	if conn != nil {
		session = conn.(*gorms.GormConn).Tx(ctx)
	}
	return session.Model(&data.Task{}).
		Where("id=?", ts.Id).
		Select("sort", "stage_code").
		Updates(&ts).
		Error
}

func (t *TaskDao) UpdateTaskFieldsWithVersion(ctx context.Context, taskId int64, versionCode int64, fields map[string]interface{}) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	res := t.conn.Session(ctx).
		Model(&data.Task{}).
		Where("id = ? AND version_code = ?", taskId, versionCode).
		Updates(fields)
	return res.RowsAffected, res.Error
}

func (t *TaskDao) UpdateTaskFields(ctx context.Context, taskId int64, fields map[string]interface{}) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	res := t.conn.Session(ctx).
		Model(&data.Task{}).
		Where("id = ?", taskId).
		Updates(fields)
	return res.RowsAffected, res.Error
}
func (t *TaskDao) FindTaskById(ctx context.Context, taskCode int64) (ts *data.Task, err error) {
	var task data.Task
	err = t.conn.Reader(ctx).WithContext(ctx).Where("id=?", taskCode).First(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (t *TaskDao) SaveTask(ctx context.Context, conn database.DbConn, ts *data.Task) error {
	return conn.(*gorms.GormConn).Tx(ctx).Save(&ts).Error
}

func (t *TaskDao) SaveTaskMember(ctx context.Context, conn database.DbConn, tm *data.TaskMember) error {
	return conn.(*gorms.GormConn).Tx(ctx).Save(&tm).Error
}

func (t *TaskDao) FindTaskSort(ctx context.Context, projectCode int64, stageCode int64) (v *int, err error) {
	session := t.conn.Reader(ctx).WithContext(ctx)
	err = session.Model(&data.Task{}).
		Where("project_code=? and stage_code=?", projectCode, stageCode).
		Select("max(sort)").Scan(&v).Error
	if err == nil && v == nil {
		zero := 0
		v = &zero
	}
	return
}

func (t *TaskDao) FindTaskMaxIdNum(ctx context.Context, projectCode int64) (v *int, err error) {
	session := t.conn.Reader(ctx).WithContext(ctx)
	err = session.Model(&data.Task{}).
		Where("project_code=?", projectCode).
		Select("max(id_num)").Scan(&v).Error
	if err == nil && v == nil {
		zero := 0
		v = &zero
	}
	return
}

func (t *TaskDao) FindTaskMemberByTaskId(ctx context.Context, taskCode int64, memberId int64) (task *data.TaskMember, err error) {
	var tm data.TaskMember
	err = t.conn.Reader(ctx).WithContext(ctx).
		Where("task_code=? and member_code=?", taskCode, memberId).
		Limit(1).
		Find(&tm).Error
	if err != nil {
		return nil, err
	}
	if tm.Id == 0 {
		return nil, nil
	}
	return &tm, nil
}

func (t *TaskDao) FindTaskByStageCode(ctx context.Context, stageCode int) (list []*data.Task, err error) {
	session := t.conn.Reader(ctx).WithContext(ctx)
	err = session.Model(&data.Task{}).
		Where("stage_code=? and deleted =0", stageCode).
		Order("sort asc").
		Find(&list).Error
	return
}

func (t *TaskDao) FindTaskMemberByTaskIds(ctx context.Context, taskIds []int64, memberId int64) ([]*data.TaskMember, error) {
	var list []*data.TaskMember
	err := t.conn.Reader(ctx).WithContext(ctx).
		Where("task_code IN (?)", taskIds).
		Where("member_code = ?", memberId).
		Find(&list).Error
	return list, err
}

func NewTaskDao() *TaskDao {
	return &TaskDao{
		conn: gorms.New(),
	}
}
