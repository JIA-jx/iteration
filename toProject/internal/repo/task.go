package repo

import (
	"context"
	"test.com/rpcOrgrpc/analysis"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database"
)

type TaskStagesTemplateRepo interface {
	FindInProTemIds(ctx context.Context, ids []int) ([]data.MsTaskStagesTemplate, error)
	FindByProjectTemplateId(ctx context.Context, projectTemplateCode int) (list []*data.MsTaskStagesTemplate, err error)
}
type TaskStagesRepo interface {
	SaveTaskStages(ctx context.Context, conn database.DbConn, ts *data.TaskStages) error
	FindStagesByProjectId(ctx context.Context, projectCode int64, page int64, pageSize int64) (list []*data.TaskStages, total int64, err error)
	FindById(ctx context.Context, id int) (ts *data.TaskStages, err error)
}

type TaskRepo interface {
	FindTaskMemberByTaskIds(ctx context.Context, taskIds []int64, memberId int64) ([]*data.TaskMember, error)
	FindTaskByStageCode(ctx context.Context, stageCode int) (list []*data.Task, err error)
	FindTaskMemberByTaskId(ctx context.Context, taskCode int64, memberId int64) (task *data.TaskMember, err error)
	FindTaskMaxIdNum(ctx context.Context, projectCode int64) (v *int, err error)
	FindTaskSort(ctx context.Context, projectCode int64, stageCode int64) (v *int, err error)
	SaveTask(ctx context.Context, conn database.DbConn, ts *data.Task) error
	SaveTaskMember(ctx context.Context, conn database.DbConn, tm *data.TaskMember) error
	FindTaskById(ctx context.Context, taskCode int64) (ts *data.Task, err error)
	UpdateTaskSort(ctx context.Context, conn database.DbConn, ts *data.Task) error
	FindTaskByStageCodeLtSort(ctx context.Context, stageCode int, sort int) (ts *data.Task, err error)
	FindTaskByAssignTo(ctx context.Context, memberId int64, done int, page int64, size int64) ([]*data.Task, int64, error)
	FindTaskByMemberCode(ctx context.Context, memberId int64, done int, page int64, size int64) (tList []*data.Task, total int64, err error)
	FindTaskByCreateBy(ctx context.Context, memberId int64, done int, page int64, size int64) (tList []*data.Task, total int64, err error)
	FindTaskMemberPage(ctx context.Context, taskCode int64, page int64, size int64) (list []*data.TaskMember, total int64, err error)
	FindTaskByIds(ctx context.Context, taskIdList []int64) (list []*data.Task, err error)
	FindTaskByIdWithVersion(ctx context.Context, id int64) (*data.Task, error)
	UpdateTaskSortWithVersion(ctx context.Context, conn database.DbConn, task *data.Task) (rowsAffected int64, err error)
	UpdateTaskFields(ctx context.Context, taskId int64, fields map[string]interface{}) (int64, error)
	UpdateTaskFieldsWithVersion(ctx context.Context, taskId int64, versionCode int64, fields map[string]interface{}) (int64, error)
	CountTaskStatusGroup(ctx context.Context, projectCode string, beginTime, endTime int64) (*data.DeliveryResult, error)
	BurndownByDay(ctx context.Context, projectCode string) ([]*analysis.BurndownPoint, error)
	BurndownDailyDone(ctx context.Context, projectCode string, beginTime, endTime int64) ([]*data.BurndownDailyDoneStat, error)
	CountByProjectAndTime(ctx context.Context, projectCode string, beginTime int64, endTime int64) (int64, error)
	TeamMemberTaskStat(ctx context.Context, projectCode string, beginTime, endTime int64) ([]*data.MemberTaskStat, error)
	GetByCode(ctx context.Context, taskCode string) (*data.Task, error)
	ListMemberByTaskCode(ctx context.Context, taskCode string) ([]*data.TaskMember, error)
	ListLogByTaskCode(ctx context.Context, taskCode string) ([]*data.TaskLog, error)
}
