package domain

import (
	"context"

	"test.com/others/errs"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"

	"go.uber.org/zap"
)

type TaskDomain struct {
	taskRepo repo.TaskRepo
}

func NewTaskDomain() *TaskDomain {
	return &TaskDomain{
		taskRepo: dao.NewTaskDao(),
	}
}

func (d *TaskDomain) FindProjectIdByTaskId(ctx context.Context, taskId int64) (int64, bool, *errs.MError) {
	zap.L().Info("TaskDomain.FindProjectIdByTaskId", zap.Int64("taskId", taskId))
	task, err := d.taskRepo.FindTaskById(ctx, taskId)
	if err != nil {
		zap.L().Error("TaskDomain.FindProjectIdByTaskId.taskRepo.FindTaskById",
			zap.Int64("taskId", taskId), zap.Error(err))
		return 0, false, model.DBError
	}
	if task == nil {
		return 0, false, nil
	}
	return task.ProjectCode, true, nil
}
