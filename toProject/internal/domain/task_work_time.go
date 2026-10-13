package domain

import (
	"context"
	"go.uber.org/zap"
	"test.com/others/errs"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
	"time"
)

type TaskWorkTimeDomain struct {
	taskWorkTimeRepo repo.TaskWorkTimeRepo
	userRpcDomain    *UserRpcDomain
}

func NewTaskWorkTimeDomain() *TaskWorkTimeDomain {
	return &TaskWorkTimeDomain{
		taskWorkTimeRepo: dao.NewTaskWorkTimeDao(),
		userRpcDomain:    NewUserRpcDomain(),
	}
}

func (d *TaskWorkTimeDomain) TaskWorkTimeList(ctx context.Context, taskCode int64) ([]*data.TaskWorkTimeDisplay, *errs.MError) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var list []*data.TaskWorkTime
	var err error
	list, err = d.taskWorkTimeRepo.FindWorkTimeList(c, taskCode)
	if err != nil {
		zap.L().Error("project task TaskWorkTimeList taskWorkTimeRepo.FindWorkTimeList error", zap.Error(err))
		return nil, model.DBError
	}
	if len(list) == 0 {
		return []*data.TaskWorkTimeDisplay{}, nil
	}

	var displayList []*data.TaskWorkTimeDisplay
	var mIdList []int64
	for _, v := range list {
		mIdList = append(mIdList, v.MemberCode)
	}

	_, mMap, err := d.userRpcDomain.MemberList(ctx, mIdList)
	if err != nil {
		return nil, errs.ToMError(err)
	}

	for _, v := range list {
		display := v.ToDisplay()
		message := mMap[v.MemberCode]
		m := data.Member{}
		if message != nil {
			m.Name = message.Name
			m.Id = message.Id
			m.Avatar = message.Avatar
			m.Code = message.Code
		}
		display.Member = m
		displayList = append(displayList, display)
	}

	return displayList, nil
}

func (d *TaskWorkTimeDomain) TaskWorkTimeStat(ctx context.Context, taskCode int64) (*data.WorkTimeStat, error) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var stat *data.WorkTimeStat
	var err error
	stat, err = d.taskWorkTimeRepo.TaskWorkTimeStat(c, taskCode)
	if err != nil {
		zap.L().Error("TaskWorkTimeStat taskWorkTimeRepo.TaskWorkTimeStat error", zap.Error(err))
		return nil, model.DBError
	}

	if stat == nil {
		stat = &data.WorkTimeStat{
			TotalWorkTime: 0,
			AvgWorkTime:   0,
			MaxWorkTime:   0,
			MinWorkTime:   0,
		}
	}

	return stat, nil
}
