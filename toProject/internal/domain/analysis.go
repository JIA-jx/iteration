package domain

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"math"
	"test.com/rpcOrgrpc/analysis"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
	"time"
)

type AnalysisDomain struct {
	taskRepo         repo.TaskRepo
	projectRepo      repo.ProjectRepo
	projectLogRepo   repo.ProjectLogRepo
	taskWorkTimeRepo repo.TaskWorkTimeRepo
	sourceLinkRepo   repo.SourceLinkRepo
}

const maxBurndownDays = 3660

var ErrBurndownRangeTooLong = errors.New("burndown range too long")

func dayAxis(beginSec, endSec int64) []int64 {
	start := time.Unix(beginSec, 0).In(time.Local)
	last := time.Unix(endSec, 0).In(time.Local)
	day := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.Local)

	days := make([]int64, 0, 32)
	for !day.After(last) {
		days = append(days, day.Unix())
		day = day.AddDate(0, 0, 1)
	}
	return days
}

func (d *AnalysisDomain) DeliveryRate(ctx context.Context, projectCode string, beginTime, endTime int64) (*data.DeliveryResult, error) {
	logger := zap.L().With(
		zap.String("projectCode", projectCode),
		zap.Int64("beginTime", beginTime),
		zap.Int64("endTime", endTime),
		zap.String("method", "AnalysisDomain.DeliveryRate"),
	)

	if projectCode == "" || beginTime <= 0 || endTime <= 0 || beginTime > endTime {
		logger.Warn("invalid parameter for delivery rate")
		return &data.DeliveryResult{}, errors.New("invalid parameter for delivery rate")
	}

	stat, err := d.taskRepo.CountTaskStatusGroup(ctx, projectCode, beginTime, endTime)
	if err != nil {
		logger.Error("failed to count task status", zap.Error(err))
		return &data.DeliveryResult{}, err
	}

	total := stat.Total

	if total == 0 {
		logger.Info("no task data in time range")
		return &data.DeliveryResult{}, nil
	}

	rate := float64(stat.Completed) / float64(total) * 100
	rate = math.Round(rate*100) / 100

	result := &data.DeliveryResult{
		Total:     total,
		Completed: stat.Completed,
		Doing:     stat.Doing,
		Overdue:   stat.Overdue,
		Rate:      rate,
	}

	logger.Info("delivery rate calculate success", zap.Any("result", result))
	return result, nil
}

func (d *AnalysisDomain) Burndown(ctx context.Context, projectCode string, beginTime int64, endTime int64) ([]*analysis.BurndownPoint, error) {
	if projectCode == "" || beginTime <= 0 || endTime <= 0 || beginTime > endTime {
		zap.L().Warn("burndown invalid parameter",
			zap.String("project", projectCode),
			zap.Int64("begin", beginTime),
			zap.Int64("end", endTime))
		return []*analysis.BurndownPoint{}, nil
	}

	if endTime/1000-beginTime/1000 > int64(maxBurndownDays)*86400 {
		zap.L().Warn("burndown range too long",
			zap.String("project", projectCode),
			zap.Int64("begin", beginTime),
			zap.Int64("end", endTime))
		return nil, ErrBurndownRangeTooLong
	}

	totalTask, err := d.taskRepo.CountByProjectAndTime(ctx, projectCode, beginTime, endTime)
	if err != nil {
		zap.L().Error("burndown count total task failed", zap.Error(err))
		return nil, err
	}

	if totalTask == 0 {
		return []*analysis.BurndownPoint{}, nil
	}

	dailyDoneList, err := d.taskRepo.BurndownDailyDone(ctx, projectCode, beginTime, endTime)
	if err != nil {
		zap.L().Error("burndown daily done query failed", zap.Error(err))
		return nil, err
	}

	daysUnix := dayAxis(beginTime/1000, endTime/1000)
	totalDays := len(daysUnix)
	if totalDays == 0 {
		return []*analysis.BurndownPoint{}, nil
	}

	points := make([]*analysis.BurndownPoint, 0, totalDays)
	realRemaining := totalTask

	for i, daySec := range daysUnix {
		var done int64
		for _, stat := range dailyDoneList {
			if stat.DayUnix == daySec {
				done = stat.Count
				break
			}
		}

		realRemaining -= done
		if realRemaining < 0 {
			realRemaining = 0
		}

		idealRemaining := totalTask - (totalTask * int64(i) / int64(totalDays))
		if idealRemaining < 0 {
			idealRemaining = 0
		}

		points = append(points, &analysis.BurndownPoint{
			Date:      int64(daySec * 1000),
			Remaining: realRemaining,
			Ideal:     idealRemaining,
		})
	}

	return points, nil
}

func (d *AnalysisDomain) TeamLoad(ctx context.Context, projectCode string, beginTime int64, endTime int64) ([]*analysis.TeamLoadMember, error) {
	if projectCode == "" {
		zap.L().Warn("TeamLoad parameter invalid: projectCode empty")
		return []*analysis.TeamLoadMember{}, nil
	}

	memberStats, err := d.taskRepo.TeamMemberTaskStat(ctx, projectCode, beginTime, endTime)
	if err != nil {
		zap.L().Error("TeamLoad query member stat failed", zap.Error(err))
		return nil, err
	}

	result := make([]*analysis.TeamLoadMember, 0, len(memberStats))
	for _, stat := range memberStats {
		var level int32 = 1
		var suggest string

		switch {
		case stat.TaskCount >= 15:
			level = 3
			suggest = "任务过载，建议拆分或协助"
		case stat.TaskCount >= 8:
			level = 2
			suggest = "任务适中，可正常推进"
		default:
			level = 1
			suggest = "负载较低，可分配更多任务"
		}

		result = append(result, &analysis.TeamLoadMember{
			MemberCode: stat.MemberCode,
			Name:       stat.Name,
			Avatar:     stat.Avatar,
			TaskCount:  stat.TaskCount,
			WorkHour:   stat.WorkHour,
			LoadLevel:  level,
			Suggest:    suggest,
		})
	}

	return result, nil
}

func (d *AnalysisDomain) TaskAIContext(ctx context.Context, taskCode string) (task *data.Task, logs []*data.TaskLog, members []*data.TaskMember, err error) {
	if taskCode == "" {
		zap.L().Warn("TaskAIContext taskCode is empty")
		return nil, nil, nil, errors.New("taskCode is required")
	}

	task, err = d.taskRepo.GetByCode(ctx, taskCode)
	if err != nil {
		zap.L().Error("get task failed", zap.String("taskCode", taskCode), zap.Error(err))
		return nil, nil, nil, err
	}
	if task == nil {
		return nil, nil, nil, errors.New("task not found")
	}

	logs, err = d.taskRepo.ListLogByTaskCode(ctx, taskCode)
	if err != nil {
		zap.L().Error("list task log failed", zap.String("taskCode", taskCode), zap.Error(err))
		return nil, nil, nil, err
	}

	members, err = d.taskRepo.ListMemberByTaskCode(ctx, taskCode)
	if err != nil {
		zap.L().Error("list task member failed", zap.String("taskCode", taskCode), zap.Error(err))
		return nil, nil, nil, err
	}

	return task, logs, members, nil
}

func NewAnalysisDomain() *AnalysisDomain {
	return &AnalysisDomain{
		taskRepo:         dao.NewTaskDao(),
		projectRepo:      dao.NewProjectDao(),
		projectLogRepo:   dao.NewProjectLogDao(),
		taskWorkTimeRepo: dao.NewTaskWorkTimeDao(),
		sourceLinkRepo:   dao.NewSourceLinkDao(),
	}
}
