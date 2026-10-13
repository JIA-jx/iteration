package domain

import (
	"context"

	"test.com/toProject/internal/data"
)

type RecordLoader interface {
	LoadProjectTasks(ctx context.Context, projectCode int64) ([]*data.Task, error)
	LoadProjectWorkTime(ctx context.Context, projectCode int64, tasks []*data.Task, beginTime int64, endTime int64) (map[int64]int64, error)
	LoadProjectDependencies(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error)
}

type ProjectRecordSource struct {
	taskRepo interface {
		ListTaskByProject(ctx context.Context, projectCode int64) ([]*data.Task, error)
	}
	workRepo interface {
		ListWorkTimeByTaskIds(ctx context.Context, taskIds []int64) ([]*data.TaskWorkTime, error)
	}
	depRepo interface {
		ListByProject(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error)
	}
}

func NewProjectRecordSource(
	taskRepo interface {
		ListTaskByProject(ctx context.Context, projectCode int64) ([]*data.Task, error)
	},
	workRepo interface {
		ListWorkTimeByTaskIds(ctx context.Context, taskIds []int64) ([]*data.TaskWorkTime, error)
	},
	depRepo interface {
		ListByProject(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error)
	},
) *ProjectRecordSource {
	return &ProjectRecordSource{
		taskRepo: taskRepo,
		workRepo: workRepo,
		depRepo:  depRepo,
	}
}

func (s *ProjectRecordSource) LoadProjectTasks(ctx context.Context, projectCode int64) ([]*data.Task, error) {
	tasks, err := s.taskRepo.ListTaskByProject(ctx, projectCode)
	if err != nil {
		return nil, err
	}
	filtered := make([]*data.Task, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		filtered = append(filtered, task)
	}
	return filtered, nil
}

func (s *ProjectRecordSource) LoadProjectWorkTime(ctx context.Context, projectCode int64, tasks []*data.Task, beginTime int64, endTime int64) (map[int64]int64, error) {
	byTask := make(map[int64]int64, len(tasks))
	if s.workRepo == nil || len(tasks) == 0 {
		return byTask, nil
	}

	taskIds := make([]int64, 0, len(tasks))
	for _, task := range tasks {
		if task != nil {
			taskIds = append(taskIds, task.Id)
		}
	}

	records, err := s.workRepo.ListWorkTimeByTaskIds(ctx, taskIds)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if record == nil {
			continue
		}
		ts := record.BeginTime
		if ts <= 0 {
			ts = record.CreateTime
		}
		if endTime > 0 {
			if record.BeginTime > endTime || record.CreateTime > endTime {
				continue
			}
		}
		if beginTime > 0 && ts > 0 && ts < beginTime {
			continue
		}
		byTask[record.TaskCode] += int64(record.Num)
	}
	return byTask, nil
}

func (s *ProjectRecordSource) LoadProjectDependencies(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error) {
	if s.depRepo == nil {
		return nil, nil
	}
	dependencies, err := s.depRepo.ListByProject(ctx, projectCode)
	if err != nil {
		return nil, err
	}
	filtered := make([]*data.TaskDependency, 0, len(dependencies))
	for _, dependency := range dependencies {
		if dependency == nil {
			continue
		}
		filtered = append(filtered, dependency)
	}
	return filtered, nil
}
