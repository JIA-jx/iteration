package domain

import (
	"context"

	"test.com/toProject/internal/data"
)

type stubProjectRepo struct {
	project *data.Project
	err     error
}

func (s *stubProjectRepo) FindProjectById(ctx context.Context, projectCode int64) (*data.Project, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.project == nil {
		return nil, nil
	}
	return s.project, nil
}

type stubDependencyRepo struct {
	list          []*data.TaskDependency
	saved         []*data.TaskDependency
	saveErr       error
	deleteErr     error
	deletedIds    []int64
	deletedPairs  [][3]int64
	deletedByTask []int64
}

func (s *stubDependencyRepo) Save(ctx context.Context, td *data.TaskDependency) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saved = append(s.saved, td)
	return nil
}

func (s *stubDependencyRepo) Delete(ctx context.Context, id int64) (int64, error) {
	if s.deleteErr != nil {
		return 0, s.deleteErr
	}
	s.deletedIds = append(s.deletedIds, id)
	return 1, nil
}

func (s *stubDependencyRepo) ListByProject(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error) {
	return s.list, nil
}

func (s *stubDependencyRepo) ListByTask(ctx context.Context, taskCode int64) ([]*data.TaskDependency, error) {
	return s.list, nil
}

func (s *stubDependencyRepo) DeleteByTask(ctx context.Context, taskCode int64) (int64, error) {
	s.deletedByTask = append(s.deletedByTask, taskCode)
	return 1, nil
}

func (s *stubDependencyRepo) DeleteByTaskPair(ctx context.Context, projectCode, taskCode, preTaskCode int64) (int64, error) {
	if s.deleteErr != nil {
		return 0, s.deleteErr
	}
	s.deletedPairs = append(s.deletedPairs, [3]int64{projectCode, taskCode, preTaskCode})
	return 1, nil
}

type recordedSchedule struct {
	taskCode  int64
	beginTime int64
	endTime   int64
}

type stubSchedulePersister struct {
	records []recordedSchedule
	err     error
}

func (s *stubSchedulePersister) UpdateTaskSchedule(ctx context.Context, taskCode int64, beginTime, endTime int64) error {
	if s.err != nil {
		return s.err
	}
	s.records = append(s.records, recordedSchedule{taskCode: taskCode, beginTime: beginTime, endTime: endTime})
	return nil
}

func testProject(code int64) *data.Project {
	return &data.Project{Id: code, Name: "P"}
}
