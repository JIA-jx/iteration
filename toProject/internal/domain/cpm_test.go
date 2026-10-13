package domain

import (
	"context"
	"errors"
	"strings"
	"testing"

	"test.com/toProject/internal/data"
)

func testTaskDependency(id, projectCode, taskCode, preTaskCode int64, depType string, lag int64) *data.TaskDependency {
	return &data.TaskDependency{
		Id:          id,
		ProjectCode: projectCode,
		TaskCode:    taskCode,
		PreTaskCode: preTaskCode,
		Type:        depType,
		Lag:         lag,
	}
}

func mustBuildGraph(t *testing.T, projectCode int64, tasks []*data.Task, dependencies []*data.TaskDependency) *Graph {
	t.Helper()
	graph, err := BuildDAG(projectCode, tasks, dependencies)
	if err != nil {
		t.Fatalf("BuildDAG returned error: %v", err)
	}
	return graph
}

func assertInt64Slice(t *testing.T, label string, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", label, got, want)
		}
	}
}

func changeCodes(changes []ScheduleChange) []int64 {
	codes := make([]int64, 0, len(changes))
	for _, change := range changes {
		codes = append(codes, change.TaskCode)
	}
	return codes
}

func TestNormalizeDependencyType(t *testing.T) {
	for _, depType := range []string{"FS", "fs", " fs ", data.DependencyTypeFS} {
		normalized, err := NormalizeDependencyType(depType)
		if err != nil {
			t.Fatalf("NormalizeDependencyType(%q) error: %v", depType, err)
		}
		if normalized != data.DependencyTypeFS {
			t.Fatalf("NormalizeDependencyType(%q) = %q, want FS", depType, normalized)
		}
	}

	normalized, err := NormalizeDependencyType("")
	if err != nil || normalized != DefaultDependencyType {
		t.Fatalf("empty type normalized = %q, err = %v, want default", normalized, err)
	}

	ss, err := NormalizeDependencyType("ss")
	if err != nil || ss != data.DependencyTypeSS {
		t.Fatalf("ss normalized = %q, err = %v", ss, err)
	}

	for _, unsupported := range []string{"FF", "SF", "ff", "sf"} {
		_, err := NormalizeDependencyType(unsupported)
		var target *UnsupportedDependencyError
		if !errors.As(err, &target) {
			t.Fatalf("NormalizeDependencyType(%q) error = %v, want UnsupportedDependencyError", unsupported, err)
		}
		if !strings.Contains(strings.ToUpper(err.Error()), strings.ToUpper(unsupported)) {
			t.Fatalf("error %q does not mention %q", err.Error(), unsupported)
		}
	}

	if _, err := NormalizeDependencyType("XX"); err == nil {
		t.Fatal("expected error for unknown dependency type")
	}
}

func TestBuildDAGShape(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 2, data.Started, data.Done, 1),
		testTask(2, 10, 2, 4, data.Started, data.Doing, 0),
		testTask(3, 10, 4, 6, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 0),
		testTaskDependency(2, 9, 3, 2, data.DependencyTypeFS, 0),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)

	if graph.NodeCount() != 3 {
		t.Fatalf("node count = %d, want 3", graph.NodeCount())
	}
	if graph.EdgeCount() != 2 {
		t.Fatalf("edge count = %d, want 2", graph.EdgeCount())
	}
	assertInt64Slice(t, "task codes", graph.TaskCodes(), []int64{1, 2, 3})
	assertInt64Slice(t, "successors of 1", graph.Nodes[1].Successors, []int64{2})
	assertInt64Slice(t, "predecessors of 2", graph.Nodes[2].Predecessors, []int64{1})

	order, err := graph.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort error: %v", err)
	}
	assertInt64Slice(t, "topological order", order, []int64{1, 2, 3})
}

func TestBuildDAGSkipsForeignTasksAndDuplicates(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 2, data.Started, data.Done, 1),
		nil,
		testTask(1, 10, 0, 2, data.Started, data.Done, 1),
	}
	dependencies := []*data.TaskDependency{
		nil,
		testTaskDependency(1, 9, 99, 1, data.DependencyTypeFS, 0),
		testTaskDependency(2, 9, 2, 1, data.DependencyTypeFS, 0),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)
	if graph.NodeCount() != 1 {
		t.Fatalf("node count = %d, want 1", graph.NodeCount())
	}
	if graph.EdgeCount() != 0 {
		t.Fatalf("edge count = %d, want 0", graph.EdgeCount())
	}
}

func TestBuildDAGRejectsSelfReference(t *testing.T) {
	tasks := []*data.Task{testTask(1, 10, 0, 2, data.Started, data.Done, 1)}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 1, 1, data.DependencyTypeFS, 0),
	}

	_, err := BuildDAG(9, tasks, dependencies)
	if !errors.Is(err, ErrDependencySelfRef) {
		t.Fatalf("error = %v, want %v", err, ErrDependencySelfRef)
	}
}

func TestBuildDAGRejectsUnsupportedType(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 2, data.Started, data.Done, 1),
		testTask(2, 10, 2, 4, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeFF, 0),
	}

	_, err := BuildDAG(9, tasks, dependencies)
	var target *UnsupportedDependencyError
	if !errors.As(err, &target) {
		t.Fatalf("error = %v, want UnsupportedDependencyError", err)
	}
}

func TestBuildDAGEmptyProject(t *testing.T) {
	graph := mustBuildGraph(t, 3, nil, nil)
	if graph.NodeCount() != 0 {
		t.Fatalf("node count = %d, want 0", graph.NodeCount())
	}
	order, err := graph.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort error: %v", err)
	}
	if len(order) != 0 {
		t.Fatalf("order = %v, want empty", order)
	}
}

func TestTopologicalSortDetectsCycle(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 1, data.NoStarted, data.Wait, 0),
		testTask(2, 10, 1, 2, data.NoStarted, data.Wait, 0),
		testTask(3, 10, 2, 3, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 0),
		testTaskDependency(2, 9, 3, 2, data.DependencyTypeFS, 0),
		testTaskDependency(3, 9, 1, 3, data.DependencyTypeFS, 0),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)
	order, err := graph.TopologicalSort()
	if err == nil {
		t.Fatal("expected a cycle error")
	}
	if order != nil {
		t.Fatalf("order = %v, want nil on cycle", order)
	}

	var cycleErr *DependencyCycleError
	if !errors.As(err, &cycleErr) {
		t.Fatalf("error = %v, want DependencyCycleError", err)
	}
	if strings.Contains(cycleErr.Path(), "->  ->") {
		t.Fatalf("cycle path looks malformed: %q", cycleErr.Path())
	}
	if cycleErr.Path() != "1 -> 2 -> 3 -> 1" {
		t.Fatalf("cycle path = %q, want %q", cycleErr.Path(), "1 -> 2 -> 3 -> 1")
	}
	if !strings.Contains(err.Error(), "任务依赖存在环") {
		t.Fatalf("error message = %q, want a readable cycle description", err.Error())
	}
}

func TestTopologicalSortDetectsTwoNodeCycle(t *testing.T) {
	tasks := []*data.Task{
		testTask(7, 10, 0, 1, data.NoStarted, data.Wait, 0),
		testTask(8, 10, 1, 2, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 7, 8, data.DependencyTypeFS, 0),
		testTaskDependency(2, 9, 8, 7, data.DependencyTypeSS, 0),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)
	_, err := graph.TopologicalSort()
	var cycleErr *DependencyCycleError
	if !errors.As(err, &cycleErr) {
		t.Fatalf("error = %v, want DependencyCycleError", err)
	}
	if cycleErr.Path() != "7 -> 8 -> 7" {
		t.Fatalf("cycle path = %q, want %q", cycleErr.Path(), "7 -> 8 -> 7")
	}
}

func TestCriticalPathAnalyzesTouchingDurations(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 4, data.NoStarted, data.Wait, 0),
		testTask(2, 10, 0, 5, data.NoStarted, data.Wait, 0),
		testTask(3, 10, 5, 8, data.NoStarted, data.Wait, 0),
		testTask(4, 10, 8, 11, data.NoStarted, data.Wait, 0),
		testTask(5, 10, 11, 12, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 3, 1, data.DependencyTypeFS, 0),
		testTaskDependency(2, 9, 3, 2, data.DependencyTypeFS, 0),
		testTaskDependency(3, 9, 4, 3, data.DependencyTypeFS, 0),
		testTaskDependency(4, 9, 5, 4, data.DependencyTypeSS, testMillisPerDay),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)
	result, err := graph.CriticalPath()
	if err != nil {
		t.Fatalf("CriticalPath error: %v", err)
	}

	if result.ProjectDuration != 11*testMillisPerDay {
		t.Fatalf("project duration = %d, want %d", result.ProjectDuration, 11*testMillisPerDay)
	}
	assertInt64Slice(t, "critical path", result.CriticalPath, []int64{2, 3, 4})

	var nilResult *CPMResult
	if nilResult.ScheduleOf(1) != nil {
		t.Fatal("ScheduleOf on nil result should be nil")
	}

	a := result.ScheduleOf(1)
	if a == nil {
		t.Fatal("missing schedule for task 1")
	}
	if a.EarliestStart != 0 || a.EarliestFinish != 4*testMillisPerDay {
		t.Fatalf("A es/ef = %d/%d, want 0/%d", a.EarliestStart, a.EarliestFinish, 4*testMillisPerDay)
	}
	if a.Float != 3*testMillisPerDay {
		t.Fatalf("A float = %d, want %d", a.Float, 3*testMillisPerDay)
	}
	if a.Critical {
		t.Fatal("A should not be critical")
	}
	if a.LatestStart != testMillisPerDay || a.LatestFinish != 5*testMillisPerDay {
		t.Fatalf("A ls/lf = %d/%d, want %d/%d", a.LatestStart, a.LatestFinish, testMillisPerDay, 5*testMillisPerDay)
	}

	b := result.ScheduleOf(2)
	if !b.Critical || b.Float != 0 {
		t.Fatalf("B critical/float = %v/%d, want true/0", b.Critical, b.Float)
	}
	if b.EarliestFinish != 5*testMillisPerDay || b.LatestFinish != 5*testMillisPerDay {
		t.Fatalf("B ef/lf = %d/%d, want %d", b.EarliestFinish, b.LatestFinish, 5*testMillisPerDay)
	}

	e := result.ScheduleOf(5)
	if e.EarliestStart != 12*testMillisPerDay || e.EarliestFinish != 13*testMillisPerDay {
		t.Fatalf("E es/ef = %d/%d, want %d/%d", e.EarliestStart, e.EarliestFinish, 12*testMillisPerDay, 13*testMillisPerDay)
	}
	if e.Float != testMillisPerDay {
		t.Fatalf("E float = %d, want %d", e.Float, testMillisPerDay)
	}
	if result.TaskCount != 5 {
		t.Fatalf("task count = %d, want 5", result.TaskCount)
	}
	if result.DependencyCount != 4 {
		t.Fatalf("dependency count = %d, want 4", result.DependencyCount)
	}
	if len(result.Schedules) != 5 {
		t.Fatalf("schedule entries = %d, want 5", len(result.Schedules))
	}
	assertInt64Slice(t, "critical tasks", result.CriticalTasks, []int64{2, 3, 4})
}

func TestCriticalPathPicksOneOfMultipleCriticalBranches(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 4, data.NoStarted, data.Wait, 0),
		testTask(2, 10, 0, 4, data.NoStarted, data.Wait, 0),
	}
	graph := mustBuildGraph(t, 9, tasks, nil)

	result, err := graph.CriticalPath()
	if err != nil {
		t.Fatalf("CriticalPath error: %v", err)
	}
	if result.ProjectDuration != 4*testMillisPerDay {
		t.Fatalf("project duration = %d, want %d", result.ProjectDuration, 4*testMillisPerDay)
	}
	assertInt64Slice(t, "critical path", result.CriticalPath, []int64{1})
	assertInt64Slice(t, "critical tasks", result.CriticalTasks, []int64{1, 2})
}

func TestCriticalPathCycleReturnsError(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 1, data.NoStarted, data.Wait, 0),
		testTask(2, 10, 1, 2, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 0),
		testTaskDependency(2, 9, 1, 2, data.DependencyTypeFS, 0),
	}
	graph := mustBuildGraph(t, 9, tasks, dependencies)

	_, err := graph.CriticalPath()
	var cycleErr *DependencyCycleError
	if !errors.As(err, &cycleErr) {
		t.Fatalf("error = %v, want DependencyCycleError", err)
	}
}

func TestForwardPassHonoursFSLag(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 2, data.NoStarted, data.Wait, 0),
		testTask(2, 10, 2, 4, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 3*testMillisPerDay),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)
	order, info, projectDuration, err := graph.ComputeSchedule()
	if err != nil {
		t.Fatalf("ComputeSchedule error: %v", err)
	}
	if len(order) != 2 {
		t.Fatalf("order = %v, want 2 nodes", order)
	}
	if projectDuration != 7*testMillisPerDay {
		t.Fatalf("project duration = %d, want %d", projectDuration, 7*testMillisPerDay)
	}
	if info[1].EarliestStart != 0 || info[1].EarliestFinish != 2*testMillisPerDay {
		t.Fatalf("A es/ef = %d/%d, want 0/%d", info[1].EarliestStart, info[1].EarliestFinish, 2*testMillisPerDay)
	}
	if info[2].EarliestStart != 5*testMillisPerDay {
		t.Fatalf("B es = %d, want %d", info[2].EarliestStart, 5*testMillisPerDay)
	}

	result, err := graph.CriticalPath()
	if err != nil {
		t.Fatalf("CriticalPath error: %v", err)
	}
	assertInt64Slice(t, "critical path", result.CriticalPath, []int64{2})

	a := result.ScheduleOf(1)
	if a.Float != 3*testMillisPerDay || a.Critical {
		t.Fatalf("A float/critical = %d/%v, want %d/false", a.Float, a.Critical, 3*testMillisPerDay)
	}
	if a.LatestStart != 0 || a.LatestFinish != 2*testMillisPerDay {
		t.Fatalf("A ls/lf = %d/%d, want 0/%d", a.LatestStart, a.LatestFinish, 2*testMillisPerDay)
	}

	b := result.ScheduleOf(2)
	if b.Float != 0 || !b.Critical {
		t.Fatalf("B float/critical = %d/%v, want 0/true", b.Float, b.Critical)
	}
	if b.EarliestStart != 5*testMillisPerDay || b.LatestStart != 5*testMillisPerDay {
		t.Fatalf("B es/ls = %d/%d, want %d", b.EarliestStart, b.LatestStart, 5*testMillisPerDay)
	}
	if b.EarliestFinish != 7*testMillisPerDay || b.LatestFinish != 7*testMillisPerDay {
		t.Fatalf("B ef/lf = %d/%d, want %d", b.EarliestFinish, b.LatestFinish, 7*testMillisPerDay)
	}
}

func TestForwardPassHonoursSSDependency(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 3, data.NoStarted, data.Wait, 0),
		testTask(2, 10, 0, 7, data.NoStarted, data.Wait, 0),
		testTask(3, 10, 5, 6, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeSS, 0),
		testTaskDependency(2, 9, 3, 2, data.DependencyTypeSS, 5*testMillisPerDay),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)
	_, info, projectDuration, err := graph.ComputeSchedule()
	if err != nil {
		t.Fatalf("ComputeSchedule error: %v", err)
	}
	if projectDuration != 7*testMillisPerDay {
		t.Fatalf("project duration = %d, want %d", projectDuration, 7*testMillisPerDay)
	}

	if info[2].EarliestStart != 0 {
		t.Fatalf("B es = %d, want 0", info[2].EarliestStart)
	}
	if info[3].EarliestStart != 5*testMillisPerDay {
		t.Fatalf("C es = %d, want %d", info[3].EarliestStart, 5*testMillisPerDay)
	}
	if info[3].EarliestFinish != 6*testMillisPerDay {
		t.Fatalf("C ef = %d, want %d", info[3].EarliestFinish, 6*testMillisPerDay)
	}

	result, err := graph.CriticalPath()
	if err != nil {
		t.Fatalf("CriticalPath error: %v", err)
	}
	assertInt64Slice(t, "critical path", result.CriticalPath, []int64{1, 2})
	assertInt64Slice(t, "critical tasks", result.CriticalTasks, []int64{1, 2})

	a := result.ScheduleOf(1)
	if !a.Critical || a.Float != 0 {
		t.Fatalf("A critical/float = %v/%d, want true/0", a.Critical, a.Float)
	}
	b := result.ScheduleOf(2)
	if !b.Critical || b.Float != 0 {
		t.Fatalf("B critical/float = %v/%d, want true/0", b.Critical, b.Float)
	}
	c := result.ScheduleOf(3)
	if c.Critical || c.Float != testMillisPerDay {
		t.Fatalf("C critical/float = %v/%d, want false/%d", c.Critical, c.Float, testMillisPerDay)
	}
}

func TestAutoScheduleShiftsSuccessorAndIsIdempotent(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 1, data.NoStarted, data.Wait, 0),
		testTask(2, 10, 0.5, 1.5, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 0),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)
	changes, err := graph.AutoSchedule()
	if err != nil {
		t.Fatalf("AutoSchedule error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %v, want exactly one change", changes)
	}
	change := changes[0]
	if change.TaskCode != 2 {
		t.Fatalf("changed task = %d, want 2", change.TaskCode)
	}
	if change.OriginalStart != testMillisPerDay/2 {
		t.Fatalf("original start = %d, want %d", change.OriginalStart, testMillisPerDay/2)
	}
	if change.NewStart != testMillisPerDay || change.NewEnd != 2*testMillisPerDay {
		t.Fatalf("new start/end = %d/%d, want %d/%d", change.NewStart, change.NewEnd, testMillisPerDay, 2*testMillisPerDay)
	}
	if change.ShiftMillis != testMillisPerDay/2 {
		t.Fatalf("shift = %d, want %d", change.ShiftMillis, testMillisPerDay/2)
	}

	secondGraph := mustBuildGraph(t, 9, tasks, dependencies)
	secondChanges, err := secondGraph.AutoSchedule()
	if err != nil {
		t.Fatalf("second AutoSchedule error: %v", err)
	}
	if len(secondChanges) != 1 {
		t.Fatalf("second run on the untouched graph produced %d changes, want 1", len(secondChanges))
	}

	tasks[1].BeginTime = change.NewStart
	tasks[1].EndTime = change.NewEnd
	appliedGraph := mustBuildGraph(t, 9, tasks, dependencies)
	appliedChanges, err := appliedGraph.AutoSchedule()
	if err != nil {
		t.Fatalf("AutoSchedule after applying error: %v", err)
	}
	if len(appliedChanges) != 0 {
		t.Fatalf("AutoSchedule after applying = %v, want empty (idempotent)", appliedChanges)
	}
}

func TestAutoScheduleCascadesAlongChain(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 1, data.NoStarted, data.Wait, 0),
		testTask(2, 10, 0.5, 1.0, data.NoStarted, data.Wait, 0),
		testTask(3, 10, 1.2, 2.2, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 0),
		testTaskDependency(2, 9, 3, 2, data.DependencyTypeFS, 0),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)
	changes, err := graph.AutoSchedule()
	if err != nil {
		t.Fatalf("AutoSchedule error: %v", err)
	}
	assertInt64Slice(t, "changed tasks", changeCodes(changes), []int64{2, 3})

	first := changes[0]
	if first.NewStart != testMillisPerDay || first.NewEnd != 3*testMillisPerDay/2 {
		t.Fatalf("task 2 new start/end = %d/%d, want %d/%d", first.NewStart, first.NewEnd, testMillisPerDay, 3*testMillisPerDay/2)
	}
	if first.ShiftMillis != testMillisPerDay/2 {
		t.Fatalf("task 2 shift = %d, want %d", first.ShiftMillis, testMillisPerDay/2)
	}

	second := changes[1]
	if second.NewStart != 3*testMillisPerDay/2 {
		t.Fatalf("task 3 new start = %d, want %d", second.NewStart, 3*testMillisPerDay/2)
	}
	if second.NewEnd != 5*testMillisPerDay/2 {
		t.Fatalf("task 3 new end = %d, want %d", second.NewEnd, 5*testMillisPerDay/2)
	}

	tasks[1].BeginTime = first.NewStart
	tasks[1].EndTime = first.NewEnd
	tasks[2].BeginTime = second.NewStart
	tasks[2].EndTime = second.NewEnd
	appliedGraph := mustBuildGraph(t, 9, tasks, dependencies)
	appliedChanges, err := appliedGraph.AutoSchedule()
	if err != nil {
		t.Fatalf("AutoSchedule after applying error: %v", err)
	}
	if len(appliedChanges) != 0 {
		t.Fatalf("second run = %v, want empty", appliedChanges)
	}
}

func TestAutoScheduleUsesLagAndSS(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 2, data.NoStarted, data.Wait, 0),
		testTask(2, 10, 1, 2, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeSS, 3*testMillisPerDay),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)
	changes, err := graph.AutoSchedule()
	if err != nil {
		t.Fatalf("AutoSchedule error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %v, want 1", changes)
	}
	if changes[0].NewStart != 3*testMillisPerDay {
		t.Fatalf("new start = %d, want %d", changes[0].NewStart, 3*testMillisPerDay)
	}
	if changes[0].NewEnd != 4*testMillisPerDay {
		t.Fatalf("new end = %d, want %d", changes[0].NewEnd, 4*testMillisPerDay)
	}
}

func TestAutoScheduleNoChangeWhenConstraintsSatisfied(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 1, data.Done, data.Done, 1),
		testTask(2, 10, 2, 3, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 0),
	}

	graph := mustBuildGraph(t, 9, tasks, dependencies)
	changes, err := graph.AutoSchedule()
	if err != nil {
		t.Fatalf("AutoSchedule error: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("changes = %v, want empty", changes)
	}
}

func TestAutoScheduleOnCycleReturnsCycleError(t *testing.T) {
	tasks := []*data.Task{
		testTask(1, 10, 0, 1, data.NoStarted, data.Wait, 0),
		testTask(2, 10, 1, 2, data.NoStarted, data.Wait, 0),
	}
	dependencies := []*data.TaskDependency{
		testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 0),
		testTaskDependency(2, 9, 1, 2, data.DependencyTypeFS, 0),
	}
	graph := mustBuildGraph(t, 9, tasks, dependencies)

	_, err := graph.AutoSchedule()
	var cycleErr *DependencyCycleError
	if !errors.As(err, &cycleErr) {
		t.Fatalf("error = %v, want DependencyCycleError", err)
	}
}

type trackingLoader struct {
	tasks        []*data.Task
	dependencies []*data.TaskDependency
	endTime      int64
}

func (t *trackingLoader) LoadProjectTasks(ctx context.Context, projectCode int64) ([]*data.Task, error) {
	return t.tasks, nil
}

func (t *trackingLoader) LoadProjectWorkTime(ctx context.Context, projectCode int64, tasks []*data.Task, beginTime int64, endTime int64) (map[int64]int64, error) {
	t.endTime = endTime
	return map[int64]int64{}, nil
}

func (t *trackingLoader) LoadProjectDependencies(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error) {
	return t.dependencies, nil
}

func newTestCPMDomain(loader *trackingLoader, depRepo *stubDependencyRepo, persister SchedulePersister) *CPMDomain {
	return &CPMDomain{
		records:     loader,
		depRepo:     depRepo,
		projectRepo: &stubProjectRepo{project: testProject(9)},
		persister:   persister,
	}
}

func TestCPMDomainAutoSchedulePersistsAndIsIdempotent(t *testing.T) {
	loader := &trackingLoader{
		tasks: []*data.Task{
			testTask(1, 10, 0, 1, data.NoStarted, data.Wait, 0),
			testTask(2, 10, 0.5, 1.5, data.NoStarted, data.Wait, 0),
		},
		dependencies: []*data.TaskDependency{
			testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 0),
		},
	}
	persister := &stubSchedulePersister{}
	domain := newTestCPMDomain(loader, &stubDependencyRepo{}, persister)

	first, err := domain.AutoSchedule(context.Background(), "9")
	if err != nil {
		t.Fatalf("AutoSchedule error: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first changes = %v, want 1", first)
	}
	if len(persister.records) != 1 {
		t.Fatalf("persisted records = %v, want 1", persister.records)
	}
	if persister.records[0].beginTime != testMillisPerDay || persister.records[0].endTime != 2*testMillisPerDay {
		t.Fatalf("persisted schedule = %+v, want begin=%d end=%d", persister.records[0], testMillisPerDay, 2*testMillisPerDay)
	}

	loader.tasks[1].BeginTime = persister.records[0].beginTime
	loader.tasks[1].EndTime = persister.records[0].endTime

	second, err := domain.AutoSchedule(context.Background(), "9")
	if err != nil {
		t.Fatalf("second AutoSchedule error: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second changes = %v, want empty (idempotent)", second)
	}
	if len(persister.records) != 1 {
		t.Fatalf("persisted records after second run = %v, want still 1", persister.records)
	}
}

func TestCPMDomainBuildGraphAndCriticalPath(t *testing.T) {
	loader := &trackingLoader{
		tasks: []*data.Task{
			testTask(1, 10, 0, 4, data.NoStarted, data.Wait, 0),
			testTask(2, 10, 0, 6, data.NoStarted, data.Wait, 0),
			testTask(3, 10, 6, 9, data.NoStarted, data.Wait, 0),
		},
		dependencies: []*data.TaskDependency{
			testTaskDependency(1, 9, 3, 1, data.DependencyTypeFS, 0),
			testTaskDependency(2, 9, 3, 2, data.DependencyTypeFS, 0),
		},
	}
	domain := newTestCPMDomain(loader, &stubDependencyRepo{}, &stubSchedulePersister{})

	graph, err := domain.BuildGraph(context.Background(), "9")
	if err != nil {
		t.Fatalf("BuildGraph error: %v", err)
	}
	if graph.NodeCount() != 3 {
		t.Fatalf("node count = %d, want 3", graph.NodeCount())
	}

	result, err := domain.CriticalPath(context.Background(), "9")
	if err != nil {
		t.Fatalf("CriticalPath error: %v", err)
	}
	if result.ProjectDuration != 9*testMillisPerDay {
		t.Fatalf("project duration = %d, want %d", result.ProjectDuration, 9*testMillisPerDay)
	}
	assertInt64Slice(t, "critical path", result.CriticalPath, []int64{1, 3})
	assertInt64Slice(t, "critical tasks", result.CriticalTasks, []int64{1, 2, 3})
}

func TestCPMDomainValidationAndErrors(t *testing.T) {
	domain := newTestCPMDomain(&trackingLoader{}, &stubDependencyRepo{}, &stubSchedulePersister{})
	ctx := context.Background()

	if _, err := domain.BuildGraph(ctx, ""); !errors.Is(err, ErrProjectCodeRequired) {
		t.Fatalf("empty project code error = %v, want %v", err, ErrProjectCodeRequired)
	}
	if _, err := domain.BuildGraph(ctx, "abc"); err == nil {
		t.Fatal("expected error for non numeric project code")
	}
	if _, err := domain.BuildGraph(ctx, "-3"); !errors.Is(err, ErrProjectCodeRequired) {
		t.Fatalf("negative project code error = %v, want %v", err, ErrProjectCodeRequired)
	}

	empty := newTestCPMDomain(&trackingLoader{}, &stubDependencyRepo{}, &stubSchedulePersister{})
	emptyResult, err := empty.CriticalPath(ctx, "9")
	if err != nil {
		t.Fatalf("CriticalPath on empty project error: %v", err)
	}
	if emptyResult.TaskCount != 0 || len(emptyResult.CriticalPath) != 0 {
		t.Fatalf("empty project result = %+v, want zero tasks", emptyResult)
	}
}

func TestCPMDomainValidateProject(t *testing.T) {
	domain := newTestCPMDomain(&trackingLoader{}, &stubDependencyRepo{}, &stubSchedulePersister{})
	if err := domain.ValidateProject(context.Background(), 9); err != nil {
		t.Fatalf("ValidateProject error: %v", err)
	}
	if err := domain.ValidateProject(context.Background(), 0); !errors.Is(err, ErrProjectCodeRequired) {
		t.Fatalf("zero project error = %v, want %v", err, ErrProjectCodeRequired)
	}

	missing := newTestCPMDomain(&trackingLoader{}, &stubDependencyRepo{}, &stubSchedulePersister{})
	missing.projectRepo = &stubProjectRepo{project: nil}
	if err := missing.ValidateProject(context.Background(), 9); !errors.Is(err, ErrProjectNotFound) {
		t.Fatalf("missing project error = %v, want %v", err, ErrProjectNotFound)
	}
}

func TestCPMDomainDependencyCrud(t *testing.T) {
	depRepo := &stubDependencyRepo{}
	domain := newTestCPMDomain(&trackingLoader{}, depRepo, &stubSchedulePersister{})
	ctx := context.Background()

	saved, err := domain.SaveDependency(ctx, 9, 2, 1, "fs", 60000)
	if err != nil {
		t.Fatalf("SaveDependency error: %v", err)
	}
	if saved.Type != data.DependencyTypeFS {
		t.Fatalf("saved type = %q, want FS", saved.Type)
	}
	if saved.Lag != 60000 {
		t.Fatalf("saved lag = %d, want 60000", saved.Lag)
	}
	if saved.CreatedAt.IsZero() {
		t.Fatal("saved dependency has zero CreatedAt")
	}
	if len(depRepo.saved) != 1 {
		t.Fatalf("saved dependencies = %d, want 1", len(depRepo.saved))
	}

	if _, err := domain.SaveDependency(ctx, 0, 2, 1, "FS", 0); !errors.Is(err, ErrProjectCodeRequired) {
		t.Fatalf("zero project error = %v, want %v", err, ErrProjectCodeRequired)
	}
	if _, err := domain.SaveDependency(ctx, 9, 0, 1, "FS", 0); err == nil {
		t.Fatal("expected error for empty task code")
	}
	if _, err := domain.SaveDependency(ctx, 9, 1, 1, "FS", 0); !errors.Is(err, ErrDependencySelfRef) {
		t.Fatalf("self reference error = %v, want %v", err, ErrDependencySelfRef)
	}
	if _, err := domain.SaveDependency(ctx, 9, 2, 1, "FF", 0); err == nil {
		t.Fatal("expected error for unsupported dependency type")
	}

	deleted, err := domain.DeleteDependency(ctx, 42, 0, 0, 0)
	if err != nil {
		t.Fatalf("DeleteDependency by id error: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted by id = %d, want 1", deleted)
	}
	assertInt64Slice(t, "deleted ids", depRepo.deletedIds, []int64{42})

	deleted, err = domain.DeleteDependency(ctx, 0, 9, 2, 1)
	if err != nil {
		t.Fatalf("DeleteDependency by pair error: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted by pair = %d, want 1", deleted)
	}
	if len(depRepo.deletedPairs) != 1 {
		t.Fatalf("deleted pairs = %v, want 1 entry", depRepo.deletedPairs)
	}

	if _, err := domain.DeleteDependency(ctx, 0, 9, 0, 0); err == nil {
		t.Fatal("expected error when neither id nor pair is provided")
	}

	depRepo.list = []*data.TaskDependency{testTaskDependency(1, 9, 2, 1, "FS", 0)}
	list, err := domain.ListDependency(ctx, 9)
	if err != nil {
		t.Fatalf("ListDependency error: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("dependency list = %v, want 1 entry", list)
	}
	if _, err := domain.ListDependency(ctx, 0); !errors.Is(err, ErrProjectCodeRequired) {
		t.Fatalf("ListDependency zero project error = %v, want %v", err, ErrProjectCodeRequired)
	}
}

func TestCPMDomainApplyScheduleChecksPersister(t *testing.T) {
	loader := &trackingLoader{
		tasks: []*data.Task{
			testTask(1, 10, 0, 1, data.NoStarted, data.Wait, 0),
			testTask(2, 10, 0.5, 1.5, data.NoStarted, data.Wait, 0),
		},
		dependencies: []*data.TaskDependency{
			testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 0),
		},
	}
	domain := newTestCPMDomain(loader, &stubDependencyRepo{}, nil)

	graph, err := domain.BuildGraphByCode(context.Background(), 9)
	if err != nil {
		t.Fatalf("BuildGraphByCode error: %v", err)
	}
	changes, err := domain.ApplySchedule(context.Background(), graph)
	if err != nil {
		t.Fatalf("ApplySchedule error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %v, want 1", changes)
	}

	if _, err := domain.ApplySchedule(context.Background(), nil); !errors.Is(err, ErrGraphEmpty) {
		t.Fatalf("nil graph error = %v, want %v", err, ErrGraphEmpty)
	}

	if err := domain.UpdateTaskSchedule(context.Background(), 1, 0, 0); err == nil {
		t.Fatal("expected error when schedule persister is not assembled")
	}
}

func TestCPMDomainUpdateTaskScheduleUsesPersister(t *testing.T) {
	persister := &stubSchedulePersister{}
	domain := newTestCPMDomain(&trackingLoader{}, &stubDependencyRepo{}, persister)
	if err := domain.UpdateTaskSchedule(context.Background(), 5, 1000, 2000); err != nil {
		t.Fatalf("UpdateTaskSchedule error: %v", err)
	}
	if len(persister.records) != 1 {
		t.Fatalf("records = %v, want 1", persister.records)
	}
	if persister.records[0].taskCode != 5 || persister.records[0].beginTime != 1000 || persister.records[0].endTime != 2000 {
		t.Fatalf("record = %+v, want task 5 1000..2000", persister.records[0])
	}
}

func TestCPMDomainSaveDependencySurfacesCycle(t *testing.T) {
	depRepo := &stubDependencyRepo{
		list: []*data.TaskDependency{
			testTaskDependency(1, 9, 2, 1, data.DependencyTypeFS, 0),
			testTaskDependency(2, 9, 1, 2, data.DependencyTypeFS, 0),
		},
	}
	loader := &trackingLoader{
		tasks: []*data.Task{
			testTask(1, 10, 0, 1, data.NoStarted, data.Wait, 0),
			testTask(2, 10, 1, 2, data.NoStarted, data.Wait, 0),
		},
		dependencies: depRepo.list,
	}
	domain := newTestCPMDomain(loader, depRepo, &stubSchedulePersister{})

	_, err := domain.SaveDependency(context.Background(), 9, 3, 1, "FS", 0)
	var cycleErr *DependencyCycleError
	if !errors.As(err, &cycleErr) {
		t.Fatalf("error = %v, want DependencyCycleError", err)
	}
	if len(depRepo.saved) != 1 {
		t.Fatalf("saved dependencies = %d, want 1", len(depRepo.saved))
	}
}

func TestCPMDomainValidateProjectPropagatesRepoError(t *testing.T) {
	sentinel := errors.New("db down")
	domain := &CPMDomain{projectRepo: &stubProjectRepo{err: sentinel}}
	if err := domain.ValidateProject(context.Background(), 9); !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want %v", err, sentinel)
	}
}

func TestGraphNilSafety(t *testing.T) {
	var graph *Graph
	if graph.NodeCount() != 0 {
		t.Fatal("nil graph node count should be 0")
	}
	if graph.EdgeCount() != 0 {
		t.Fatal("nil graph edge count should be 0")
	}
	if graph.TaskCodes() != nil {
		t.Fatal("nil graph task codes should be nil")
	}
	if _, err := graph.TopologicalSort(); !errors.Is(err, ErrGraphEmpty) {
		t.Fatalf("nil graph topological sort error = %v, want %v", err, ErrGraphEmpty)
	}
	if _, err := graph.AutoSchedule(); !errors.Is(err, ErrGraphEmpty) {
		t.Fatalf("nil graph auto schedule error = %v, want %v", err, ErrGraphEmpty)
	}
	if _, err := graph.CriticalPath(); !errors.Is(err, ErrGraphEmpty) {
		t.Fatalf("nil graph critical path error = %v, want %v", err, ErrGraphEmpty)
	}
}
