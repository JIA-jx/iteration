package domain

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"test.com/toProject/internal/data"
)

const testMillisPerDay = int64(24 * 60 * 60 * 1000)

func testTask(id int64, schedule int, beginDay, endDay float64, status, executeStatus, done int) *data.Task {
	return &data.Task{
		Id:            id,
		Name:          "T" + string(rune('0'+id)),
		Schedule:      schedule,
		BeginTime:     int64(beginDay * float64(testMillisPerDay)),
		EndTime:       int64(endDay * float64(testMillisPerDay)),
		Status:        status,
		ExecuteStatus: executeStatus,
		Done:          done,
	}
}

type fakeRecordLoader struct {
	tasks        []*data.Task
	dependencies []*data.TaskDependency
	workTime     map[int64]int64
	taskErr      error
	depErr       error
	workErr      error
	lastEndTime  int64
	lastProjects []int64
}

func (f *fakeRecordLoader) LoadProjectTasks(ctx context.Context, projectCode int64) ([]*data.Task, error) {
	f.lastProjects = append(f.lastProjects, projectCode)
	if f.taskErr != nil {
		return nil, f.taskErr
	}
	return f.tasks, nil
}

func (f *fakeRecordLoader) LoadProjectWorkTime(ctx context.Context, projectCode int64, tasks []*data.Task, beginTime int64, endTime int64) (map[int64]int64, error) {
	f.lastEndTime = endTime
	if f.workErr != nil {
		return nil, f.workErr
	}
	result := make(map[int64]int64, len(f.workTime))
	for code, minutes := range f.workTime {
		result[code] = minutes
	}
	return result, nil
}

func (f *fakeRecordLoader) LoadProjectDependencies(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error) {
	if f.depErr != nil {
		return nil, f.depErr
	}
	return f.dependencies, nil
}

func assertNear(t *testing.T, label string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 0.011 {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
}

func TestTaskCompletionStatusMapping(t *testing.T) {
	notStarted := testTask(1, 10, 0, 10, data.NoStarted, data.Wait, 0)
	if got := TaskCompletion(notStarted, 0); got != 0 {
		t.Fatalf("not started task completion = %v, want 0", got)
	}

	doing := testTask(2, 10, 0, 10, data.Started, data.Doing, 0)
	assertNear(t, "doing task completion", TaskCompletion(doing, 6), 0.6)

	overflow := testTask(3, 10, 0, 10, data.Started, data.Doing, 0)
	if got := TaskCompletion(overflow, 25); got != 1 {
		t.Fatalf("overworked task completion = %v, want 1", got)
	}

	doneByFlag := testTask(4, 10, 0, 10, data.Started, data.Doing, 1)
	if got := TaskCompletion(doneByFlag, 3); got != 1 {
		t.Fatalf("done task completion = %v, want 1", got)
	}

	doneByExecuteStatus := testTask(5, 10, 0, 10, data.Started, data.Done, 0)
	if got := TaskCompletion(doneByExecuteStatus, 0); got != 1 {
		t.Fatalf("execute status done completion = %v, want 1", got)
	}

	closed := testTask(6, 10, 0, 10, data.Started, data.Closed, 0)
	if got := TaskCompletion(closed, 0); got != 1 {
		t.Fatalf("closed task completion = %v, want 1", got)
	}

	zeroScheduleWorking := testTask(7, 0, 0, 10, data.Started, data.Doing, 0)
	if got := TaskCompletion(zeroScheduleWorking, 3); got != 1 {
		t.Fatalf("zero schedule working completion = %v, want 1", got)
	}

	zeroScheduleIdle := testTask(8, 0, 0, 10, data.NoStarted, data.Wait, 0)
	if got := TaskCompletion(zeroScheduleIdle, 0); got != 0 {
		t.Fatalf("zero schedule idle completion = %v, want 0", got)
	}

	if got := TaskCompletion(nil, 5); got != 0 {
		t.Fatalf("nil task completion = %v, want 0", got)
	}
}

func TestPlannedValueLinearSpread(t *testing.T) {
	task := testTask(1, 80, 0, 8, data.NoStarted, data.Wait, 0)

	assertNear(t, "pv before begin", PlannedValueOfTask(task, -1000), 0)
	assertNear(t, "pv at begin", PlannedValueOfTask(task, 0), 0)
	assertNear(t, "pv 1 day", PlannedValueOfTask(task, testMillisPerDay), 10)
	assertNear(t, "pv mid", PlannedValueOfTask(task, 4*testMillisPerDay), 40)
	assertNear(t, "pv at end", PlannedValueOfTask(task, 8*testMillisPerDay), 80)
	assertNear(t, "pv after end", PlannedValueOfTask(task, 20*testMillisPerDay), 80)

	noSchedule := testTask(2, 0, 0, 8, data.NoStarted, data.Wait, 0)
	assertNear(t, "pv zero schedule", PlannedValueOfTask(noSchedule, 4*testMillisPerDay), 0)

	noWindow := testTask(3, 30, 0, 0, data.NoStarted, data.Wait, 0)
	assertNear(t, "pv missing window before cutoff", PlannedValueOfTask(noWindow, 0), 30)
	assertNear(t, "pv missing window negative cutoff", PlannedValueOfTask(noWindow, -1), 0)

	reversed := testTask(4, 30, 8, 4, data.NoStarted, data.Wait, 0)
	assertNear(t, "pv reversed window at start", PlannedValueOfTask(reversed, 5*testMillisPerDay), 0)
	assertNear(t, "pv reversed window after start", PlannedValueOfTask(reversed, 9*testMillisPerDay), 30)

	if got := PlannedValueOfTask(nil, 5*testMillisPerDay); got != 0 {
		t.Fatalf("nil task planned value = %v, want 0", got)
	}
}

func TestCalculateEVMCoreQuantities(t *testing.T) {
	snapshot := &EVMSnapshot{
		ProjectCode: 100,
		CutoffTime:  5 * testMillisPerDay,
		Tasks: []*data.Task{
			testTask(1, 80, 0, 8, data.Started, data.Doing, 0),
			testTask(2, 20, 0, 1, data.Started, data.Done, 1),
			testTask(3, 50, 20, 25, data.NoStarted, data.Wait, 0),
		},
		WorkTimeByTask: map[int64]int64{
			1: 8,
			2: 6,
		},
	}

	report := CalculateEVM(snapshot)

	assertNear(t, "bac", report.BAC, 150)
	assertNear(t, "pv", report.PV, 70)
	assertNear(t, "ev", report.EV, 28)
	assertNear(t, "ac", report.AC, 14)
	assertNear(t, "sv", report.SV, -42)
	assertNear(t, "cv", report.CV, 14)
	assertNear(t, "spi", report.SPI, 0.4)
	assertNear(t, "cpi", report.CPI, 2)
	assertNear(t, "eac", report.EAC, 75)
	assertNear(t, "etc", report.ETC, 61)
	assertNear(t, "vac", report.VAC, 75)
	assertNear(t, "completion", report.Completion, 18.67)
	assertNear(t, "progress rate", report.ProgressRate, -60)

	if !report.PVValid || !report.ACValid || !report.CPIValid {
		t.Fatalf("validity flags = %v/%v/%v, want all true", report.PVValid, report.ACValid, report.CPIValid)
	}
	if report.TaskCount != 3 {
		t.Fatalf("task count = %d, want 3", report.TaskCount)
	}
	if report.ScheduleHealth != HealthSevere {
		t.Fatalf("schedule health = %q (spi=%v), want %q", report.ScheduleHealth, report.SPI, HealthSevere)
	}
	if report.CostHealth != HealthAhead {
		t.Fatalf("cost health = %q (cpi=%v), want %q", report.CostHealth, report.CPI, HealthAhead)
	}

	joined := strings.Join(report.Insights, " | ")
	for _, want := range []string{"进度严重落后", "成本优于计划", "EAC"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("insights %q does not contain %q", joined, want)
		}
	}
}

func TestCalculateEVMNilSnapshot(t *testing.T) {
	report := CalculateEVM(nil)
	if report == nil {
		t.Fatal("report is nil")
	}
	if report.ScheduleHealth != HealthInsufficient || report.CostHealth != HealthInsufficient {
		t.Fatalf("health = %q/%q, want both %q", report.ScheduleHealth, report.CostHealth, HealthInsufficient)
	}
	if len(report.Insights) == 0 {
		t.Fatal("expected an insight describing missing data")
	}
}

func TestCalculateEVMZeroPlanNoDivideByZero(t *testing.T) {
	report := CalculateEVM(&EVMSnapshot{
		ProjectCode: 1,
		CutoffTime:  5 * testMillisPerDay,
		Tasks: []*data.Task{
			testTask(1, 0, 0, 8, data.Started, data.Doing, 0),
			testTask(2, 0, 0, 8, data.NoStarted, data.Wait, 0),
		},
		WorkTimeByTask: map[int64]int64{1: 3},
	})

	if report.PVValid {
		t.Fatal("pvValid should be false when planned value is zero")
	}
	if report.SPI != 0 {
		t.Fatalf("spi = %v, want 0", report.SPI)
	}
	if report.ScheduleHealth != HealthInsufficient {
		t.Fatalf("schedule health = %q, want %q", report.ScheduleHealth, HealthInsufficient)
	}
	if report.EAC != 0 || report.ETC != 0 || report.VAC != 0 {
		t.Fatalf("eac/etc/vac = %v/%v/%v, want all 0", report.EAC, report.ETC, report.VAC)
	}
	if !strings.Contains(strings.Join(report.Insights, " | "), "PV 为 0") {
		t.Fatalf("insights = %v, want a PV zero hint", report.Insights)
	}
}

func TestCalculateEVMZeroCostNoDivideByZero(t *testing.T) {
	report := CalculateEVM(&EVMSnapshot{
		ProjectCode: 1,
		CutoffTime:  10 * testMillisPerDay,
		Tasks: []*data.Task{
			testTask(1, 40, 0, 4, data.Started, data.Done, 1),
		},
		WorkTimeByTask: map[int64]int64{},
	})

	assertNear(t, "pv", report.PV, 40)
	assertNear(t, "ev", report.EV, 40)
	assertNear(t, "ac", report.AC, 0)
	assertNear(t, "spi", report.SPI, 1)
	if report.ACValid {
		t.Fatal("acValid should be false when actual cost is zero")
	}
	if report.CPIValid {
		t.Fatal("cpiValid should be false when actual cost is zero")
	}
	if report.CPI != 0 {
		t.Fatalf("cpi = %v, want 0", report.CPI)
	}
	if report.EAC != 0 || report.ETC != 0 || report.VAC != 0 {
		t.Fatalf("eac/etc/vac = %v/%v/%v, want all 0", report.EAC, report.ETC, report.VAC)
	}
	if report.CostHealth != HealthInsufficient {
		t.Fatalf("cost health = %q, want %q", report.CostHealth, HealthInsufficient)
	}
	if !strings.Contains(strings.Join(report.Insights, " | "), "AC 为 0") {
		t.Fatalf("insights = %v, want an AC zero hint", report.Insights)
	}
}

func TestCalculateEVMEmptyProject(t *testing.T) {
	report := CalculateEVM(&EVMSnapshot{ProjectCode: 9, CutoffTime: testMillisPerDay})
	if report.TaskCount != 0 {
		t.Fatalf("task count = %d, want 0", report.TaskCount)
	}
	if report.BAC != 0 || report.PV != 0 || report.EV != 0 || report.AC != 0 {
		t.Fatalf("raw quantities = %v/%v/%v/%v, want all 0", report.BAC, report.PV, report.EV, report.AC)
	}
	if report.PVValid || report.ACValid || report.CPIValid {
		t.Fatal("validity flags should all be false for an empty project")
	}
	if report.SPI != 0 || report.CPI != 0 {
		t.Fatalf("spi/cpi = %v/%v, want 0/0", report.SPI, report.CPI)
	}
}

func TestHealthClassificationBoundaries(t *testing.T) {
	const totalSchedule = 200
	cases := []struct {
		name       string
		completion float64
		workTime   int64
		wantSPI    string
		wantCPI    string
	}{
		{name: "both healthy", completion: 1, workTime: 200, wantSPI: HealthHealthy, wantCPI: HealthHealthy},
		{name: "spi lower bound healthy", completion: 0.95, workTime: 190, wantSPI: HealthHealthy, wantCPI: HealthHealthy},
		{name: "spi warning", completion: 0.9, workTime: 180, wantSPI: HealthWarning, wantCPI: HealthHealthy},
		{name: "spi warning lower bound", completion: 0.85, workTime: 170, wantSPI: HealthWarning, wantCPI: HealthHealthy},
		{name: "spi severe", completion: 0.84, workTime: 168, wantSPI: HealthSevere, wantCPI: HealthHealthy},
		{name: "cpi warning", completion: 1, workTime: 222, wantSPI: HealthHealthy, wantCPI: HealthWarning},
		{name: "cpi severe", completion: 1, workTime: 260, wantSPI: HealthHealthy, wantCPI: HealthSevere},
		{name: "cpi ahead", completion: 1, workTime: 180, wantSPI: HealthHealthy, wantCPI: HealthAhead},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := testTask(1, totalSchedule, 0, 10, data.Started, data.Doing, 0)
			if tc.completion >= 1 {
				task.Done = 1
				task.ExecuteStatus = data.Done
			} else {
				task.WorkTime = int(math.Round(tc.completion * totalSchedule))
			}

			snapshot := &EVMSnapshot{
				ProjectCode:    1,
				CutoffTime:     10 * testMillisPerDay,
				Tasks:          []*data.Task{task},
				WorkTimeByTask: map[int64]int64{1: tc.workTime},
			}

			report := CalculateEVM(snapshot)
			assertNear(t, tc.name+" ev", report.EV, totalSchedule*tc.completion)

			if report.ScheduleHealth != tc.wantSPI {
				t.Fatalf("schedule health = %q (spi=%v), want %q", report.ScheduleHealth, report.SPI, tc.wantSPI)
			}
			if report.CostHealth != tc.wantCPI {
				t.Fatalf("cost health = %q (cpi=%v), want %q", report.CostHealth, report.CPI, tc.wantCPI)
			}
		})
	}
}

func TestRateHealthDirect(t *testing.T) {
	cases := []struct {
		rate  float64
		valid bool
		want  string
	}{
		{rate: 0, valid: false, want: HealthInsufficient},
		{rate: 0.84, valid: true, want: HealthSevere},
		{rate: 0.85, valid: true, want: HealthWarning},
		{rate: 0.949, valid: true, want: HealthWarning},
		{rate: 0.95, valid: true, want: HealthHealthy},
		{rate: 1.05, valid: true, want: HealthHealthy},
		{rate: 1.051, valid: true, want: HealthAhead},
	}

	for _, tc := range cases {
		if got := rateHealth(tc.rate, tc.valid); got != tc.want {
			t.Fatalf("rateHealth(%v, %v) = %q, want %q", tc.rate, tc.valid, got, tc.want)
		}
	}
}

func TestEVMDomainReport(t *testing.T) {
	loader := &fakeRecordLoader{
		tasks: []*data.Task{
			testTask(1, 10, 0, 10, data.Started, data.Doing, 0),
		},
		workTime: map[int64]int64{1: 4},
	}
	domain := &EVMDomain{records: loader, projectRepo: &stubProjectRepo{project: testProject(77)}}

	report, err := domain.Report(context.Background(), 77, testMillisPerDay, 5*testMillisPerDay)
	if err != nil {
		t.Fatalf("Report returned error: %v", err)
	}
	assertNear(t, "pv", report.PV, 5)
	assertNear(t, "ev", report.EV, 4)
	assertNear(t, "ac", report.AC, 4)
	if report.ProjectCode != 77 {
		t.Fatalf("project code = %d, want 77", report.ProjectCode)
	}
	if loader.lastEndTime != 5*testMillisPerDay {
		t.Fatalf("cutoff passed to loader = %d, want %d", loader.lastEndTime, 5*testMillisPerDay)
	}
}

func TestEVMDomainValidation(t *testing.T) {
	domain := &EVMDomain{records: &fakeRecordLoader{}, projectRepo: &stubProjectRepo{project: testProject(1)}}
	ctx := context.Background()

	if _, err := domain.Report(ctx, 0, testMillisPerDay, 2*testMillisPerDay); err == nil {
		t.Fatal("expected error for empty project code")
	}
	if _, err := domain.Report(ctx, 1, 0, testMillisPerDay); err == nil {
		t.Fatal("expected error for zero begin time")
	}
	if _, err := domain.Report(ctx, 1, 2*testMillisPerDay, testMillisPerDay); err == nil {
		t.Fatal("expected error for begin time after end time")
	}
	if _, err := domain.Report(ctx, 1, testMillisPerDay, 0); err == nil {
		t.Fatal("expected error for zero end time")
	}
}

func TestEVMDomainProjectNotFound(t *testing.T) {
	domain := &EVMDomain{records: &fakeRecordLoader{}, projectRepo: &stubProjectRepo{project: nil}}
	_, err := domain.Report(context.Background(), 5, testMillisPerDay, 2*testMillisPerDay)
	if !errors.Is(err, ErrProjectNotFound) {
		t.Fatalf("error = %v, want %v", err, ErrProjectNotFound)
	}
}

func TestEVMDomainPropagatesLoaderError(t *testing.T) {
	sentinel := errors.New("db down")
	domain := &EVMDomain{records: &fakeRecordLoader{taskErr: sentinel}, projectRepo: &stubProjectRepo{project: testProject(5)}}
	_, err := domain.Report(context.Background(), 5, testMillisPerDay, 2*testMillisPerDay)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want %v", err, sentinel)
	}
}

func TestRound2GuardsNonFiniteValues(t *testing.T) {
	if got := round2(math.NaN()); got != 0 {
		t.Fatalf("round2(NaN) = %v, want 0", got)
	}
	if got := round2(math.Inf(1)); got != 0 {
		t.Fatalf("round2(+Inf) = %v, want 0", got)
	}
	assertNear(t, "round2", round2(1.005), 1.0)
	assertNear(t, "round2 up", round2(1.006), 1.01)
}
