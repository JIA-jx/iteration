package domain

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
	"test.com/toProject/internal/repo"
)

const (
	LagSemanticsFS = "前驱完成时间 + Lag 不晚于后继开始时间"
	LagSemanticsSS = "前驱开始时间 + Lag 不晚于后继开始时间"

	DefaultDependencyType = data.DependencyTypeFS
)

var (
	ErrProjectCodeRequired = errors.New("projectCode 不能为空")
	ErrProjectNotFound     = errors.New("项目不存在")
	ErrNoTasksInProject    = errors.New("项目下没有任务，无法构建依赖图")
	ErrDependencySelfRef   = errors.New("任务不能依赖自身")
	ErrGraphEmpty          = errors.New("依赖图为空")
)

type UnsupportedDependencyError struct {
	Type string
}

func (e *UnsupportedDependencyError) Error() string {
	return fmt.Sprintf("暂不支持的依赖类型 %s，当前仅支持 %s 与 %s", e.Type, data.DependencyTypeFS, data.DependencyTypeSS)
}

type DependencyCycleError struct {
	Cycle []int64
}

func (e *DependencyCycleError) Error() string {
	return "任务依赖存在环：" + FormatCycle(e.Cycle)
}

func (e *DependencyCycleError) Path() string {
	return FormatCycle(e.Cycle)
}

type CycleError = DependencyCycleError

func FormatCycle(cycle []int64) string {
	if len(cycle) == 0 {
		return ""
	}
	parts := make([]string, 0, len(cycle))
	for _, code := range cycle {
		parts = append(parts, fmt.Sprintf("%d", code))
	}
	return strings.Join(parts, " -> ")
}

func DependencyTypeName(depType string) string {
	switch depType {
	case data.DependencyTypeSS:
		return "开始-开始"
	case data.DependencyTypeFF:
		return "完成-完成"
	case data.DependencyTypeSF:
		return "开始-完成"
	default:
		return "完成-开始"
	}
}

func NormalizeDependencyType(depType string) (string, error) {
	if strings.TrimSpace(depType) == "" {
		return DefaultDependencyType, nil
	}
	upper := strings.ToUpper(strings.TrimSpace(depType))
	switch upper {
	case data.DependencyTypeFS, data.DependencyTypeSS:
		return upper, nil
	case data.DependencyTypeFF, data.DependencyTypeSF:
		return "", &UnsupportedDependencyError{Type: upper}
	default:
		return "", fmt.Errorf("未知的依赖类型 %s，仅支持 FS 与 SS", depType)
	}
}

type TaskNode struct {
	TaskCode      int64   `json:"taskCode"`
	Name          string  `json:"name"`
	BeginTime     int64   `json:"beginTime"`
	EndTime       int64   `json:"endTime"`
	Schedule      int     `json:"schedule"`
	Status        int     `json:"status"`
	ExecuteStatus int     `json:"executeStatus"`
	Sort          int     `json:"sort"`
	Duration      int64   `json:"duration"`
	Predecessors  []int64 `json:"predecessors"`
	Successors    []int64 `json:"successors"`
}

type Dependency struct {
	Id          int64  `json:"id"`
	ProjectCode int64  `json:"projectCode"`
	TaskCode    int64  `json:"taskCode"`
	PreTaskCode int64  `json:"preTaskCode"`
	Type        string `json:"type"`
	Lag         int64  `json:"lag"`
}

func (d Dependency) TypeName() string {
	return DependencyTypeName(d.Type)
}

type Graph struct {
	ProjectCode int64
	Nodes       map[int64]*TaskNode
	Order       []int64
	OutEdges    map[int64][]Dependency
	InEdges     map[int64][]Dependency
}

func BuildDAG(projectCode int64, tasks []*data.Task, dependencies []*data.TaskDependency) (*Graph, error) {
	graph := &Graph{
		ProjectCode: projectCode,
		Nodes:       make(map[int64]*TaskNode, len(tasks)),
		Order:       make([]int64, 0, len(tasks)),
		OutEdges:    make(map[int64][]Dependency, len(tasks)),
		InEdges:     make(map[int64][]Dependency, len(tasks)),
	}

	for _, task := range tasks {
		if task == nil {
			continue
		}
		if _, exists := graph.Nodes[task.Id]; exists {
			continue
		}
		duration := task.EndTime - task.BeginTime
		if duration < 0 {
			duration = 0
		}
		graph.Nodes[task.Id] = &TaskNode{
			TaskCode:      task.Id,
			Name:          task.Name,
			BeginTime:     task.BeginTime,
			EndTime:       task.EndTime,
			Schedule:      task.Schedule,
			Status:        task.Status,
			ExecuteStatus: task.ExecuteStatus,
			Sort:          task.Sort,
			Duration:      duration,
			Predecessors:  make([]int64, 0),
			Successors:    make([]int64, 0),
		}
		graph.Order = append(graph.Order, task.Id)
	}

	if len(graph.Nodes) == 0 {
		return graph, nil
	}

	for _, dependency := range dependencies {
		if dependency == nil {
			continue
		}
		taskCode := dependency.TaskCode
		preTaskCode := dependency.PreTaskCode
		if taskCode == preTaskCode {
			return nil, fmt.Errorf("%w：%d", ErrDependencySelfRef, taskCode)
		}
		taskNode, taskExists := graph.Nodes[taskCode]
		preNode, preExists := graph.Nodes[preTaskCode]
		if !taskExists || !preExists {
			continue
		}
		depType, err := NormalizeDependencyType(dependency.Type)
		if err != nil {
			return nil, err
		}
		edge := Dependency{
			Id:          dependency.Id,
			ProjectCode: dependency.ProjectCode,
			TaskCode:    taskCode,
			PreTaskCode: preTaskCode,
			Type:        depType,
			Lag:         dependency.Lag,
		}
		graph.OutEdges[preTaskCode] = append(graph.OutEdges[preTaskCode], edge)
		graph.InEdges[taskCode] = append(graph.InEdges[taskCode], edge)
		taskNode.Predecessors = append(taskNode.Predecessors, preTaskCode)
		preNode.Successors = append(preNode.Successors, taskCode)
	}

	return graph, nil
}

func (g *Graph) TaskCodes() []int64 {
	if g == nil {
		return nil
	}
	codes := make([]int64, 0, len(g.Order))
	codes = append(codes, g.Order...)
	return codes
}

func (g *Graph) NodeCount() int {
	if g == nil {
		return 0
	}
	return len(g.Nodes)
}

func (g *Graph) EdgeCount() int {
	if g == nil {
		return 0
	}
	count := 0
	for _, edges := range g.OutEdges {
		count += len(edges)
	}
	return count
}

func (g *Graph) TopologicalSort() ([]int64, error) {
	if g == nil {
		return nil, ErrGraphEmpty
	}
	if len(g.Nodes) == 0 {
		return []int64{}, nil
	}

	indegree := make(map[int64]int, len(g.Nodes))
	for code := range g.Nodes {
		indegree[code] = 0
	}
	for _, edges := range g.OutEdges {
		for _, edge := range edges {
			if edge.TaskCode == edge.PreTaskCode {
				continue
			}
			indegree[edge.TaskCode]++
		}
	}

	queue := make([]int64, 0, len(g.Nodes))
	for _, code := range g.Order {
		if indegree[code] == 0 {
			queue = append(queue, code)
		}
	}

	sorted := make([]int64, 0, len(g.Nodes))
	for len(queue) > 0 {
		code := queue[0]
		queue = queue[1:]
		sorted = append(sorted, code)

		for _, edge := range g.OutEdges[code] {
			if edge.TaskCode == edge.PreTaskCode {
				continue
			}
			indegree[edge.TaskCode]--
			if indegree[edge.TaskCode] == 0 {
				queue = append(queue, edge.TaskCode)
			}
		}
	}

	if len(sorted) != len(g.Nodes) {
		return nil, &DependencyCycleError{Cycle: g.findCycle(indegree)}
	}
	return sorted, nil
}

func (g *Graph) findCycle(indegree map[int64]int) []int64 {
	residual := make(map[int64][]int64, len(g.Nodes))
	for _, edges := range g.OutEdges {
		for _, edge := range edges {
			if edge.TaskCode == edge.PreTaskCode {
				continue
			}
			addEdge := func(from, to int64) {
				if indegree[from] <= 0 || indegree[to] <= 0 {
					return
				}
				residual[from] = append(residual[from], to)
			}
			addEdge(edge.PreTaskCode, edge.TaskCode)
			addEdge(edge.TaskCode, edge.PreTaskCode)
		}
	}

	var cycle []int64
	state := make(map[int64]int, len(residual))
	path := make([]int64, 0, len(residual))
	index := make(map[int64]int, len(residual))

	var visit func(code int64) bool
	visit = func(code int64) bool {
		state[code] = 1
		index[code] = len(path)
		path = append(path, code)

		for _, next := range residual[code] {
			if state[next] == 1 {
				start := index[next]
				cycle = make([]int64, 0, len(path)-start+1)
				cycle = append(cycle, path[start:]...)
				cycle = append(cycle, next)
				return true
			}
			if state[next] == 0 && visit(next) {
				return true
			}
		}

		state[code] = 2
		path = path[:len(path)-1]
		return false
	}

	for _, code := range g.Order {
		if indegree[code] <= 0 || state[code] != 0 {
			continue
		}
		if visit(code) {
			return cycle
		}
	}

	fallback := make([]int64, 0, len(residual)+1)
	for _, code := range g.Order {
		if indegree[code] > 0 {
			fallback = append(fallback, code)
		}
	}
	if len(fallback) > 0 {
		fallback = append(fallback, fallback[0])
	}
	return fallback
}

func (g *Graph) ForwardPass(order []int64) map[int64]*ScheduleInfo {
	info := make(map[int64]*ScheduleInfo, len(g.Nodes))
	for code, node := range g.Nodes {
		info[code] = &ScheduleInfo{
			TaskCode:      code,
			Name:          node.Name,
			Duration:      node.Duration,
			OriginalStart: node.BeginTime,
			OriginalEnd:   node.EndTime,
		}
	}

	for _, code := range order {
		record := info[code]
		if record == nil {
			continue
		}
		var earliest int64
		for _, edge := range g.InEdges[code] {
			pre := info[edge.PreTaskCode]
			if pre == nil {
				continue
			}
			candidate := pre.EarliestFinish + edge.Lag
			if edge.Type == data.DependencyTypeSS {
				candidate = pre.EarliestStart + edge.Lag
			}
			if candidate > earliest {
				earliest = candidate
			}
		}
		record.EarliestStart = earliest
		record.EarliestFinish = earliest + record.Duration
	}
	return info
}

func (g *Graph) BackwardPass(order []int64, info map[int64]*ScheduleInfo) int64 {
	var projectDuration int64
	for _, record := range info {
		if record.EarliestFinish > projectDuration {
			projectDuration = record.EarliestFinish
		}
	}

	for _, record := range info {
		record.LatestFinish = projectDuration
	}

	for i := len(order) - 1; i >= 0; i-- {
		code := order[i]
		record := info[code]
		if record == nil {
			continue
		}

		for _, edge := range g.OutEdges[code] {
			successor := info[edge.TaskCode]
			if successor == nil {
				continue
			}
			var candidate int64
			if edge.Type == data.DependencyTypeSS {
				candidate = successor.LatestStart - edge.Lag + record.Duration
			} else {
				candidate = successor.LatestStart - edge.Lag
			}
			if candidate < record.LatestFinish {
				record.LatestFinish = candidate
			}
		}

		record.LatestStart = record.LatestFinish - record.Duration
		record.Float = record.LatestFinish - record.EarliestFinish
		record.Critical = record.Float == 0
	}

	return projectDuration
}

func (g *Graph) ComputeSchedule() ([]int64, map[int64]*ScheduleInfo, int64, error) {
	order, err := g.TopologicalSort()
	if err != nil {
		return nil, nil, 0, err
	}
	info := g.ForwardPass(order)
	projectDuration := g.BackwardPass(order, info)
	return order, info, projectDuration, nil
}

func (g *Graph) findCriticalPath(order []int64, info map[int64]*ScheduleInfo) []*ScheduleInfo {
	if len(order) == 0 {
		return []*ScheduleInfo{}
	}

	var start int64
	found := false
	for _, code := range order {
		record := info[code]
		if record == nil || !record.Critical {
			continue
		}
		if !found {
			start = code
			found = true
			continue
		}
		if record.EarliestStart < info[start].EarliestStart {
			start = code
		}
	}
	if !found {
		return []*ScheduleInfo{}
	}

	path := make([]*ScheduleInfo, 0, len(order))
	current := start
	guard := 0
	for guard <= len(order) {
		guard++
		record := info[current]
		if record == nil {
			break
		}
		path = append(path, record)

		var next int64
		foundNext := false
		for _, edge := range g.OutEdges[current] {
			successor := info[edge.TaskCode]
			if successor == nil || !successor.Critical {
				continue
			}
			next = edge.TaskCode
			foundNext = true
			break
		}
		if !foundNext {
			break
		}
		current = next
	}
	return path
}

func (g *Graph) CriticalTaskCodes(info map[int64]*ScheduleInfo) []int64 {
	codes := make([]int64, 0)
	for _, code := range g.Order {
		record := info[code]
		if record == nil {
			continue
		}
		if record.Critical {
			codes = append(codes, code)
		}
	}
	return codes
}

func (g *Graph) CriticalPath() (*CPMResult, error) {
	order, info, projectDuration, err := g.ComputeSchedule()
	if err != nil {
		return nil, err
	}

	schedules := make([]*ScheduleInfo, 0, len(g.Order))
	for _, code := range g.Order {
		record := info[code]
		if record == nil {
			continue
		}
		schedules = append(schedules, record)
	}

	criticalPath := g.findCriticalPath(order, info)
	criticalCodes := make([]int64, 0, len(criticalPath))
	for _, record := range criticalPath {
		criticalCodes = append(criticalCodes, record.TaskCode)
	}

	return &CPMResult{
		ProjectCode:     g.ProjectCode,
		ProjectDuration: projectDuration,
		CriticalPath:    criticalCodes,
		CriticalTasks:   g.CriticalTaskCodes(info),
		Schedules:       schedules,
		TaskCount:       len(g.Nodes),
		DependencyCount: g.EdgeCount(),
	}, nil
}

func (g *Graph) AutoSchedule() ([]ScheduleChange, error) {
	order, err := g.TopologicalSort()
	if err != nil {
		return nil, err
	}

	state := make(map[int64]*autoScheduleState, len(g.Nodes))
	for code, node := range g.Nodes {
		duration := node.Duration
		if duration < 0 {
			duration = 0
		}
		state[code] = &autoScheduleState{
			start:    node.BeginTime,
			end:      node.BeginTime + duration,
			original: node.BeginTime,
			duration: duration,
		}
	}

	changes := make([]ScheduleChange, 0)
	for _, code := range order {
		record := state[code]
		if record == nil {
			continue
		}
		current := record.start
		for _, edge := range g.InEdges[code] {
			pre := state[edge.PreTaskCode]
			if pre == nil {
				continue
			}
			candidate := pre.end + edge.Lag
			if edge.Type == data.DependencyTypeSS {
				candidate = pre.start + edge.Lag
			}
			if candidate > current {
				current = candidate
			}
		}

		if current == record.start {
			continue
		}

		shift := current - record.start
		record.start = current
		record.end = current + record.duration
		changes = append(changes, ScheduleChange{
			TaskCode:      code,
			Name:          g.Nodes[code].Name,
			OriginalStart: record.original,
			OriginalEnd:   record.original + record.duration,
			NewStart:      record.start,
			NewEnd:        record.end,
			ShiftMillis:   shift,
		})
	}
	return changes, nil
}

type autoScheduleState struct {
	start    int64
	end      int64
	original int64
	duration int64
}

type ScheduleInfo struct {
	TaskCode       int64  `json:"taskCode"`
	Name           string `json:"name"`
	Duration       int64  `json:"duration"`
	OriginalStart  int64  `json:"originalStart"`
	OriginalEnd    int64  `json:"originalEnd"`
	EarliestStart  int64  `json:"earliestStart"`
	EarliestFinish int64  `json:"earliestFinish"`
	LatestStart    int64  `json:"latestStart"`
	LatestFinish   int64  `json:"latestFinish"`
	Float          int64  `json:"float"`
	Critical       bool   `json:"critical"`
}

type CPMResult struct {
	ProjectCode     int64           `json:"projectCode"`
	ProjectDuration int64           `json:"projectDuration"`
	CriticalPath    []int64         `json:"criticalPath"`
	CriticalTasks   []int64         `json:"criticalTasks"`
	Schedules       []*ScheduleInfo `json:"schedules"`
	TaskCount       int             `json:"taskCount"`
	DependencyCount int             `json:"dependencyCount"`
}

func (r *CPMResult) ScheduleOf(taskCode int64) *ScheduleInfo {
	if r == nil {
		return nil
	}
	for _, record := range r.Schedules {
		if record != nil && record.TaskCode == taskCode {
			return record
		}
	}
	return nil
}

type ScheduleChange struct {
	TaskCode      int64  `json:"taskCode"`
	Name          string `json:"name"`
	OriginalStart int64  `json:"originalStart"`
	OriginalEnd   int64  `json:"originalEnd"`
	NewStart      int64  `json:"newStart"`
	NewEnd        int64  `json:"newEnd"`
	ShiftMillis   int64  `json:"shiftMillis"`
}

type ProjectDataSource interface {
	FindProjectById(ctx context.Context, projectCode int64) (*data.Project, error)
	ListTaskByProject(ctx context.Context, projectCode int64) ([]*data.Task, error)
	ListWorkTimeByTaskIds(ctx context.Context, taskIds []int64) ([]*data.TaskWorkTime, error)
	ListByProject(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error)
}

type SchedulePersister interface {
	UpdateTaskSchedule(ctx context.Context, taskCode int64, beginTime, endTime int64) error
}

type CPMDomain struct {
	records     RecordLoader
	depRepo     repo.TaskDependencyRepo
	projectRepo projectFinder
	persister   SchedulePersister
	dataSource  ProjectDataSource
	now         func() time.Time
}

func NewCPMDomain() *CPMDomain {
	return &CPMDomain{
		records: NewProjectRecordSource(
			dao.NewTaskDao(),
			dao.NewTaskWorkTimeDao(),
			dao.NewTaskDependencyDao(),
		),
		depRepo:     dao.NewTaskDependencyDao(),
		projectRepo: dao.NewProjectDao(),
		persister:   dao.NewTaskDao(),
		now:         time.Now,
	}
}

func parseProjectCode(projectCode string) (int64, error) {
	trimmed := strings.TrimSpace(projectCode)
	if trimmed == "" {
		return 0, ErrProjectCodeRequired
	}
	var code int64
	if _, err := fmt.Sscanf(trimmed, "%d", &code); err != nil {
		return 0, fmt.Errorf("projectCode 无效：%s", projectCode)
	}
	if code <= 0 {
		return 0, ErrProjectCodeRequired
	}
	return code, nil
}

func (d *CPMDomain) ValidateProject(ctx context.Context, projectCode int64) error {
	if projectCode <= 0 {
		return ErrProjectCodeRequired
	}
	if d.dataSource != nil {
		project, err := d.dataSource.FindProjectById(ctx, projectCode)
		if err != nil {
			return err
		}
		if project == nil {
			return ErrProjectNotFound
		}
		return nil
	}
	if d.projectRepo == nil {
		return nil
	}
	project, err := d.projectRepo.FindProjectById(ctx, projectCode)
	if err != nil {
		return err
	}
	if project == nil {
		return ErrProjectNotFound
	}
	return nil
}

func (d *CPMDomain) BuildGraph(ctx context.Context, projectCode string) (*Graph, error) {
	code, err := parseProjectCode(projectCode)
	if err != nil {
		return nil, err
	}
	return d.BuildGraphByCode(ctx, code)
}

func (d *CPMDomain) BuildGraphByCode(ctx context.Context, projectCode int64) (*Graph, error) {
	if projectCode <= 0 {
		return nil, ErrProjectCodeRequired
	}
	tasks, err := d.listTasks(ctx, projectCode)
	if err != nil {
		return nil, err
	}
	dependencies, err := d.listDependencies(ctx, projectCode)
	if err != nil {
		return nil, err
	}
	return BuildDAG(projectCode, tasks, dependencies)
}

func (d *CPMDomain) listTasks(ctx context.Context, projectCode int64) ([]*data.Task, error) {
	if d.dataSource != nil {
		return d.dataSource.ListTaskByProject(ctx, projectCode)
	}
	if d.records == nil {
		return nil, errors.New("数据源未装配")
	}
	return d.records.LoadProjectTasks(ctx, projectCode)
}

func (d *CPMDomain) listDependencies(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error) {
	if d.dataSource != nil {
		return d.dataSource.ListByProject(ctx, projectCode)
	}
	if d.records == nil {
		return nil, errors.New("数据源未装配")
	}
	return d.records.LoadProjectDependencies(ctx, projectCode)
}

func (d *CPMDomain) CriticalPath(ctx context.Context, projectCode string) (*CPMResult, error) {
	graph, err := d.BuildGraph(ctx, projectCode)
	if err != nil {
		return nil, err
	}
	return graph.CriticalPath()
}

func (d *CPMDomain) AutoSchedule(ctx context.Context, projectCode string) ([]ScheduleChange, error) {
	graph, err := d.BuildGraph(ctx, projectCode)
	if err != nil {
		return nil, err
	}
	return d.ApplySchedule(ctx, graph)
}

func (d *CPMDomain) ApplySchedule(ctx context.Context, graph *Graph) ([]ScheduleChange, error) {
	if graph == nil {
		return nil, ErrGraphEmpty
	}
	changes, err := graph.AutoSchedule()
	if err != nil {
		return nil, err
	}
	if len(changes) == 0 || d.persister == nil {
		return changes, nil
	}

	applied := make([]ScheduleChange, 0, len(changes))
	for _, change := range changes {
		if err := d.persister.UpdateTaskSchedule(ctx, change.TaskCode, change.NewStart, change.NewEnd); err != nil {
			for i := len(applied) - 1; i >= 0; i-- {
				prev := applied[i]
				_ = d.persister.UpdateTaskSchedule(ctx, prev.TaskCode, prev.OriginalStart, prev.OriginalEnd)
			}
			return nil, err
		}
		applied = append(applied, change)
	}
	return changes, nil
}

func (d *CPMDomain) UpdateTaskSchedule(ctx context.Context, taskCode int64, beginTime, endTime int64) error {
	if d.persister == nil {
		return errors.New("排期写入器未装配")
	}
	return d.persister.UpdateTaskSchedule(ctx, taskCode, beginTime, endTime)
}

func (d *CPMDomain) SaveDependency(ctx context.Context, projectCode, taskCode, preTaskCode int64, depType string, lag int64) (*data.TaskDependency, error) {
	if projectCode <= 0 {
		return nil, ErrProjectCodeRequired
	}
	if taskCode <= 0 || preTaskCode <= 0 {
		return nil, errors.New("taskCode 与 preTaskCode 不能为空")
	}
	if taskCode == preTaskCode {
		return nil, ErrDependencySelfRef
	}
	normalized, err := NormalizeDependencyType(depType)
	if err != nil {
		return nil, err
	}

	dependency := &data.TaskDependency{
		ProjectCode: projectCode,
		TaskCode:    taskCode,
		PreTaskCode: preTaskCode,
		Type:        normalized,
		Lag:         lag,
		CreatedAt:   d.nowTime(),
	}
	if d.depRepo == nil {
		return dependency, nil
	}

	existed := false
	var previousLag int64
	if deps, listErr := d.listDependencies(ctx, projectCode); listErr == nil {
		for _, dep := range deps {
			if dep.TaskCode == taskCode && dep.PreTaskCode == preTaskCode && dep.Type == normalized {
				existed = true
				previousLag = dep.Lag
				break
			}
		}
	}

	if err := d.depRepo.Save(ctx, dependency); err != nil {
		return nil, err
	}

	readCtx := gorms.WithForceMaster(ctx)
	graph, err := d.BuildGraphByCode(readCtx, projectCode)
	if err != nil {
		d.rollbackDependency(ctx, dependency, existed, previousLag)
		return nil, err
	}
	if _, err := graph.TopologicalSort(); err != nil {
		d.rollbackDependency(ctx, dependency, existed, previousLag)
		return nil, err
	}

	return dependency, nil
}

func (d *CPMDomain) rollbackDependency(ctx context.Context, dependency *data.TaskDependency, existed bool, previousLag int64) {
	if d.depRepo == nil || dependency == nil {
		return
	}

	if existed {
		if err := d.depRepo.Save(ctx, &data.TaskDependency{
			ProjectCode: dependency.ProjectCode,
			TaskCode:    dependency.TaskCode,
			PreTaskCode: dependency.PreTaskCode,
			Type:        dependency.Type,
			Lag:         previousLag,
			CreatedAt:   d.nowTime(),
		}); err != nil {
			zap.L().Error("rollback dependency save failed",
				zap.Int64("project", dependency.ProjectCode),
				zap.Int64("task", dependency.TaskCode),
				zap.Error(err))
		}
		return
	}

	if dependency.Id > 0 {
		if _, err := d.depRepo.Delete(ctx, dependency.Id); err != nil {
			zap.L().Error("rollback dependency delete failed",
				zap.Int64("id", dependency.Id), zap.Error(err))
		}
		return
	}
	if _, err := d.depRepo.DeleteByTaskPair(ctx, dependency.ProjectCode, dependency.TaskCode, dependency.PreTaskCode); err != nil {
		zap.L().Error("rollback dependency delete by pair failed",
			zap.Int64("project", dependency.ProjectCode),
			zap.Int64("task", dependency.TaskCode), zap.Error(err))
	}
}

func (d *CPMDomain) DeleteDependency(ctx context.Context, dependencyId, projectCode, taskCode, preTaskCode int64) (int64, error) {
	if d.depRepo == nil {
		return 0, nil
	}
	if dependencyId > 0 {
		return d.depRepo.Delete(ctx, dependencyId)
	}
	if taskCode <= 0 || preTaskCode <= 0 {
		return 0, errors.New("taskDependencyId 或 projectCode+taskCode+preTaskCode 至少要提供一组")
	}
	return d.depRepo.DeleteByTaskPair(ctx, projectCode, taskCode, preTaskCode)
}

func (d *CPMDomain) ListDependency(ctx context.Context, projectCode int64) ([]*data.TaskDependency, error) {
	if projectCode <= 0 {
		return nil, ErrProjectCodeRequired
	}
	if d.depRepo == nil {
		return nil, nil
	}
	return d.depRepo.ListByProject(ctx, projectCode)
}

func (d *CPMDomain) nowTime() time.Time {
	if d.now == nil {
		return time.Now()
	}
	return d.now()
}
