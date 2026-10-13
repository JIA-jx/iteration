package domain

import (
	"context"
	"errors"
	"fmt"
	"math"

	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
)

const (
	HealthHealthy      = "健康"
	HealthAhead        = "优于计划"
	HealthWarning      = "预警"
	HealthSevere       = "严重"
	HealthInsufficient = "数据不足"
	HealthRateHealthy  = 0.95
	HealthRateWarning  = 0.85
	HealthRateAhead    = 1.05
)

type EVMSnapshot struct {
	ProjectCode    int64
	CutoffTime     int64
	Tasks          []*data.Task
	WorkTimeByTask map[int64]int64
}

type EVMReport struct {
	ProjectCode    int64    `json:"projectCode"`
	BeginTime      int64    `json:"beginTime"`
	EndTime        int64    `json:"endTime"`
	CutoffTime     int64    `json:"cutoffTime"`
	TaskCount      int      `json:"taskCount"`
	BAC            float64  `json:"bac"`
	PV             float64  `json:"pv"`
	EV             float64  `json:"ev"`
	AC             float64  `json:"ac"`
	SV             float64  `json:"sv"`
	CV             float64  `json:"cv"`
	SPI            float64  `json:"spi"`
	CPI            float64  `json:"cpi"`
	EAC            float64  `json:"eac"`
	ETC            float64  `json:"etc"`
	VAC            float64  `json:"vac"`
	Completion     float64  `json:"completion"`
	ProgressRate   float64  `json:"progressRate"`
	ScheduleHealth string   `json:"scheduleHealth"`
	CostHealth     string   `json:"costHealth"`
	PVValid        bool     `json:"pvValid"`
	ACValid        bool     `json:"acValid"`
	CPIValid       bool     `json:"cpiValid"`
	Insights       []string `json:"insights"`
}

func IsTaskDone(t *data.Task) bool {
	if t == nil {
		return false
	}
	if t.Done == 1 {
		return true
	}
	if t.ExecuteStatus == data.Done || t.ExecuteStatus == data.Closed {
		return true
	}
	return false
}

func IsTaskStarted(t *data.Task) bool {
	if t == nil {
		return false
	}
	if IsTaskDone(t) {
		return true
	}
	if t.Status == data.Started {
		return true
	}
	if t.ExecuteStatus == data.Doing || t.ExecuteStatus == data.Pause {
		return true
	}
	if t.WorkTime > 0 {
		return true
	}
	return false
}

func TaskCompletion(t *data.Task, workTime int64) float64 {
	if t == nil {
		return 0
	}
	if IsTaskDone(t) {
		return 1
	}
	if !IsTaskStarted(t) {
		return 0
	}
	if t.Schedule > 0 {
		ratio := float64(workTime) / float64(t.Schedule)
		if ratio < 0 {
			return 0
		}
		if ratio > 1 {
			return 1
		}
		return ratio
	}
	if workTime > 0 {
		return 1
	}
	return 0
}

func PlannedValueOfTask(t *data.Task, cutoffTime int64) float64 {
	if t == nil || t.Schedule <= 0 {
		return 0
	}
	if t.EndTime <= t.BeginTime {
		if cutoffTime >= t.BeginTime {
			return float64(t.Schedule)
		}
		return 0
	}
	if cutoffTime <= t.BeginTime {
		return 0
	}
	if cutoffTime >= t.EndTime {
		return float64(t.Schedule)
	}
	span := float64(t.EndTime - t.BeginTime)
	elapsed := float64(cutoffTime - t.BeginTime)
	return float64(t.Schedule) * elapsed / span
}

func CalculateEVM(snapshot *EVMSnapshot) *EVMReport {
	report := &EVMReport{
		Insights: make([]string, 0),
	}
	if snapshot == nil {
		report.ScheduleHealth = HealthInsufficient
		report.CostHealth = HealthInsufficient
		report.Insights = append(report.Insights, "缺少挣值输入数据，无法完成计算")
		return report
	}

	report.ProjectCode = snapshot.ProjectCode
	report.CutoffTime = snapshot.CutoffTime
	report.TaskCount = len(snapshot.Tasks)

	var bac, pv, ev, ac float64
	for _, task := range snapshot.Tasks {
		if task == nil {
			continue
		}
		workTime := snapshot.WorkTimeByTask[task.Id]
		bac += float64(task.Schedule)
		pv += PlannedValueOfTask(task, snapshot.CutoffTime)
		ev += float64(task.Schedule) * TaskCompletion(task, workTime)
		ac += float64(workTime)
	}

	report.BAC = round2(bac)
	report.PV = round2(pv)
	report.EV = round2(ev)
	report.AC = round2(ac)
	report.SV = round2(report.EV - report.PV)
	report.CV = round2(report.EV - report.AC)

	report.PVValid = report.PV > 0
	report.ACValid = report.AC > 0
	if report.PVValid {
		report.SPI = round2(report.EV / report.PV)
	}
	if report.ACValid {
		report.CPI = round2(report.EV / report.AC)
	}
	report.CPIValid = report.ACValid && report.CPI > 0

	if report.CPIValid {
		report.EAC = round2(report.BAC / report.CPI)
		report.ETC = round2(report.EAC - report.AC)
		report.VAC = round2(report.BAC - report.EAC)
	}

	if report.BAC > 0 {
		report.Completion = round2(report.EV / report.BAC * 100)
	}
	if report.SV != 0 && report.PV > 0 {
		report.ProgressRate = round2(report.SV / report.PV * 100)
	}

	report.ScheduleHealth = rateHealth(report.SPI, report.PVValid)
	report.CostHealth = rateHealth(report.CPI, report.ACValid)
	report.Insights = buildInsights(report)
	return report
}

func rateHealth(rate float64, valid bool) string {
	if !valid {
		return HealthInsufficient
	}
	if rate >= HealthRateHealthy && rate <= HealthRateAhead {
		return HealthHealthy
	}
	if rate > HealthRateAhead {
		return HealthAhead
	}
	if rate >= HealthRateWarning {
		return HealthWarning
	}
	return HealthSevere
}

func buildInsights(report *EVMReport) []string {
	insights := make([]string, 0, 6)

	if !report.PVValid {
		insights = append(insights, "统计区间内计划价值 PV 为 0，SPI 不可用，请检查任务计划工时与计划开始/结束时间是否已维护")
	}
	if !report.ACValid {
		insights = append(insights, "统计区间内实际成本 AC 为 0，CPI 与完工估算 EAC 不可用，请检查是否已填报实际工时")
	}

	if report.PVValid {
		switch {
		case report.SPI < HealthRateWarning:
			insights = append(insights, fmt.Sprintf("进度严重落后（SPI=%.2f），实际完成量远低于计划，预计完工时间需要显著延后", report.SPI))
			insights = append(insights, "建议优先核对关键路径上的任务并补充资源，或调整项目里程碑基线")
		case report.SPI < HealthRateHealthy:
			insights = append(insights, fmt.Sprintf("进度落后（SPI=%.2f），实际完成量低于计划，预计完工时间需要延后", report.SPI))
		case report.SPI > HealthRateAhead:
			insights = append(insights, fmt.Sprintf("进度优于计划（SPI=%.2f），实际完成量领先计划，可提前释放资源或提前交付", report.SPI))
		default:
			insights = append(insights, fmt.Sprintf("进度符合计划（SPI=%.2f）", report.SPI))
		}
	}

	if report.ACValid {
		switch {
		case report.CPI < HealthRateWarning:
			insights = append(insights, fmt.Sprintf("成本严重超支（CPI=%.2f），实际成本远高于挣值，需要立即复盘工时投入", report.CPI))
		case report.CPI < HealthRateHealthy:
			insights = append(insights, fmt.Sprintf("成本超支（CPI=%.2f），实际成本高于挣值", report.CPI))
		case report.CPI > HealthRateAhead:
			insights = append(insights, fmt.Sprintf("成本优于计划（CPI=%.2f），单位成本产出高于计划", report.CPI))
		default:
			insights = append(insights, fmt.Sprintf("成本符合计划（CPI=%.2f）", report.CPI))
		}
	}

	if report.CPIValid {
		if report.VAC < 0 {
			insights = append(insights, fmt.Sprintf("按当前成本绩效推算，完工估算 EAC=%.0f 高于预算 BAC=%.0f，预计超支 %.0f", report.EAC, report.BAC, -report.VAC))
		} else if report.VAC > 0 {
			insights = append(insights, fmt.Sprintf("按当前成本绩效推算，完工估算 EAC=%.0f 低于预算 BAC=%.0f，预计结余 %.0f", report.EAC, report.BAC, report.VAC))
		} else {
			insights = append(insights, fmt.Sprintf("按当前成本绩效推算，完工估算 EAC=%.0f 与预算一致", report.EAC))
		}
	}

	if len(insights) == 0 {
		insights = append(insights, "剔除分母为零的情况后各指标均正常，无可提示偏差")
	}
	return insights
}

func round2(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*100) / 100
}

type projectFinder interface {
	FindProjectById(ctx context.Context, projectCode int64) (*data.Project, error)
}

type EVMDomain struct {
	projectRepo projectFinder
	records     RecordLoader
}

func (d *EVMDomain) Report(ctx context.Context, projectCode int64, beginTime, endTime int64) (*EVMReport, error) {
	if projectCode <= 0 {
		return nil, errors.New("projectCode 无效")
	}
	if beginTime <= 0 || endTime <= 0 || beginTime > endTime {
		return nil, errors.New("统计区间无效")
	}
	if d.records == nil {
		return nil, errors.New("数据源未装配")
	}

	if d.projectRepo != nil {
		project, err := d.projectRepo.FindProjectById(ctx, projectCode)
		if err != nil {
			return nil, err
		}
		if project == nil {
			return nil, ErrProjectNotFound
		}
	}

	tasks, err := d.records.LoadProjectTasks(ctx, projectCode)
	if err != nil {
		return nil, err
	}
	for _, task := range tasks {
		if task != nil && task.BeginTime <= 0 {
			task.BeginTime = beginTime
		}
	}

	workTimeByTask, err := d.records.LoadProjectWorkTime(ctx, projectCode, tasks, beginTime, endTime)
	if err != nil {
		return nil, err
	}

	report := CalculateEVM(&EVMSnapshot{
		ProjectCode:    projectCode,
		CutoffTime:     endTime,
		Tasks:          tasks,
		WorkTimeByTask: workTimeByTask,
	})
	if report != nil {
		report.BeginTime = beginTime
		report.EndTime = endTime
	}
	return report, nil
}

func NewEVMDomain() *EVMDomain {
	return &EVMDomain{
		projectRepo: dao.NewProjectDao(),
		records: NewProjectRecordSource(
			dao.NewTaskDao(),
			dao.NewTaskWorkTimeDao(),
			dao.NewTaskDependencyDao(),
		),
	}
}
