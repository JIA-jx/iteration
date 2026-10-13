package domain

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"test.com/others/llm"
	"test.com/toProject/config"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
)

const (
	PromptTaskAnalyze = "task_analyze"
	PromptVersionV1   = "v1"
)

type AIClient interface {
	CallJSON(ctx context.Context, prompt string, out interface{}) error
}

type AIDomain struct {
	client       AIClient
	fallbackOnly bool
}

type AnalyzeResult struct {
	Result   *data.AIResult
	Degraded bool
	Reason   string
}

func NewAIDomain() *AIDomain {
	return newAIDomainWithClient(buildDefaultAIClient())
}

func newAIDomainWithClient(client AIClient) *AIDomain {
	return &AIDomain{
		client:       client,
		fallbackOnly: client == nil,
	}
}

func buildDefaultAIClient() AIClient {
	if config.AIAPIKEY == "" {
		return nil
	}

	var cache llm.Cache
	if dao.Rc != nil && dao.Rc.Rdb != nil {
		cache = llm.NewRedisCache(dao.Rc.Rdb)
	}

	return llm.WithDefaults(config.AIAPIKEY, cache)
}

func init() {
	_ = llm.RegisterPrompt(llm.PromptTemplate{
		Name:         PromptTaskAnalyze,
		Version:      PromptVersionV1,
		Description:  "任务分析与风险识别",
		RequiredVars: []string{"task_id", "task_name", "status", "assign_to", "pri", "logs"},
		Template: `你是一个研发任务分析专家。请严格按 JSON 格式返回，不要输出多余文字。
任务数据：
任务ID：{{task_id}}
任务名称：{{task_name}}
状态：{{status}}
负责人：{{assign_to}}
优先级：{{pri}}

执行日志：
{{logs}}

请返回如下结构的 JSON：
{
  "Suggest": "优化建议",
  "Analysis": "任务分析描述",
  "WorkHour": 预估总工时,
  "RemainHour": 预估剩余工时
}`,
	})
}

func (d *AIDomain) TaskAnalyzeContext(ctx context.Context, task *data.Task, logs []*data.TaskLog) (*AnalyzeResult, error) {
	if task == nil {
		return nil, errors.New("任务不能为空")
	}

	if d.client == nil || d.fallbackOnly {
		return d.heuristicAnalyze(task, logs, "ai client not configured"), nil
	}

	if !dao.AIQuotaAvailableFor(ctx) {
		zap.L().Warn("ai quota unavailable, degraded to heuristic",
			zap.Int64("task_id", task.Id))
		return d.heuristicAnalyze(task, logs, "ai quota exceeded or tenant disabled"), nil
	}

	prompt, err := buildTaskAnalyzePrompt(task, logs)
	if err != nil {
		return d.heuristicAnalyze(task, logs, "prompt render failed: "+err.Error()), nil
	}

	callCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	var result data.AIResult
	if err := d.client.CallJSON(callCtx, prompt, &result); err != nil {
		zap.L().Warn("ai task analyze degraded to heuristic",
			zap.Int64("task_id", task.Id), zap.Error(err))
		return d.heuristicAnalyze(task, logs, err.Error()), nil
	}

	if strings.TrimSpace(result.Suggest) == "" && strings.TrimSpace(result.Analysis) == "" {
		return d.heuristicAnalyze(task, logs, "ai returned empty content"), nil
	}

	if result.WorkHour <= 0 {
		result.WorkHour = task.Schedule
	}
	if result.RemainHour < 0 {
		result.RemainHour = 0
	}

	return &AnalyzeResult{Result: &result}, nil
}

func (d *AIDomain) TaskAnalyzeWithFallback(ctx context.Context, task *data.Task, logs []*data.TaskLog) *AnalyzeResult {
	if task == nil {
		return &AnalyzeResult{
			Result:   &data.AIResult{Analysis: "任务不能为空"},
			Degraded: true,
			Reason:   "nil task",
		}
	}

	res, err := d.TaskAnalyzeContext(ctx, task, logs)
	if err != nil || res == nil {
		return d.heuristicAnalyze(task, logs, fmt.Sprintf("%v", err))
	}
	return res
}

func buildTaskAnalyzePrompt(task *data.Task, logs []*data.TaskLog) (string, error) {
	var sb strings.Builder
	for _, l := range logs {
		sb.WriteString("- ")
		sb.WriteString(l.CreateTime)
		sb.WriteString(": ")
		sb.WriteString(l.Content)
		sb.WriteString("\n")
	}

	return llm.Render(PromptTaskAnalyze, PromptVersionV1, map[string]string{
		"task_id":   fmt.Sprintf("%d", task.Id),
		"task_name": task.Name,
		"status":    fmt.Sprintf("%d", task.Status),
		"assign_to": fmt.Sprintf("%d", task.AssignTo),
		"pri":       fmt.Sprintf("%d", task.Pri),
		"logs":      sb.String(),
	})
}

func (d *AIDomain) heuristicAnalyze(task *data.Task, logs []*data.TaskLog, reason string) *AnalyzeResult {
	planned := task.Schedule
	if planned <= 0 {
		planned = defaultSchedule
	}

	progress := taskProgress(task)
	remain := planned - int(float64(planned)*progress)
	if remain < 0 {
		remain = 0
	}

	risk := ""
	if task.EndTime > 0 && task.Done != 1 && task.EndTime < time.Now().UnixMilli() {
		risk = "任务已超过计划截止时间且未完成"
	}
	if len(logs) == 0 {
		if risk != "" {
			risk += "；"
		}
		risk += "缺少执行日志，无法判断实际推进情况"
	}

	suggest := "按当前进度推进"
	if remain > planned/2 {
		suggest = "剩余工时较多，建议拆分任务或补充人力"
	}
	if risk != "" {
		suggest = "存在风险，建议尽快同步负责人并调整排期"
	}

	return &AnalyzeResult{
		Degraded: true,
		Reason:   reason,
		Result: &data.AIResult{
			Suggest:    suggest,
			Analysis:   fmt.Sprintf("基于规则的降级分析：计划工时 %d，完成度约 %d%%", planned, int(progress*100)),
			WorkHour:   planned,
			RemainHour: remain,
		},
	}
}

const defaultSchedule = 8

func taskProgress(task *data.Task) float64 {
	if task.Done == 1 {
		return 1
	}

	switch task.ExecuteStatus {
	case data.Done, data.Closed:
		return 1
	case data.Doing:
		return 0.5
	default:
		return 0
	}
}

func (d *AIDomain) EvalMemberLoad(workHour int, taskCount int) (level int32, suggest string) {
	score := workHour/8 + taskCount

	switch {
	case score <= 2:
		return 1, "负载极低，可分配更多任务"
	case score <= 4:
		return 2, "负载适中，工作状态良好"
	case score <= 7:
		return 3, "负载偏高，建议适当减少新任务"
	default:
		return 4, "负载过载，必须暂停新任务，优先处理现有工作"
	}
}

func (d *AIDomain) CheckTaskRisk(task *data.Task, workHour int, remainHour int) (risk string, err error) {
	if task == nil {
		return "", errors.New("任务不能为空")
	}

	if remainHour > task.Schedule {
		risk += "【延期风险】剩余工时超过计划工时，可能延期\n"
	}

	level, _ := d.EvalMemberLoad(workHour, 1)
	if level >= 3 {
		risk += "【资源风险】负责人当前负载过高，影响任务推进\n"
	}

	if risk == "" {
		risk = "暂无明显风险"
	}

	return risk, nil
}
