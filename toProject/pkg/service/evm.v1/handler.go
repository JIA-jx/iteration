package evm_v1

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	common "test.com/others"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/domain"
)

type Handler struct {
	evm *domain.EVMDomain
	cpm *domain.CPMDomain
}

func NewHandler(evm *domain.EVMDomain, cpm *domain.CPMDomain) *Handler {
	return &Handler{evm: evm, cpm: cpm}
}

func (h *Handler) evmDomain() *domain.EVMDomain {
	if h.evm != nil {
		return h.evm
	}
	return domain.NewEVMDomain()
}

func (h *Handler) cpmDomain() *domain.CPMDomain {
	if h.cpm != nil {
		return h.cpm
	}
	return domain.NewCPMDomain()
}

func (h *Handler) Route(r *gin.Engine) {
	g := r.Group("/iteration")
	g.POST("/evm/report", h.evmReport)
	g.POST("/cpm/critical-path", h.cpmCriticalPath)
	g.POST("/cpm/auto-schedule", h.cpmAutoSchedule)
	g.POST("/cpm/dependency/save", h.cpmDependencySave)
	g.POST("/cpm/dependency/delete", h.cpmDependencyDelete)
	g.POST("/cpm/dependency/list", h.cpmDependencyList)
}

func (h *Handler) evmReport(c *gin.Context) {
	result := &common.Result{}
	ctx := c.Request.Context()

	projectCode, err := parsePositiveInt64(c.PostForm("projectCode"), "projectCode 无效")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}
	beginTime, err := parsePositiveInt64(c.PostForm("beginTime"), "beginTime 无效")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}
	endTime, err := parsePositiveInt64(c.PostForm("endTime"), "endTime 无效")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}
	if beginTime > endTime {
		c.JSON(http.StatusOK, result.Fail(400, "beginTime 不能晚于 endTime"))
		return
	}

	report, err := h.evmDomain().Report(ctx, projectCode, beginTime, endTime)
	if err != nil {
		writeDomainError(c, result, err)
		return
	}
	if report == nil {
		c.JSON(http.StatusOK, result.Fail(500, "挣值计算返回空结果"))
		return
	}
	c.JSON(http.StatusOK, result.Success(report))
}

func (h *Handler) cpmCriticalPath(c *gin.Context) {
	result := &common.Result{}
	ctx := c.Request.Context()

	projectCode, err := parsePositiveInt64(c.PostForm("projectCode"), "projectCode 无效")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}

	cpm := h.cpmDomain()
	if err := cpm.ValidateProject(ctx, projectCode); err != nil {
		writeDomainError(c, result, err)
		return
	}

	criticalPath, err := cpm.CriticalPath(ctx, formatInt64(projectCode))
	if err != nil {
		writeDomainError(c, result, err)
		return
	}
	if criticalPath == nil {
		c.JSON(http.StatusOK, result.Fail(500, "关键路径计算返回空结果"))
		return
	}
	c.JSON(http.StatusOK, result.Success(criticalPath))
}

func (h *Handler) cpmAutoSchedule(c *gin.Context) {
	result := &common.Result{}
	ctx := c.Request.Context()

	projectCode, err := parsePositiveInt64(c.PostForm("projectCode"), "projectCode 无效")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}

	cpm := h.cpmDomain()
	if err := cpm.ValidateProject(ctx, projectCode); err != nil {
		writeDomainError(c, result, err)
		return
	}

	changes, err := cpm.AutoSchedule(ctx, formatInt64(projectCode))
	if err != nil {
		writeDomainError(c, result, err)
		return
	}
	if len(changes) == 0 {
		c.JSON(http.StatusOK, result.Success(gin.H{
			"changes":    []*domain.ScheduleChange{},
			"changed":    0,
			"idempotent": true,
			"message":    "当前计划已满足全部依赖约束，无需顺延",
		}))
		return
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"changes":    changes,
		"changed":    len(changes),
		"idempotent": false,
	}))
}

func (h *Handler) cpmDependencySave(c *gin.Context) {
	result := &common.Result{}
	ctx := c.Request.Context()

	projectCode, err := parsePositiveInt64(c.PostForm("projectCode"), "projectCode 无效")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}
	taskCode, err := parsePositiveInt64(c.PostForm("taskCode"), "taskCode 无效")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}
	preTaskCode, err := parsePositiveInt64(c.PostForm("preTaskCode"), "preTaskCode 无效")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}

	depType := c.PostForm("type")
	if _, err := domain.NormalizeDependencyType(depType); err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}

	var lag int64
	if raw := strings.TrimSpace(c.PostForm("lag")); raw != "" {
		lag, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			c.JSON(http.StatusOK, result.Fail(400, "lag 无效，必须是毫秒整数"))
			return
		}
	}

	cpm := h.cpmDomain()
	if err := cpm.ValidateProject(ctx, projectCode); err != nil {
		writeDomainError(c, result, err)
		return
	}

	dependency, err := cpm.SaveDependency(ctx, projectCode, taskCode, preTaskCode, depType, lag)
	if err != nil {
		writeDomainError(c, result, err)
		return
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"dependency": dependency,
		"typeName":   domain.DependencyTypeName(dependency.Type),
	}))
}

func (h *Handler) cpmDependencyDelete(c *gin.Context) {
	result := &common.Result{}
	ctx := c.Request.Context()

	var dependencyId int64
	if raw := strings.TrimSpace(c.PostForm("taskDependencyId")); raw != "" {
		dependencyId, _ = strconv.ParseInt(raw, 10, 64)
	}
	projectCode, pErr := parsePositiveInt64(c.PostForm("projectCode"), "projectCode 无效")
	if pErr != nil {
		c.JSON(http.StatusOK, result.Fail(400, pErr.Error()))
		return
	}
	taskCode, _ := strconv.ParseInt(strings.TrimSpace(c.PostForm("taskCode")), 10, 64)
	preTaskCode, _ := strconv.ParseInt(strings.TrimSpace(c.PostForm("preTaskCode")), 10, 64)

	if dependencyId <= 0 && (taskCode <= 0 || preTaskCode <= 0) {
		c.JSON(http.StatusOK, result.Fail(400, "请提供 taskDependencyId，或 projectCode+taskCode+preTaskCode"))
		return
	}

	deleted, err := h.cpmDomain().DeleteDependency(ctx, dependencyId, projectCode, taskCode, preTaskCode)
	if err != nil {
		writeDomainError(c, result, err)
		return
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"deleted": deleted,
		"message": deleteMessage(deleted),
	}))
}

func (h *Handler) cpmDependencyList(c *gin.Context) {
	result := &common.Result{}
	ctx := c.Request.Context()

	projectCode, err := parsePositiveInt64(c.PostForm("projectCode"), "projectCode 无效")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}

	cpm := h.cpmDomain()
	if err := cpm.ValidateProject(ctx, projectCode); err != nil {
		writeDomainError(c, result, err)
		return
	}

	list, err := cpm.ListDependency(ctx, projectCode)
	if err != nil {
		writeDomainError(c, result, err)
		return
	}
	if list == nil {
		list = []*data.TaskDependency{}
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  list,
		"total": len(list),
	}))
}

func parsePositiveInt64(raw string, message string) (int64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, errors.New(message)
	}
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || value <= 0 {
		return 0, errors.New(message)
	}
	return value, nil
}

func formatInt64(value int64) string {
	return strconv.FormatInt(value, 10)
}

func deleteMessage(deleted int64) string {
	if deleted <= 0 {
		return "未找到匹配的依赖关系"
	}
	return "依赖关系已删除"
}

func writeDomainError(c *gin.Context, result *common.Result, err error) {
	if err == nil {
		c.JSON(http.StatusOK, result.Fail(500, "未知错误"))
		return
	}

	var cycleErr *domain.DependencyCycleError
	if errors.As(err, &cycleErr) {
		c.JSON(http.StatusOK, result.Fail(409, "检测到任务依赖环："+cycleErr.Path()))
		return
	}

	var unsupportedErr *domain.UnsupportedDependencyError
	if errors.As(err, &unsupportedErr) {
		c.JSON(http.StatusOK, result.Fail(400, unsupportedErr.Error()))
		return
	}

	switch {
	case errors.Is(err, domain.ErrProjectNotFound):
		c.JSON(http.StatusOK, result.Fail(404, err.Error()))
	case errors.Is(err, domain.ErrProjectCodeRequired),
		errors.Is(err, domain.ErrNoTasksInProject),
		errors.Is(err, domain.ErrDependencySelfRef),
		errors.Is(err, domain.ErrGraphEmpty):
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
	default:
		c.JSON(http.StatusOK, result.Fail(500, err.Error()))
	}
}
