package ops_v1

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	common "test.com/others"
	"test.com/others/audit"
	"test.com/others/llm"
	"test.com/others/tenant"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
	"test.com/toProject/internal/worker"
)

type Handler struct {
	reconcile   *worker.ReconcileWorker
	archive     *worker.ArchiveWorker
	auditReader audit.Reader
}

func NewHandler(reconcile *worker.ReconcileWorker, archive *worker.ArchiveWorker, auditReader audit.Reader) *Handler {
	return &Handler{
		reconcile:   reconcile,
		archive:     archive,
		auditReader: auditReader,
	}
}

func (h *Handler) Route(r *gin.Engine) {
	g := r.Group("/iteration/ops")
	g.POST("/audit/list", h.auditList)
	g.POST("/reconcile/report", h.reconcileReport)
	g.POST("/reconcile/run", h.reconcileRun)
	g.POST("/archive/run", h.archiveRun)
	g.POST("/replication/stats", h.replicationStats)
	g.POST("/ai/prompts", h.aiPrompts)
	g.POST("/tenant/list", h.tenantList)
	g.POST("/tenant/save", h.tenantSave)
}

func (h *Handler) tenantList(c *gin.Context) {
	result := &common.Result{}
	list, err := dao.ListTenants(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "查询租户失败: "+err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  list,
		"total": len(list),
	}))
}

func (h *Handler) tenantSave(c *gin.Context) {
	result := &common.Result{}
	ctx := c.Request.Context()

	tenantId := parseInt64Default(c.PostForm("id"), 0)
	row := &data.Tenant{}
	if tenantId > 0 {
		existing, err := dao.FindTenantById(ctx, tenantId)
		if err != nil {
			c.JSON(http.StatusOK, result.Fail(500, "查询租户失败: "+err.Error()))
			return
		}
		if existing == nil {
			c.JSON(http.StatusOK, result.Fail(404, "租户不存在"))
			return
		}
		row = existing
	}

	if v := strings.TrimSpace(c.PostForm("code")); v != "" {
		row.Code = v
	}
	if v := strings.TrimSpace(c.PostForm("name")); v != "" {
		row.Name = v
	}
	if row.Code == "" || row.Name == "" {
		c.JSON(http.StatusOK, result.Fail(400, "code 与 name 不能为空"))
		return
	}
	if v := strings.TrimSpace(c.PostForm("status")); v != "" {
		row.Status = int8(parseInt64Default(v, 1))
	}
	if v := strings.TrimSpace(c.PostForm("memberQuota")); v != "" {
		row.MemberQuota = int(parseInt64Default(v, 0))
	}
	if v := strings.TrimSpace(c.PostForm("projectQuota")); v != "" {
		row.ProjectQuota = int(parseInt64Default(v, 0))
	}
	if v := strings.TrimSpace(c.PostForm("storageQuotaMb")); v != "" {
		row.StorageQuotaMb = parseInt64Default(v, 0)
	}
	if v := strings.TrimSpace(c.PostForm("aiQuotaTokens")); v != "" {
		row.AIQuotaTokens = parseInt64Default(v, 0)
	}
	if v := strings.TrimSpace(c.PostForm("expireAt")); v != "" {
		if ms := parseInt64Default(v, 0); ms > 0 {
			ts := time.UnixMilli(ms)
			row.ExpireAt = &ts
		}
	}

	if err := dao.SaveTenant(ctx, row); err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "保存租户失败: "+err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(row))
}

func (h *Handler) auditList(c *gin.Context) {
	result := &common.Result{}
	if h.auditReader == nil {
		c.JSON(http.StatusOK, result.Fail(503, "审计组件未初始化"))
		return
	}

	query := audit.Query{
		TenantId:   tenant.IdFromContext(c.Request.Context()),
		Action:     c.PostForm("action"),
		Resource:   c.PostForm("resource"),
		ResourceId: c.PostForm("resourceId"),
		ActorId:    parseInt64Default(c.PostForm("actorId"), 0),
		TraceId:    c.PostForm("traceId"),
		BeginTime:  parseTimeDefault(c.PostForm("beginTime")),
		EndTime:    parseTimeDefault(c.PostForm("endTime")),
		Page:       int(parseInt64Default(c.PostForm("page"), 1)),
		PageSize:   int(parseInt64Default(c.PostForm("pageSize"), 20)),
	}

	list, total, err := h.auditReader.List(c.Request.Context(), query)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "查询审计日志失败: "+err.Error()))
		return
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  list,
		"total": total,
		"page":  query.Page,
	}))
}

func (h *Handler) reconcileReport(c *gin.Context) {
	result := &common.Result{}
	if h.reconcile == nil {
		c.JSON(http.StatusOK, result.Fail(503, "对账组件未初始化"))
		return
	}
	c.JSON(http.StatusOK, result.Success(h.reconcile.LastReport()))
}

func (h *Handler) reconcileRun(c *gin.Context) {
	result := &common.Result{}
	if h.reconcile == nil {
		c.JSON(http.StatusOK, result.Fail(503, "对账组件未初始化"))
		return
	}

	report, err := h.reconcile.Reconcile(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "对账执行失败: "+err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(report))
}

func (h *Handler) archiveRun(c *gin.Context) {
	result := &common.Result{}
	if h.archive == nil {
		c.JSON(http.StatusOK, result.Fail(503, "归档组件未初始化"))
		return
	}

	report, err := h.archive.ArchiveOnce(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "归档执行失败: "+err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(report))
}

func (h *Handler) replicationStats(c *gin.Context) {
	result := &common.Result{}
	stats := gorms.ReplicationStats()
	c.JSON(http.StatusOK, result.Success(gin.H{
		"reads":            stats.Reads,
		"masterReads":      stats.MasterReads,
		"slaveReads":       stats.SlaveReads,
		"autoReads":        stats.AutoReads,
		"fallbackToMaster": stats.FallbackRead,
		"replicationLagMs": gorms.ReplicationLag().Milliseconds(),
	}))
}

func (h *Handler) aiPrompts(c *gin.Context) {
	result := &common.Result{}
	c.JSON(http.StatusOK, result.Success(llm.ListPrompts()))
}

func parseInt64Default(raw string, def int64) int64 {
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return def
	}
	return v
}

func parseTimeDefault(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{}
	}
	if ms < 1e12 {
		return time.Unix(ms, 0)
	}
	return time.UnixMilli(ms)
}
