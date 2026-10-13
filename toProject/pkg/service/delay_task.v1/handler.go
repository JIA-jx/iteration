package delay_task_v1

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	common "test.com/others"
)

type Handler struct {
	svc *DelayTaskService
}

func NewHandler(svc *DelayTaskService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) svcOrNil() *DelayTaskService {
	if h.svc != nil {
		return h.svc
	}

	return CurrentService()
}

func (h *Handler) Route(r *gin.Engine) {
	g := r.Group("/iteration/delay")
	g.POST("/schedule", h.schedule)
	g.POST("/cancel", h.cancel)
	g.POST("/query", h.query)
	g.POST("/list", h.list)
	g.POST("/stats", h.stats)
	g.POST("/rebuild", h.rebuild)
}

func (h *Handler) schedule(c *gin.Context) {
	result := &common.Result{}
	svc := h.svcOrNil()
	if svc == nil {
		c.JSON(http.StatusOK, result.Fail(503, "延时任务服务未初始化"))
		return
	}

	taskType := c.PostForm("type")
	ctx := c.Request.Context()

	switch taskType {
	case TypeTaskRemind:
		taskId, err := strconv.ParseInt(c.PostForm("taskId"), 10, 64)
		if err != nil || taskId <= 0 {
			c.JSON(http.StatusOK, result.Fail(400, "taskId 无效"))
			return
		}

		t, err := svc.taskQuery.FindTaskById(ctx, taskId)
		if err != nil {
			c.JSON(http.StatusOK, result.Fail(500, "查询任务失败: "+err.Error()))
			return
		}
		if t == nil {
			c.JSON(http.StatusOK, result.Fail(404, "任务不存在"))
			return
		}

		created, err := svc.ScheduleTaskReminder(ctx, t)
		if err != nil {
			c.JSON(http.StatusOK, result.Fail(500, "排期失败: "+err.Error()))
			return
		}
		c.JSON(http.StatusOK, result.Success(gin.H{
			"taskId":  TaskReminderId(taskId),
			"created": created,
			"reason": func() string {
				if created {
					return ""
				}
				return "任务已完成/无截止时间/提醒点已过，无需排期"
			}(),
		}))

	case TypeInviteExpire:
		inviteId := c.PostForm("inviteId")
		if inviteId == "" {
			c.JSON(http.StatusOK, result.Fail(400, "inviteId 不能为空"))
			return
		}
		projectCode, _ := strconv.ParseInt(c.PostForm("projectCode"), 10, 64)
		inviterId, _ := strconv.ParseInt(c.PostForm("inviterId"), 10, 64)
		ttlSeconds, _ := strconv.ParseInt(c.PostForm("ttlSeconds"), 10, 64)
		ttl := time.Duration(ttlSeconds) * time.Second

		expireAt, err := svc.ScheduleInviteExpire(ctx, inviteId, projectCode, inviterId, ttl)
		if err != nil {
			c.JSON(http.StatusOK, result.Fail(500, "排期失败: "+err.Error()))
			return
		}
		c.JSON(http.StatusOK, result.Success(gin.H{
			"taskId":   InviteExpireId(inviteId),
			"expireAt": expireAt,
		}))

	default:
		c.JSON(http.StatusOK, result.Fail(400, "不支持的 type："+taskType))
	}
}

func (h *Handler) cancel(c *gin.Context) {
	result := &common.Result{}
	svc := h.svcOrNil()
	if svc == nil {
		c.JSON(http.StatusOK, result.Fail(503, "延时任务服务未初始化"))
		return
	}

	ctx := c.Request.Context()
	switch c.PostForm("type") {
	case TypeTaskRemind:
		taskId, err := strconv.ParseInt(c.PostForm("taskId"), 10, 64)
		if err != nil || taskId <= 0 {
			c.JSON(http.StatusOK, result.Fail(400, "taskId 无效"))
			return
		}
		if err := svc.CancelTaskReminder(ctx, taskId); err != nil {
			c.JSON(http.StatusOK, result.Fail(500, "取消失败: "+err.Error()))
			return
		}
		c.JSON(http.StatusOK, result.Success(gin.H{"taskId": TaskReminderId(taskId)}))

	case TypeInviteExpire:
		inviteId := c.PostForm("inviteId")
		if inviteId == "" {
			c.JSON(http.StatusOK, result.Fail(400, "inviteId 不能为空"))
			return
		}
		if err := svc.CancelInviteExpire(ctx, inviteId); err != nil {
			c.JSON(http.StatusOK, result.Fail(500, "取消失败: "+err.Error()))
			return
		}
		c.JSON(http.StatusOK, result.Success(gin.H{"taskId": InviteExpireId(inviteId)}))

	default:
		c.JSON(http.StatusOK, result.Fail(400, "不支持的 type"))
	}
}

func (h *Handler) query(c *gin.Context) {
	result := &common.Result{}
	svc := h.svcOrNil()
	if svc == nil {
		c.JSON(http.StatusOK, result.Fail(503, "延时任务服务未初始化"))
		return
	}

	status, err := svc.QueryStatus(c.Request.Context(), c.PostForm("taskId"))
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "查询失败: "+err.Error()))
		return
	}
	if status == nil {
		c.JSON(http.StatusOK, result.Fail(404, "任务不存在"))
		return
	}
	c.JSON(http.StatusOK, result.Success(status))
}

func (h *Handler) list(c *gin.Context) {
	result := &common.Result{}
	svc := h.svcOrNil()
	if svc == nil {
		c.JSON(http.StatusOK, result.Fail(503, "延时任务服务未初始化"))
		return
	}

	page, _ := strconv.ParseInt(c.DefaultPostForm("page", "1"), 10, 64)
	pageSize, _ := strconv.ParseInt(c.DefaultPostForm("pageSize", "20"), 10, 64)

	list, total, err := svc.List(c.Request.Context(), c.PostForm("type"), c.PostForm("status"), page, pageSize)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "查询失败: "+err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  list,
		"total": total,
	}))
}

func (h *Handler) stats(c *gin.Context) {
	result := &common.Result{}
	svc := h.svcOrNil()
	if svc == nil {
		c.JSON(http.StatusOK, result.Fail(503, "延时任务服务未初始化"))
		return
	}

	pending, inflight, err := svc.QueueStats(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "查询失败: "+err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"pending":  pending,
		"inflight": inflight,
	}))
}

func (h *Handler) rebuild(c *gin.Context) {
	result := &common.Result{}
	svc := h.svcOrNil()
	if svc == nil {
		c.JSON(http.StatusOK, result.Fail(503, "延时任务服务未初始化"))
		return
	}

	limit, _ := strconv.Atoi(c.DefaultPostForm("limit", "200"))
	created, err := svc.RebuildReminders(c.Request.Context(), limit)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "重建失败: "+err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(gin.H{"created": created}))
}
