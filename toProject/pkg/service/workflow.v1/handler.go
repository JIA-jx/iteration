package workflow_v1

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	common "test.com/others"
	"test.com/others/wfengine"
	"test.com/toProject/internal/dao"
)

type Handler struct {
	svc *Service
}

func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) Route(r *gin.Engine) {
	g := r.Group("/iteration/workflow")
	g.POST("/definition/save", h.saveDefinition)
	g.POST("/definition/list", h.listDefinitions)
	g.POST("/definition/get", h.getDefinition)
	g.POST("/start", h.start)
	g.POST("/submit", h.submit)
	g.POST("/approve", h.approve)
	g.POST("/reject", h.reject)
	g.POST("/transfer", h.transfer)
	g.POST("/add-signer", h.addSigner)
	g.POST("/withdraw", h.withdraw)
	g.POST("/get", h.get)
	g.POST("/list-by-biz", h.listByBiz)
	g.POST("/pending-tasks", h.pendingTasks)
	g.POST("/timeout/scan", h.scanTimeout)
}

func (h *Handler) ready(c *gin.Context, result *common.Result) bool {
	if h.svc == nil || h.svc.Engine() == nil {
		c.JSON(http.StatusOK, result.Fail(503, "审批流服务未初始化"))
		return false
	}
	return true
}

func (h *Handler) saveDefinition(c *gin.Context) {
	result := &common.Result{}
	if !h.ready(c, result) {
		return
	}

	var def wfengine.Definition
	if err := bindDefinition(c, &def); err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "流程定义解析失败: "+err.Error()))
		return
	}

	activate := c.PostForm("activate") != "0"
	row, err := h.svc.SaveDefinition(c.Request.Context(), 0, &def, c.PostForm("remark"), activate)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, err.Error()))
		return
	}

	dao.AuditFromGin(c, "save_definition", "workflow_definition", def.Key, nil, row, "保存审批流定义")

	c.JSON(http.StatusOK, result.Success(gin.H{
		"defKey":  row.DefKey,
		"version": row.Version,
		"status":  row.Status,
	}))
}

func (h *Handler) listDefinitions(c *gin.Context) {
	result := &common.Result{}
	if !h.ready(c, result) {
		return
	}

	list, total, err := h.svc.ListDefinitions(c.Request.Context(), 0, c.PostForm("defKey"),
		formInt64(c, "page"), formInt64(c, "pageSize"))
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(gin.H{"list": list, "total": total}))
}

func (h *Handler) getDefinition(c *gin.Context) {
	result := &common.Result{}
	if !h.ready(c, result) {
		return
	}

	def, err := h.svc.LoadDefinition(c.Request.Context(), 0, c.PostForm("defKey"), int(formInt64(c, "version")))
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(404, err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(def))
}

func (h *Handler) start(c *gin.Context) {
	result := &common.Result{}
	if !h.ready(c, result) {
		return
	}

	variables := map[string]string{}
	if raw := c.PostForm("variables"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &variables); err != nil {
			c.JSON(http.StatusOK, result.Fail(400, "variables 必须是 JSON 对象"))
			return
		}
	}

	inst, err := h.svc.Start(c.Request.Context(), 0, c.PostForm("defKey"),
		int(formInt64(c, "version")), formInt64(c, "submitterId"), c.PostForm("bizId"), variables)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(mapWorkflowError(err), err.Error()))
		return
	}

	dao.AuditFromGin(c, "start", "workflow_instance", inst.Id, nil, gin.H{
		"defKey": inst.DefinitionKey,
		"bizId":  inst.BizId,
		"status": inst.Status,
	}, "发起审批流")

	c.JSON(http.StatusOK, result.Success(inst))
}

func (h *Handler) submit(c *gin.Context) {
	h.progress(c, func() (interface{}, error) {
		return h.svc.Submit(c.Request.Context(), c.PostForm("instanceId"), formInt64(c, "actor"), c.PostForm("comment"))
	})
}

func (h *Handler) approve(c *gin.Context) {
	h.progress(c, func() (interface{}, error) {
		return h.svc.Approve(c.Request.Context(), c.PostForm("instanceId"), c.PostForm("taskId"),
			formInt64(c, "actor"), c.PostForm("comment"))
	})
}

func (h *Handler) reject(c *gin.Context) {
	h.progress(c, func() (interface{}, error) {
		return h.svc.Reject(c.Request.Context(), c.PostForm("instanceId"), c.PostForm("taskId"),
			formInt64(c, "actor"), c.PostForm("comment"), c.PostForm("targetNodeKey"))
	})
}

func (h *Handler) transfer(c *gin.Context) {
	h.progress(c, func() (interface{}, error) {
		return h.svc.Transfer(c.Request.Context(), c.PostForm("instanceId"), c.PostForm("taskId"),
			formInt64(c, "actor"), formInt64(c, "toMemberId"), c.PostForm("comment"))
	})
}

func (h *Handler) addSigner(c *gin.Context) {
	h.progress(c, func() (interface{}, error) {
		memberIds, err := parseIdList(c.PostForm("memberIds"))
		if err != nil {
			return nil, err
		}
		before := c.PostForm("before") == "1" || strings.EqualFold(c.PostForm("before"), "true")
		return h.svc.AddSigner(c.Request.Context(), c.PostForm("instanceId"), c.PostForm("taskId"),
			formInt64(c, "actor"), before, memberIds, c.PostForm("comment"))
	})
}

func (h *Handler) withdraw(c *gin.Context) {
	h.progress(c, func() (interface{}, error) {
		return h.svc.Withdraw(c.Request.Context(), c.PostForm("instanceId"), formInt64(c, "actor"), c.PostForm("comment"))
	})
}

func (h *Handler) get(c *gin.Context) {
	result := &common.Result{}
	if !h.ready(c, result) {
		return
	}

	inst, err := h.svc.Get(c.Request.Context(), c.PostForm("instanceId"))
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(mapWorkflowError(err), err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(inst))
}

func (h *Handler) listByBiz(c *gin.Context) {
	result := &common.Result{}
	if !h.ready(c, result) {
		return
	}

	list, err := h.svc.ListByBiz(c.Request.Context(), 0, c.PostForm("bizId"))
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(list))
}

func (h *Handler) pendingTasks(c *gin.Context) {
	result := &common.Result{}
	if !h.ready(c, result) {
		return
	}

	list, err := h.svc.PendingTasks(c.Request.Context(), formInt64(c, "approverId"), 0)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(list))
}

func (h *Handler) scanTimeout(c *gin.Context) {
	result := &common.Result{}
	if !h.ready(c, result) {
		return
	}

	n, err := h.svc.HandleTimeout(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, err.Error()))
		return
	}
	c.JSON(http.StatusOK, result.Success(gin.H{"handled": n}))
}

func (h *Handler) progress(c *gin.Context, fn func() (interface{}, error)) {
	result := &common.Result{}
	if !h.ready(c, result) {
		return
	}

	inst, err := fn()
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(mapWorkflowError(err), err.Error()))
		return
	}

	dao.RecordAudit(c.Request.Context(),
		strings.TrimPrefix(c.Request.URL.Path, "/iteration/workflow/"),
		"workflow_instance", c.PostForm("instanceId"),
		formInt64(c, "actor"), nil, inst, "审批流推进")

	c.JSON(http.StatusOK, result.Success(inst))
}

func mapWorkflowError(err error) common.InterationCode {
	switch {
	case err == nil:
		return 200
	case errors.Is(err, wfengine.ErrVersionConflict):
		return 409
	case errors.Is(err, wfengine.ErrInstanceNotFound), errors.Is(err, wfengine.ErrTaskNotFound):
		return 404
	case errors.Is(err, wfengine.ErrNotApprover),
		errors.Is(err, wfengine.ErrNotSubmitter),
		errors.Is(err, wfengine.ErrNodeNotAllowed):
		return 403
	case errors.Is(err, wfengine.ErrTaskHandled):
		return 409
	default:
		return 400
	}
}

func bindDefinition(c *gin.Context, def *wfengine.Definition) error {
	raw := c.PostForm("definition")
	if strings.TrimSpace(raw) == "" {
		body, err := c.GetRawData()
		if err != nil {
			return err
		}
		raw = string(body)
	}
	if strings.TrimSpace(raw) == "" {
		return errors.New("definition 不能为空")
	}
	return json.Unmarshal([]byte(raw), def)
}

func formInt64(c *gin.Context, key string) int64 {
	raw := strings.TrimSpace(c.PostForm(key))
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func parseIdList(raw string) ([]int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if strings.HasPrefix(raw, "[") {
		var ids []int64
		if err := json.Unmarshal([]byte(raw), &ids); err != nil {
			return nil, err
		}
		return ids, nil
	}

	parts := strings.Split(raw, ",")
	ids := make([]int64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return nil, err
		}
		ids = append(ids, v)
	}
	return ids, nil
}
