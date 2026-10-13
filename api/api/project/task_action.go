package project

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"test.com/api/api/rpc"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/task"
)

func splitCodes(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func (t *HandlerRequirement) batchAction(action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		result := &common.Result{}
		codes := splitCodes(c.DefaultPostForm("taskCodes", c.PostForm("task_codes")))
		if len(codes) == 0 {
			if single := c.DefaultPostForm("taskCode", c.PostForm("task_code")); single != "" {
				codes = []string{single}
			}
		}
		if len(codes) == 0 {
			c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "taskCodes 不能为空"))
			return
		}

		value, _ := strconv.Atoi(c.DefaultPostForm("value", "1"))

		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		resp, err := rpc.TaskServiceClient.TaskBatchAction(ctx, &task.TaskBatchActionReq{
			MemberId:   c.GetInt64("memberId"),
			TaskCodes:  codes,
			MemberCode: c.DefaultPostForm("memberCode", c.PostForm("member_code")),
			Value:      int32(value),
			Tag:        c.PostForm("tag"),
			Action:     action,
		})
		if err != nil {
			code, m := errs.ParseGrpcError(err)
			c.JSON(http.StatusOK, result.Fail(code, m))
			return
		}
		c.JSON(http.StatusOK, result.Success(resp))
	}
}

func (t *HandlerRequirement) memberAction(action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		result := &common.Result{}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		memberCodes := splitCodes(c.DefaultPostForm("memberCodes", c.PostForm("member_codes")))
		if len(memberCodes) == 0 {
			if single := c.DefaultPostForm("memberCode", c.PostForm("member_code")); single != "" {
				memberCodes = []string{single}
			}
		}

		resp, err := rpc.TaskServiceClient.TaskMemberAction(ctx, &task.TaskMemberActionReq{
			MemberId:    c.GetInt64("memberId"),
			TaskCode:    c.DefaultPostForm("taskCode", c.PostForm("task_code")),
			MemberCodes: memberCodes,
			Keyword:     c.DefaultPostForm("keyword", c.PostForm("search")),
			Action:      action,
		})
		if err != nil {
			code, m := errs.ParseGrpcError(err)
			c.JSON(http.StatusOK, result.Fail(code, m))
			return
		}
		c.JSON(http.StatusOK, result.Success(resp))
	}
}

func (t *HandlerRequirement) stagesAction(action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		result := &common.Result{}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		sort, _ := strconv.Atoi(c.DefaultPostForm("sort", "0"))

		resp, err := rpc.TaskServiceClient.TaskStagesAction(ctx, &task.TaskStagesActionReq{
			MemberId:    c.GetInt64("memberId"),
			ProjectCode: c.DefaultPostForm("projectCode", c.PostForm("project_code")),
			StageCode:   c.DefaultPostForm("stageCode", c.PostForm("stage_code")),
			Name:        c.PostForm("name"),
			Description: c.PostForm("description"),
			Sort:        int32(sort),
			Action:      action,
		})
		if err != nil {
			code, m := errs.ParseGrpcError(err)
			c.JSON(http.StatusOK, result.Fail(code, m))
			return
		}
		c.JSON(http.StatusOK, result.Success(resp))
	}
}

func (t *HandlerRequirement) templateAction(action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		result := &common.Result{}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		templateCode := c.DefaultPostForm("templateCode", c.PostForm("template_code"))
		if action == "edit" || action == "delete" {
			if v := c.DefaultPostForm("stageCode", c.PostForm("stage_code")); v != "" {
				templateCode = v
			}
		}

		resp, err := rpc.TaskServiceClient.TaskStagesTemplateAction(ctx, &task.TaskStagesTemplateActionReq{
			MemberId:     c.GetInt64("memberId"),
			TemplateCode: templateCode,
			Names:        splitCodes(c.DefaultPostForm("names", c.PostForm("name"))),
			Action:       action,
		})
		if err != nil {
			code, m := errs.ParseGrpcError(err)
			c.JSON(http.StatusOK, result.Fail(code, m))
			return
		}
		c.JSON(http.StatusOK, result.Success(resp))
	}
}

func (t *HandlerRequirement) tagAction(action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		result := &common.Result{}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		page, _ := strconv.ParseInt(c.DefaultPostForm("page", "1"), 10, 64)
		pageSize, _ := strconv.ParseInt(c.DefaultPostForm("pageSize", "20"), 10, 64)
		sort, _ := strconv.Atoi(c.DefaultPostForm("sort", "0"))

		resp, err := rpc.TaskServiceClient.TaskTagAction(ctx, &task.TaskTagActionReq{
			MemberId:    c.GetInt64("memberId"),
			ProjectCode: c.DefaultPostForm("projectCode", c.PostForm("project_code")),
			TagId:       c.DefaultPostForm("tagId", c.PostForm("tag_id")),
			Name:        c.DefaultPostForm("name", c.PostForm("tag")),
			Color:       c.PostForm("color"),
			Sort:        int32(sort),
			Page:        page,
			PageSize:    pageSize,
			Action:      action,
		})
		if err != nil {
			code, m := errs.ParseGrpcError(err)
			c.JSON(http.StatusOK, result.Fail(code, m))
			return
		}
		c.JSON(http.StatusOK, result.Success(resp))
	}
}

func (t *HandlerRequirement) workTimeAction(action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		result := &common.Result{}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		workTimeId, _ := strconv.ParseInt(c.DefaultPostForm("workTimeId", c.PostForm("work_time_id")), 10, 64)
		workTimeId = decryptOrZero(c.DefaultPostForm("code", c.PostForm("workTimeCode")), workTimeId)
		page, _ := strconv.ParseInt(c.DefaultPostForm("page", "1"), 10, 64)
		pageSize, _ := strconv.ParseInt(c.DefaultPostForm("pageSize", "20"), 10, 64)
		num, _ := strconv.Atoi(c.DefaultPostForm("num", "0"))

		resp, err := rpc.TaskServiceClient.TaskWorkTimeAction(ctx, &task.TaskWorkTimeActionReq{
			MemberId:   c.GetInt64("memberId"),
			TaskCode:   c.DefaultPostForm("taskCode", c.PostForm("task_code")),
			WorkTimeId: workTimeId,
			Content:    c.PostForm("content"),
			Num:        int32(num),
			BeginTime:  parseTimeOrZero(c.PostForm("beginTime")),
			Page:       page,
			PageSize:   pageSize,
			Action:     action,
		})
		if err != nil {
			code, m := errs.ParseGrpcError(err)
			c.JSON(http.StatusOK, result.Fail(code, m))
			return
		}
		c.JSON(http.StatusOK, result.Success(resp))
	}
}
