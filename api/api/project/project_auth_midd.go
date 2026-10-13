package project

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"
	"io"
	"net/http"
	"strconv"
	"strings"
	"test.com/api/api/rpc"
	"test.com/api/pkg/model/pro"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/project"
	"time"
)

func InterationAuth() func(*gin.Context) {
	return func(c *gin.Context) {

		result := &common.Result{}

		projectCode := c.PostForm("projectCode")
		if projectCode == "" {
			projectCode = c.PostForm("project_code")
		}
		taskCode := c.PostForm("taskCode")
		if taskCode == "" {
			taskCode = c.PostForm("task_code")
		}
		if taskCode == "" {
			taskCode = firstCode(
				c.PostForm("taskCodes"),
				c.PostForm("task_codes"),
				c.PostForm("preTaskCode"),
				c.PostForm("pre_task_code"),
				c.PostForm("nextTaskCode"),
				c.PostForm("next_task_code"),
			)
		}

		if projectCode == "" && taskCode == "" && c.Request != nil && c.Request.Body != nil &&
			strings.Contains(c.GetHeader("Content-Type"), "application/json") {
			body, rErr := io.ReadAll(c.Request.Body)
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
			if rErr == nil && len(body) > 0 {
				var raw map[string]interface{}
				if json.Unmarshal(body, &raw) == nil {
					if projectCode == "" {
						projectCode = jsonScalar(raw, "projectCode", "project_code")
					}
					if taskCode == "" {
						taskCode = jsonScalar(raw, "taskCode", "task_code")
					}
				}
			}
		}

		isProjectAuth := projectCode != "" || taskCode != ""
		if isProjectAuth {
			memberId := c.GetInt64("memberId")
			pr, isMember, _, err := FindProjectByMemberId(c.Request.Context(), memberId, projectCode, taskCode)
			if err != nil {
				code, msg := errs.ParseGrpcError(err)
				c.JSON(http.StatusOK, result.Fail(code, msg))
				c.Abort()
				return
			}
			if !isMember {
				c.JSON(http.StatusOK, result.Fail(403, "不是项目成员，无操作权限"))
				c.Abort()
				return
			}
			if pr == nil {
				c.JSON(http.StatusOK, result.Fail(403, "不是项目成员，无操作权限"))
				c.Abort()
				return
			}
		}
	}
}

func firstCode(values ...string) string {
	for _, raw := range values {
		for _, part := range strings.Split(raw, ",") {
			if v := strings.TrimSpace(part); v != "" {
				return v
			}
		}
	}
	return ""
}

func jsonScalar(body map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		v, ok := body[k]
		if !ok || v == nil {
			for actual, candidate := range body {
				if candidate == nil || !strings.EqualFold(actual, k) {
					continue
				}
				v, ok = candidate, true
				break
			}
		}
		if !ok || v == nil {
			continue
		}
		switch typed := v.(type) {
		case string:
			if typed != "" {
				return typed
			}
		case float64:
			return strconv.FormatInt(int64(typed), 10)
		}
	}
	return ""
}

func FindProjectByMemberId(ctx context.Context, memberId int64, projectCode string, taskCode string) (*pro.Project, bool, bool, *errs.MError) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	msg := &project.ProjectRpcMessage{
		MemberId:    memberId,
		ProjectCode: projectCode,
		TaskCode:    taskCode,
	}
	projectResponse, err := rpc.ProjectServiceClient.FindProjectByMemberId(c, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		return nil, false, false, errs.NewError(errs.ErrorCode(code), msg)
	}
	if projectResponse == nil || projectResponse.Project == nil {
		return nil, false, false, nil
	}
	pr := &pro.Project{}
	copier.Copy(pr, projectResponse.Project)

	return pr, true, projectResponse.IsOwner, nil
}
