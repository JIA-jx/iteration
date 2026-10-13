package project

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"net/http"
	"strings"
	"sync"
	common "test.com/others"
	"test.com/others/errs"
	"time"
)

var ignores = []string{
	"iteration/login/register",
	"iteration/login",
	"iteration/login/getCaptcha",
	"iteration/organization/_getOrgList",
}

var routeNodeRules = map[string]string{
	"/iteration/selfList":                           "project/project/selflist",
	"/iteration/task/selfList":                      "project/task/selflist",
	"/iteration/recycle/list":                       "project/project/recyclelist",
	"/iteration/template":                           "project/project_template/index",
	"/iteration/template/uploadCover":               "project/project_template/uploadcover",
	"/iteration/template/save":                      "project/project_template/save",
	"/iteration/template/delete":                    "project/project_template/delete",
	"/iteration/account":                            "project/account/index",
	"/iteration/team":                               "project/department/index",
	"/iteration/team/save":                          "project/department/save",
	"/iteration/team/read":                          "project/department/read",
	"/iteration/auth":                               "project/auth/index",
	"/iteration/auth/apply":                         "project/auth/apply",
	"/iteration/menu":                               "project/menu/menu",
	"/iteration/file/upload":                        "project/file/uploadfiles",
	"/iteration/file/list":                          "project/file/index",
	"/iteration/file/download":                      "project/file/read",
	"/iteration/file/upload/init":                   "project/file/uploadfiles",
	"/iteration/file/upload/status":                 "project/file/uploadfiles",
	"/iteration/file/upload/chunk":                  "project/file/uploadfiles",
	"/iteration/file/upload/merge":                  "project/file/uploadfiles",
	"/iteration/analysis/ai/recommend":              "project/analysis/airecommend",
	"/iteration/index":                              "project/project/index",
	"/iteration/save":                               "project/project/save",
	"/iteration/read":                               "project/project/read",
	"/iteration/edit":                               "project/project/edit",
	"/iteration/recycle":                            "project/project/recycle",
	"/iteration/recovery":                           "project/project/recovery",
	"/iteration/collect":                            "project/project_collect/collect",
	"/iteration/getLogBySelfInteration":             "project/task_log/getlistbyselfproject",
	"/iteration/node":                               "project/node/index",
	"/iteration/requirement/stages":                 "project/task_stages/index",
	"/iteration/requirement/save":                   "project/task/save",
	"/iteration/requirement/sort":                   "project/task/sort",
	"/iteration/requirement/read":                   "project/task/read",
	"/iteration/requirement/member/index":           "project/task/selflist",
	"/iteration/requirement/task_stages/tasks":      "project/task_stages/tasks",
	"/iteration/requirement/member":                 "project/task_member/index",
	"/iteration/requirement/log":                    "project/task_log/index",
	"/iteration/requirement/workTimeList":           "project/task/worktimelist",
	"/iteration/requirement/saveWorkTime":           "project/task/savetaskworktime",
	"/iteration/requirement/file/uploadFiles":       "project/task/uploadfile",
	"/iteration/requirement/createComment":          "project/task/createcomment",
	"/iteration/requirement/sources":                "project/task/tasksources",
	"/iteration/requirement/sources/save":           "project/source_link",
	"/iteration/requirement/sources/delete":         "project/source_link/delete",
	"/iteration/requirement/edit":                   "project/task/edit",
	"/iteration/requirement/taskdone":               "project/task/taskdone",
	"/iteration/requirement/delete":                 "project/task/delete",
	"/iteration/stages/list":                        "project/task_stages/index",
	"/iteration/requirement/assign":                 "project/task/assigntask",
	"/iteration/requirement/assign/batch":           "project/task/batchassigntask",
	"/iteration/requirement/recycle":                "project/task/recycle",
	"/iteration/requirement/recycle/batch":          "project/task/recyclebatch",
	"/iteration/requirement/recovery":               "project/task/recovery",
	"/iteration/requirement/like":                   "project/task/like",
	"/iteration/requirement/star":                   "project/task/star",
	"/iteration/requirement/private":                "project/task/setprivate",
	"/iteration/requirement/tag":                    "project/task/settag",
	"/iteration/requirement/member/invite":          "project/task_member/invitemember",
	"/iteration/requirement/member/invite/batch":    "project/task_member/invitememberbatch",
	"/iteration/requirement/member/remove":          "project/task_member",
	"/iteration/requirement/member/search":          "project/task_member/searchinvitemember",
	"/iteration/requirement/stages/save":            "project/task_stages/save",
	"/iteration/requirement/stages/edit":            "project/task_stages/edit",
	"/iteration/requirement/stages/delete":          "project/task_stages/delete",
	"/iteration/requirement/stages/sort":            "project/task_stages/sort",
	"/iteration/requirement/stages/template":        "project/task_stages_template/index",
	"/iteration/requirement/stages/template/save":   "project/task_stages_template/save",
	"/iteration/requirement/stages/template/edit":   "project/task_stages_template/edit",
	"/iteration/requirement/stages/template/delete": "project/task_stages_template/delete",
	"/iteration/requirement/tag/index":              "project/task_tag/index",
	"/iteration/requirement/tag/save":               "project/task_tag/save",
	"/iteration/requirement/tag/edit":               "project/task_tag/edit",
	"/iteration/requirement/tag/delete":             "project/task_tag/delete",
	"/iteration/requirement/tag/tasks":              "project/task/getlistbytasktag",
	"/iteration/requirement/tag/tasktotags":         "project/task/tasktotags",
	"/iteration/requirement/workTime/edit":          "project/task/edittaskworktime",
	"/iteration/requirement/workTime/delete":        "project/task/deltaskworktime",
	"/iteration/requirement/workflow/index":         "project/task_workflow/index",
	"/iteration/requirement/workflow/save":          "project/task_workflow/save",
	"/iteration/requirement/workflow/edit":          "project/task_workflow/edit",
	"/iteration/requirement/workflow/delete":        "project/task_workflow/delete",
	"/iteration/analysis/interation/delivery":       "project/analysis/delivery",
	"/iteration/analysis/burndown":                  "project/analysis/burndown",
	"/iteration/analysis/team/load":                 "project/analysis/teamload",
	"/iteration/system/notify/list":                 "project/notify/index",
	"/iteration/system/log/query":                   "project/system/log",
	"/iteration/system/config/reload":               "project/system/config",
	"/iteration/system/cache/clear":                 "project/system/cache",
}

type nodeCacheEntry struct {
	nodes   []string
	expires time.Time
}

var (
	nodeCacheMu sync.RWMutex
	nodeCache   = make(map[int64]nodeCacheEntry)
)

const (
	nodeCacheTTL     = 3 * time.Second
	nodeCacheMaxSize = 20000
)

func PurgeNodeCache(memberId int64) {
	if memberId <= 0 {
		return
	}
	nodeCacheMu.Lock()
	delete(nodeCache, memberId)
	nodeCacheMu.Unlock()
}

func PurgeAllNodeCache() {
	nodeCacheMu.Lock()
	nodeCache = make(map[int64]nodeCacheEntry)
	nodeCacheMu.Unlock()
}

func requiredNode(c *gin.Context) string {
	path := c.FullPath()
	if path == "" && c.Request != nil && c.Request.URL != nil {
		path = c.Request.URL.Path
	}
	if path == "" {
		return ""
	}
	if node, ok := routeNodeRules[path]; ok {
		return node
	}
	return ""
}

func nodeGranted(nodes []string, required string) bool {
	if required == "" {
		return true
	}
	for _, n := range nodes {
		if n == required {
			return true
		}
	}

	parts := strings.Split(required, "/")
	for i := len(parts) - 1; i > 0; i-- {
		ancestor := strings.Join(parts[:i], "/")
		for _, n := range nodes {
			if n == ancestor {
				return true
			}
		}
	}
	return false
}

func authNodesCached(c *gin.Context, memberId int64) ([]string, error) {
	nodeCacheMu.RLock()
	entry, ok := nodeCache[memberId]
	nodeCacheMu.RUnlock()
	if ok && time.Now().Before(entry.expires) {
		return entry.nodes, nil
	}

	nodes, err := NewAuth().GetAuthNodes(c)
	if err != nil {
		return nil, err
	}

	nodeCacheMu.Lock()
	if len(nodeCache) >= nodeCacheMaxSize {
		for k, v := range nodeCache {
			if time.Now().After(v.expires) {
				delete(nodeCache, k)
			}
		}
		if len(nodeCache) >= nodeCacheMaxSize {
			nodeCache = make(map[int64]nodeCacheEntry)
		}
	}
	nodeCache[memberId] = nodeCacheEntry{nodes: nodes, expires: time.Now().Add(nodeCacheTTL)}
	nodeCacheMu.Unlock()

	return nodes, nil
}

func CallerHasNode(c *gin.Context, node string) bool {
	memberId := c.GetInt64("memberId")
	if memberId <= 0 {
		return false
	}
	nodes, err := authNodesCached(c, memberId)
	if err != nil {
		return false
	}
	return nodeGranted(nodes, node)
}

func Auth() func(*gin.Context) {
	return func(c *gin.Context) {
		result := &common.Result{}
		uri := ""
		if c.Request != nil && c.Request.URL != nil {
			uri = c.Request.URL.Path
		}

		for _, v := range ignores {
			if strings.Contains(uri, v) {
				c.Next()
				return
			}
		}

		memberId := c.GetInt64("memberId")
		if memberId <= 0 {
			c.JSON(http.StatusOK, result.Fail(401, "请先登录"))
			c.Abort()
			return
		}

		required := requiredNode(c)
		if required == "" {
			zap.L().Error("permission rule missing for route",
				zap.String("path", c.FullPath()),
				zap.String("uri", uri))
			c.JSON(http.StatusOK, result.Fail(403, "接口未配置权限节点，已拒绝"))
			c.Abort()
			return
		}

		nodes, err := authNodesCached(c, memberId)
		if err != nil {
			code, msg := errs.ParseGrpcError(err)
			c.JSON(http.StatusOK, result.Fail(code, msg))
			c.Abort()
			return
		}

		if nodeGranted(nodes, required) {
			c.Next()
			return
		}

		c.JSON(http.StatusOK, result.Fail(403, "无权限操作（需要权限节点 "+required+"）"))
		c.Abort()
	}
}
