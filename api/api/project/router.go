package project

import (
	"github.com/gin-gonic/gin"
	"log"
	"test.com/api/api/rpc"

	"test.com/api/api/midd"
	"test.com/api/api/midd/ratelimit"
	"test.com/api/router"
)

type RouterIteration struct {
}

func init() {
	log.Println("init project router")
	ru := &RouterIteration{}
	router.Register(ru)
}

func (*RouterIteration) Route(r *gin.Engine) {
	rpc.InitRpcIterationClient()
	h := New()
	t := NewRequirement()
	a := NewAccount()
	d := NewTeam()
	als := NewAnalysis()
	f := NewFile()
	s := NewSystem()
	auth := NewAuth()
	menu := NewMenu()

	g1 := r.Group("/iteration")
	g1.Use(midd.JWT_Midd())
	g1.Use(ratelimit.Middleware())
	g1.Use(Auth())

	g1.POST("/selfList", h.myProjectList)
	g1.POST("/recycle/list", h.recycleList)

	g1.POST("/template", h.projectTemplate)
	g1.POST("/template/uploadCover", h.uploadCover)
	g1.POST("/template/save", h.saveTemplate)
	g1.POST("/template/delete", h.deleteTemplate)

	g1.POST("/account", a.account)
	g1.POST("/team", d.department)
	g1.POST("/team/save", d.save)
	g1.POST("/team/read", d.read)

	g1.POST("/auth", auth.authList)
	g1.POST("/auth/apply", auth.apply)
	g1.POST("/menu", menu.menuList)

	g1.POST("/file/upload", f.upload)
	g1.POST("/file/list", f.list)
	g1.POST("/file/download", f.download)
	g1.POST("/file/upload/init", f.uploadInit)
	g1.POST("/file/upload/status", f.uploadStatus)
	g1.POST("/file/upload/chunk", f.uploadChunk)
	g1.POST("/file/upload/merge", f.uploadMerge)

	g1.POST("/analysis/ai/recommend", als.aiRecommend)

	g2 := r.Group("/iteration")
	g2.Use(midd.JWT_Midd())
	g2.Use(ratelimit.Middleware())
	g2.Use(Auth())
	g2.Use(InterationAuth())

	g2.POST("/index", h.index)
	g2.POST("/save", h.projectSave)
	g2.POST("/read", h.readProject)
	g2.POST("/edit", h.editProject)
	g2.POST("/recycle", h.recycleProject)
	g2.POST("/recovery", h.recoveryProject)
	g2.POST("/collect", h.collectProject)
	g2.POST("/getLogBySelfInteration", h.getLogBySelfProject)
	g2.POST("/node", h.nodeList)

	g2.POST("/requirement/stages", t.taskStages)
	g2.POST("/requirement/save", t.saveTask)
	g2.POST("/requirement/sort", t.taskSort)
	g2.POST("/requirement/read", t.readTask)
	g2.POST("/requirement/member/index", t.memberProjectList)
	g2.POST("/task/selfList", t.myTaskList)
	g2.POST("/requirement/task_stages/tasks", t.taskList)
	g2.POST("/requirement/member", t.listTaskMember)
	g2.POST("/requirement/log", t.taskLog)
	g2.POST("/requirement/workTimeList", t.taskWorkTimeList)
	g2.POST("/requirement/saveWorkTime", t.saveTaskWorkTime)
	g2.POST("/requirement/file/uploadFiles", t.uploadFiles)
	g2.POST("/requirement/createComment", t.createComment)
	g2.POST("/requirement/sources", t.taskSources)
	g2.POST("/requirement/sources/save", t.saveTaskSource)
	g2.POST("/requirement/sources/delete", t.deleteTaskSource)
	g2.POST("/requirement/edit", t.editTask)
	g2.POST("/requirement/taskdone", t.taskDone)
	g2.POST("/requirement/delete", t.taskDelete)
	g2.POST("/stages/list", t.taskStagesList)
	g2.POST("/requirement/assign", t.batchAction("assign"))
	g2.POST("/requirement/assign/batch", t.batchAction("batch_assign"))
	g2.POST("/requirement/recycle", t.batchAction("recycle"))
	g2.POST("/requirement/recycle/batch", t.batchAction("recycle_batch"))
	g2.POST("/requirement/recovery", t.batchAction("recovery"))
	g2.POST("/requirement/like", t.batchAction("like"))
	g2.POST("/requirement/star", t.batchAction("star"))
	g2.POST("/requirement/private", t.batchAction("set_private"))
	g2.POST("/requirement/tag", t.batchAction("set_tag"))
	g2.POST("/requirement/member/invite", t.memberAction("invite"))
	g2.POST("/requirement/member/invite/batch", t.memberAction("invite_batch"))
	g2.POST("/requirement/member/remove", t.memberAction("remove"))
	g2.POST("/requirement/member/search", t.memberAction("search"))
	g2.POST("/requirement/stages/save", t.stagesAction("save"))
	g2.POST("/requirement/stages/edit", t.stagesAction("edit"))
	g2.POST("/requirement/stages/delete", t.stagesAction("delete"))
	g2.POST("/requirement/stages/sort", t.stagesAction("sort"))
	g2.POST("/requirement/stages/template", t.templateAction("index"))
	g2.POST("/requirement/stages/template/save", t.templateAction("save"))
	g2.POST("/requirement/stages/template/edit", t.templateAction("edit"))
	g2.POST("/requirement/stages/template/delete", t.templateAction("delete"))
	g2.POST("/requirement/tag/index", t.tagAction("index"))
	g2.POST("/requirement/tag/save", t.tagAction("save"))
	g2.POST("/requirement/tag/edit", t.tagAction("edit"))
	g2.POST("/requirement/tag/delete", t.tagAction("delete"))
	g2.POST("/requirement/tag/tasks", t.tagAction("tasks"))
	g2.POST("/requirement/tag/tasktotags", t.tagAction("tasks"))
	g2.POST("/requirement/workTime/edit", t.workTimeAction("edit"))
	g2.POST("/requirement/workTime/delete", t.workTimeAction("delete"))
	g2.POST("/requirement/workflow/index", t.workTimeAction("workflow_index"))
	g2.POST("/requirement/workflow/save", t.workTimeAction("workflow_save"))
	g2.POST("/requirement/workflow/edit", t.workTimeAction("workflow_edit"))
	g2.POST("/requirement/workflow/delete", t.workTimeAction("workflow_delete"))

	g2.POST("/analysis/interation/delivery", als.delivery)
	g2.POST("/analysis/burndown", als.burndown)
	g2.POST("/analysis/team/load", als.teamLoad)

	g3 := r.Group("/iteration")
	g3.Use(midd.JWT_Midd())

	g3.Use(ratelimit.Middleware())
	g3.Use(Auth())

	g3.POST("/system/notify/list", s.list)
	g3.POST("/system/log/query", s.logQuery)
	g3.POST("/system/config/reload", s.reload)
	g3.POST("/system/cache/clear", s.clearCache)
}
