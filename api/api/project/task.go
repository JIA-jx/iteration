package project

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"test.com/api/api/rpc"
	"test.com/api/config"
	"test.com/api/pkg/model"
	"test.com/api/pkg/model/pro"
	"test.com/api/pkg/model/tasks"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/others/min"
	"test.com/others/tms"
	"test.com/rpcOrgrpc/task"
	"time"
)

var minioClient = &min.MinioClient{}

func minioSetting() *config.MinioConfig {
	if config.C == nil || config.C.MC == nil {
		return &config.MinioConfig{Bucket: "iteration"}
	}
	return config.C.MC
}

func minioObjectURL(key string) string {
	mc := minioSetting()
	scheme := "http"
	if mc.Secure {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s/%s/%s", scheme, mc.Endpoint, mc.Bucket, key)
}

func initMinioClient() error {
	if minioClient.C != nil {
		return nil
	}

	mc := minioSetting()
	if mc.Endpoint == "" || mc.AccessKey == "" || mc.SecretKey == "" {
		return errs.NewError(500, "对象存储未配置（minio.endpoint/accessKey/secretKey）")
	}

	c, err := minio.New(
		mc.Endpoint,
		&minio.Options{
			Creds:  credentials.NewStaticV4(mc.AccessKey, mc.SecretKey, ""),
			Secure: mc.Secure,
		})
	if err != nil {
		return err
	}
	minioClient.C = c
	return nil
}

type HandlerRequirement struct {
}

func (t *HandlerRequirement) taskStages(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	projectCode := c.PostForm("projectCode")
	page := &model.Page{}
	page.Bind(c)

	msg := &task.TaskReqMessage{
		MemberId:    c.GetInt64("memberId"),
		ProjectCode: projectCode,
		Page:        page.Page,
		PageSize:    page.PageSize,
	}
	stages, err := rpc.TaskServiceClient.TaskStages(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var list []*tasks.TaskStagesResp
	copier.Copy(&list, stages.List)
	if list == nil {
		list = []*tasks.TaskStagesResp{}
	}
	for _, v := range list {
		v.TasksLoading = true
		v.FixedCreator = false
		v.ShowTaskCard = false
		v.Tasks = []int{}
		v.DoneTasks = []int{}
		v.UnDoneTasks = []int{}
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  list,
		"total": stages.Total,
		"page":  page.Page,
	}))
}

func (t *HandlerRequirement) memberProjectList(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	projectCode := c.PostForm("projectCode")
	page := &model.Page{}
	page.Bind(c)

	msg := &task.TaskReqMessage{
		MemberId:    c.GetInt64("memberId"),
		ProjectCode: projectCode,
		Page:        page.Page,
		PageSize:    page.PageSize,
	}
	resp, err := rpc.TaskServiceClient.MemberProjectList(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var list []*pro.MemberProjectResp
	copier.Copy(&list, resp.List)
	if list == nil {
		list = []*pro.MemberProjectResp{}
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  list,
		"total": resp.Total,
		"page":  page.Page,
	}))

}

func (t *HandlerRequirement) taskList(c *gin.Context) {
	result := &common.Result{}
	stageCode := c.PostForm("stageCode")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	list, err := rpc.TaskServiceClient.TaskList(ctx, &task.TaskReqMessage{StageCode: stageCode, MemberId: c.GetInt64("memberId")})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	var taskDisplayList []*tasks.TaskDisplay
	copier.Copy(&taskDisplayList, list.List)
	if taskDisplayList == nil {
		taskDisplayList = []*tasks.TaskDisplay{}
	}

	for _, v := range taskDisplayList {
		if v.Tags == nil {
			v.Tags = []int{}
		}
		if v.ChildCount == nil {
			v.ChildCount = []int{}
		}
	}
	c.JSON(http.StatusOK, result.Success(taskDisplayList))
}

func (t *HandlerRequirement) saveTask(c *gin.Context) {
	result := &common.Result{}
	var req *tasks.TaskSaveReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "参数绑定失败"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &task.TaskReqMessage{
		ProjectCode: req.ProjectCode,
		Name:        req.Name,
		StageCode:   req.StageCode,
		AssignTo:    req.AssignTo,
		MemberId:    c.GetInt64("memberId"),
	}
	taskMessage, err := rpc.TaskServiceClient.SaveTask(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	td := &tasks.TaskDisplay{}
	copier.Copy(td, taskMessage)
	if td != nil {
		if td.Tags == nil {
			td.Tags = []int{}
		}
		if td.ChildCount == nil {
			td.ChildCount = []int{}
		}
	}
	c.JSON(http.StatusOK, result.Success(td))
}

func (t *HandlerRequirement) taskSort(c *gin.Context) {
	result := &common.Result{}
	var req tasks.TaskSortReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "参数格式有误"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &task.TaskReqMessage{
		PreTaskCode:  req.PreTaskCode,
		NextTaskCode: req.NextTaskCode,
		ToStageCode:  req.ToStageCode,
	}
	_, err := rpc.TaskServiceClient.TaskSort(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success([]int{}))
}

func (t *HandlerRequirement) myTaskList(c *gin.Context) {
	result := &common.Result{}
	var req tasks.MyTaskReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "参数格式有误"))
		return
	}
	memberId := c.GetInt64("memberId")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	msg := &task.TaskReqMessage{
		MemberId: memberId,
		TaskType: int32(req.TaskType),
		Type:     int32(req.Type),
		Page:     req.Page,
		PageSize: req.PageSize,
	}
	myTaskListResponse, err := rpc.TaskServiceClient.MyTaskList(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	var myTaskList []*tasks.MyTaskDisplay
	copier.Copy(&myTaskList, myTaskListResponse.List)
	if myTaskList == nil {
		myTaskList = []*tasks.MyTaskDisplay{}
	}
	for _, v := range myTaskList {
		v.ProjectInfo = tasks.ProjectInfo{
			Name: v.ProjectName,
			Code: v.ProjectCode,
		}
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  myTaskList,
		"total": myTaskListResponse.Total,
	}))
}

func (t *HandlerRequirement) readTask(c *gin.Context) {
	result := &common.Result{}
	taskCode := c.PostForm("taskCode")

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &task.TaskReqMessage{
		TaskCode: taskCode,
		MemberId: c.GetInt64("memberId"),
	}
	taskMessage, err := rpc.TaskServiceClient.ReadTask(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	td := &tasks.TaskDisplay{}
	copier.Copy(td, taskMessage)
	if td != nil {
		if td.Tags == nil {
			td.Tags = []int{}
		}
		if td.ChildCount == nil {
			td.ChildCount = []int{}
		}
	}
	c.JSON(200, result.Success(td))
}

func (t *HandlerRequirement) listTaskMember(c *gin.Context) {
	result := &common.Result{}
	taskCode := c.PostForm("taskCode")
	page := &model.Page{}
	page.Bind(c)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &task.TaskReqMessage{
		TaskCode: taskCode,
		MemberId: c.GetInt64("memberId"),
		Page:     page.Page,
		PageSize: page.PageSize,
	}
	taskMemberResponse, err := rpc.TaskServiceClient.ListTaskMember(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}
	var tms []*tasks.TaskMember
	copier.Copy(&tms, taskMemberResponse.List)
	if tms == nil {
		tms = []*tasks.TaskMember{}
	}
	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  tms,
		"total": taskMemberResponse.Total,
		"page":  page.Page,
	}))
}

func (t *HandlerRequirement) taskLog(c *gin.Context) {
	result := &common.Result{}
	var req model.TaskLogReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "参数格式有误"))
		return
	}
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &task.TaskReqMessage{
		TaskCode: req.TaskCode,
		MemberId: c.GetInt64("memberId"),
		Page:     int64(req.Page),
		PageSize: int64(req.PageSize),
		All:      int32(req.All),
		Comment:  int32(req.Comment),
	}

	taskLogResponse, err := rpc.TaskServiceClient.TaskLog(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var tms []*model.ProjectLogDisplay
	copier.Copy(&tms, taskLogResponse.List)
	if tms == nil {
		tms = []*model.ProjectLogDisplay{}
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"list":  tms,
		"total": taskLogResponse.Total,
		"page":  req.Page,
	}))
}

func (t *HandlerRequirement) taskWorkTimeList(c *gin.Context) {
	taskCode := c.PostForm("taskCode")
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &task.TaskReqMessage{
		TaskCode: taskCode,
		MemberId: c.GetInt64("memberId"),
	}

	taskWorkTimeResponse, err := rpc.TaskServiceClient.TaskWorkTimeList(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var tms []*model.TaskWorkTime
	copier.Copy(&tms, taskWorkTimeResponse.List)
	if tms == nil {
		tms = []*model.TaskWorkTime{}
	}

	c.JSON(http.StatusOK, result.Success(tms))
}

func (t *HandlerRequirement) saveTaskWorkTime(c *gin.Context) {
	result := &common.Result{}
	var req model.SaveTaskWorkTimeReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "参数格式有误"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &task.TaskReqMessage{
		TaskCode:  req.TaskCode,
		MemberId:  c.GetInt64("memberId"),
		Content:   req.Content,
		Num:       int32(req.Num),
		BeginTime: tms.ParseTime(req.BeginTime),
	}

	_, err := rpc.TaskServiceClient.SaveTaskWorkTime(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success([]int{}))
}

const maxUploadChunkSize = 64 << 20

func (t *HandlerRequirement) uploadFiles(c *gin.Context) {
	result := &common.Result{}
	req := model.UploadFileReq{}
	c.ShouldBind(&req)

	multipartForm, mfErr := c.MultipartForm()
	if mfErr != nil || multipartForm == nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "请求必须是 multipart/form-data"))
		return
	}
	file := multipartForm.File
	if len(file["file"]) == 0 {
		c.JSON(http.StatusOK, result.Fail(-1, "文件不存在"))
		return
	}
	uploadFile := file["file"][0]

	if req.CurrentChunkSize <= 0 || req.CurrentChunkSize > maxUploadChunkSize {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "currentChunkSize 超出允许范围"))
		return
	}

	if err := initMinioClient(); err != nil {
		c.JSON(http.StatusOK, result.Fail(-999, "MinIO连接失败："+err.Error()))
		return
	}

	fileKey := fmt.Sprintf("u%d/%s", c.GetInt64("memberId"), path.Base(strings.TrimSpace(req.Filename)))
	if req.Filename == "" || path.Base(req.Filename) == "." || path.Base(req.Filename) == "/" {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "filename 无效"))
		return
	}
	if req.TotalChunks <= 0 || req.ChunkNumber <= 0 || req.ChunkNumber > req.TotalChunks {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "chunkNumber 必须在 1..totalChunks 之间"))
		return
	}
	ctxTimeout, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	fileReader, err := uploadFile.Open()
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(-999, err.Error()))
		return
	}
	defer fileReader.Close()

	buf := make([]byte, req.CurrentChunkSize)
	n, readErr := io.ReadFull(fileReader, buf)
	if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
		c.JSON(http.StatusOK, result.Fail(-999, "读取文件失败"))
		return
	}
	if n <= 0 {
		c.JSON(http.StatusOK, result.Fail(-1, "文件内容为空"))
		return
	}
	buf = buf[:n]

	chunkName := fileKey + "_" + strconv.Itoa(req.ChunkNumber)
	_, err = minioClient.Put(
		ctxTimeout,
		minioSetting().Bucket,
		chunkName,
		buf,
		int64(n),
		uploadFile.Header.Get("Content-Type"),
	)
	if err != nil {
		zap.L().Error("分片上传失败", zap.Error(err))
		c.JSON(http.StatusOK, result.Fail(-999, "分片上传失败："+err.Error()))
		return
	}

	if req.ChunkNumber < req.TotalChunks {
		c.JSON(http.StatusOK, result.Success(gin.H{
			"chunkNumber": req.ChunkNumber,
			"msg":         "分片上传成功",
		}))
		return
	}

	if req.TotalChunks == req.ChunkNumber {
		_, err = minioClient.Compose(ctxTimeout, minioSetting().Bucket, fileKey, req.TotalChunks)
		if err != nil {
			zap.L().Error("文件合并失败", zap.Error(err))
			c.JSON(http.StatusOK, result.Fail(-999, "文件合并失败："+err.Error()))
			return
		}
	}

	key := minioSetting().Bucket + "/" + fileKey
	fileUrl := minioObjectURL(key)

	ctxGrpc, cancel2 := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel2()

	msg := &task.TaskFileReqMessage{
		TaskCode:         req.TaskCode,
		ProjectCode:      req.ProjectCode,
		OrganizationCode: OrganizationCode(c),
		PathName:         key,
		FileName:         req.Filename,
		Size:             int64(req.TotalSize),
		Extension:        path.Ext(key),
		FileUrl:          fileUrl,
		FileType:         uploadFile.Header.Get("Content-Type"),
		MemberId:         c.GetInt64("memberId"),
	}

	_, err = rpc.TaskServiceClient.SaveTaskFile(ctxGrpc, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(gin.H{
		"file": key,
		"hash": "",
		"key":  key,
		"url":  fileUrl,
	}))
}

func (t *HandlerRequirement) taskSources(c *gin.Context) {
	result := &common.Result{}
	taskCode := c.PostForm("taskCode")

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	sources, err := rpc.TaskServiceClient.TaskSources(ctx, &task.TaskReqMessage{TaskCode: taskCode})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	var slList []*model.SourceLink
	copier.Copy(&slList, sources.List)
	if slList == nil {
		slList = []*model.SourceLink{}
	}

	c.JSON(http.StatusOK, result.Success(slList))
}

func (t *HandlerRequirement) createComment(c *gin.Context) {
	result := &common.Result{}
	req := model.CommentReq{}
	c.ShouldBind(&req)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &task.TaskReqMessage{
		TaskCode:       req.TaskCode,
		CommentContent: req.Comment,
		Mentions:       req.Mentions,
		MemberId:       c.GetInt64("memberId"),
	}

	_, err := rpc.TaskServiceClient.CreateComment(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(true))
}

func NewRequirement() *HandlerRequirement {
	return &HandlerRequirement{}
}
func (t *HandlerRequirement) editTask(c *gin.Context) {
	result := &common.Result{}
	taskCode := c.PostForm("taskCode")
	if taskCode == "" {
		taskCode = c.PostForm("task_code")
	}
	if taskCode == "" {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "taskCode 不能为空"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	msg := &task.TaskReqMessage{
		TaskCode:    taskCode,
		MemberId:    c.GetInt64("memberId"),
		Name:        c.PostForm("name"),
		Description: c.PostForm("description"),
		TaskTag:     c.PostForm("taskTag"),
		AssignTo:    c.PostForm("assignTo"),
		StageCode:   c.PostForm("stageCode"),
	}
	if pri, err := strconv.Atoi(c.PostForm("pri")); err == nil {
		msg.Pri = int32(pri)
	}
	if schedule, err := strconv.ParseInt(c.PostForm("schedule"), 10, 64); err == nil {
		msg.Schedule = schedule
	}
	msg.EndTime = tms.ParseTime(c.PostForm("endTime"))

	resp, err := rpc.TaskServiceClient.TaskEdit(ctx, msg)
	if err != nil {
		code, m := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, m))
		return
	}
	c.JSON(http.StatusOK, result.Success(resp))
}

func (t *HandlerRequirement) taskDone(c *gin.Context) {
	result := &common.Result{}
	taskCode := c.PostForm("taskCode")
	if taskCode == "" {
		taskCode = c.PostForm("task_code")
	}
	if taskCode == "" {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "taskCode 不能为空"))
		return
	}

	done, err := strconv.Atoi(c.DefaultPostForm("done", "1"))
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "done 无效"))
		return
	}
	executeStatus, _ := strconv.Atoi(c.DefaultPostForm("executeStatus", c.DefaultPostForm("execute_status", "0")))

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	resp, err := rpc.TaskServiceClient.TaskDone(ctx, &task.TaskReqMessage{
		TaskCode:      taskCode,
		MemberId:      c.GetInt64("memberId"),
		Done:          int32(done),
		ExecuteStatus: int32(executeStatus),
	})
	if err != nil {
		code, m := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, m))
		return
	}
	c.JSON(http.StatusOK, result.Success(resp))
}

func (t *HandlerRequirement) taskDelete(c *gin.Context) {
	result := &common.Result{}
	taskCode := c.PostForm("taskCode")
	if taskCode == "" {
		taskCode = c.PostForm("task_code")
	}
	if taskCode == "" {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "taskCode 不能为空"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	resp, err := rpc.TaskServiceClient.TaskDelete(ctx, &task.TaskReqMessage{
		TaskCode: taskCode,
		MemberId: c.GetInt64("memberId"),
	})
	if err != nil {
		code, m := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, m))
		return
	}
	c.JSON(http.StatusOK, result.Success(resp))
}

func (t *HandlerRequirement) taskStagesList(c *gin.Context) {
	result := &common.Result{}
	req := model.Page{}
	req.Bind(c)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	stageCode := c.PostForm("stageCode")
	if stageCode == "" {
		stageCode = c.PostForm("stage_code")
	}

	resp, err := rpc.TaskServiceClient.TaskStagesList(ctx, &task.TaskReqMessage{
		MemberId:    c.GetInt64("memberId"),
		ProjectCode: c.PostForm("projectCode"),
		StageCode:   stageCode,
		Page:        req.Page,
		PageSize:    req.PageSize,
	})
	if err != nil {
		code, m := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, m))
		return
	}
	c.JSON(http.StatusOK, result.Success(resp))
}
func (t *HandlerRequirement) saveTaskSource(c *gin.Context) {
	result := &common.Result{}
	taskCode := c.PostForm("taskCode")
	if taskCode == "" {
		taskCode = c.PostForm("task_code")
	}
	if taskCode == "" {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "taskCode 不能为空"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	resp, err := rpc.TaskServiceClient.SaveTaskSource(ctx, &task.TaskSourceReqMessage{
		MemberId:         c.GetInt64("memberId"),
		TaskCode:         taskCode,
		SourceType:       c.DefaultPostForm("sourceType", c.DefaultPostForm("source_type", "task")),
		LinkType:         c.DefaultPostForm("linkType", c.DefaultPostForm("link_type", "file")),
		LinkCode:         c.PostForm("linkCode"),
		OrganizationCode: OrganizationCode(c),
	})
	if err != nil {
		code, m := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, m))
		return
	}
	c.JSON(http.StatusOK, result.Success(resp))
}

func (t *HandlerRequirement) deleteTaskSource(c *gin.Context) {
	result := &common.Result{}
	sourceId, err := strconv.ParseInt(c.DefaultPostForm("sourceId", c.PostForm("source_id")), 10, 64)
	if err != nil || sourceId <= 0 {
		c.JSON(http.StatusOK, result.Fail(http.StatusBadRequest, "sourceId 无效"))
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	resp, err := rpc.TaskServiceClient.DeleteTaskSource(ctx, &task.TaskSourceReqMessage{
		MemberId: c.GetInt64("memberId"),
		TaskCode: c.PostForm("taskCode"),
		SourceId: sourceId,
	})
	if err != nil {
		code, m := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, m))
		return
	}
	c.JSON(http.StatusOK, result.Success(resp))
}
