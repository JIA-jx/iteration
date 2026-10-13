package task_service_v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"test.com/toProject/config/kafka"
	"time"

	"github.com/go-redsync/redsync/v4"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
	"test.com/others/elk"
	"test.com/others/encrypts"
	"test.com/others/errs"
	"test.com/others/idem"
	"test.com/others/kk"
	"test.com/others/outbox"
	"test.com/others/tenant"
	"test.com/others/tms"
	"test.com/rpcOrgrpc/task"
	"test.com/rpcOrgrpc/user/login"
	"test.com/toProject/config"
	"test.com/toProject/internal/cachekv"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/database/gorms"
	"test.com/toProject/internal/database/tran"
	"test.com/toProject/internal/domain"
	"test.com/toProject/internal/interceptor"
	"test.com/toProject/internal/repo"
	"test.com/toProject/internal/rpc"
	"test.com/toProject/pkg/model"
	delay_task_v1 "test.com/toProject/pkg/service/delay_task.v1"
)

const taskCacheGroup = "task"

var taskCachePatterns = []string{
	"task:list:*",
	"task:stages:*",
	"task:detail:*",
	"task:log:*",
	"task:member:list:*",
	"task:worktime:list:*",
}

var lruCache, _ = lru.NewARC(1000)
var workTimeLru, _ = lru.NewARC(1000)

type TaskService struct {
	task.UnimplementedTaskServiceServer
	cache                  repo.Cache
	transaction            tran.Transaction
	projectRepo            repo.ProjectRepo
	projectTemplateRepo    repo.ProjectTemplateRepo
	taskStagesTemplateRepo repo.TaskStagesTemplateRepo
	taskStagesRepo         repo.TaskStagesRepo
	taskRepo               repo.TaskRepo
	projectLogRepo         repo.ProjectLogRepo
	taskWorkTimeRepo       repo.TaskWorkTimeRepo
	fileRepo               repo.FileRepo
	sourceLinkRepo         repo.SourceLinkRepo
	taskWorkTimeDomain     *domain.TaskWorkTimeDomain
	kafka                  *kk.KafkaWriter
	outboxRepo             outbox.Repo
	delayService           *delay_task_v1.DelayTaskService
}

var delaySvc *delay_task_v1.DelayTaskService

func SetDelayService(s *delay_task_v1.DelayTaskService) {
	delaySvc = s
}

func GetDelayService() *delay_task_v1.DelayTaskService {
	return delaySvc
}

func New() *TaskService {
	return &TaskService{
		cache:                  dao.Rc,
		transaction:            dao.NewTransaction(),
		projectRepo:            dao.NewProjectDao(),
		projectTemplateRepo:    dao.NewProjectTemplateDao(),
		taskStagesTemplateRepo: dao.NewTaskStagesTemplateDao(),
		taskStagesRepo:         dao.NewTaskStagesDao(),
		taskRepo:               dao.NewTaskDao(),
		projectLogRepo:         dao.NewProjectLogDao(),
		taskWorkTimeRepo:       dao.NewTaskWorkTimeDao(),
		fileRepo:               dao.NewFileDao(),
		sourceLinkRepo:         dao.NewSourceLinkDao(),
		taskWorkTimeDomain:     domain.NewTaskWorkTimeDomain(),
		kafka:                  kk.GetWriter(config.C.KafkaConfig.Brokers[0]),
		outboxRepo:             outbox.NewGormRepo(gorms.GetDB()),
		delayService:           delaySvc,
	}
}

func (t *TaskService) TaskStages(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskStagesResponse, error) {
	projectCode := encrypts.DecryptNoErr(msg.ProjectCode)
	page := msg.Page
	pageSize := msg.PageSize

	key := fmt.Sprintf("task:stages:%d:%d:%d", projectCode, page, pageSize)
	if v, ok := lruCache.Get(key); ok {
		return v.(*task.TaskStagesResponse), nil
	}

	val, err := dao.Rc.Get(ctx, key)
	if err == nil {
		var resp task.TaskStagesResponse
		_ = json.Unmarshal([]byte(val), &resp)
		lruCache.Add(key, &resp)
		return &resp, nil
	}

	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	stages, total, err := t.taskStagesRepo.FindStagesByProjectId(c, projectCode, page, pageSize)
	if err != nil {
		zap.L().Error("project SaveProject taskStagesRepo.FindStagesByProjectId error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	var tsMessages []*task.TaskStagesMessage
	copier.Copy(&tsMessages, stages)
	if tsMessages == nil {
		return &task.TaskStagesResponse{List: tsMessages, Total: 0}, nil
	}

	stagesMap := data.ToTaskStagesMap(stages)
	for _, v := range tsMessages {
		taskStages := stagesMap[int(v.Id)]
		v.Code = encrypts.EncryptNoErr(int64(v.Id))
		v.CreateTime = tms.FormatByMilli(taskStages.CreateTime)
		v.ProjectCode = msg.ProjectCode
	}

	resp := &task.TaskStagesResponse{
		List:  tsMessages,
		Total: total,
	}

	bs, _ := json.Marshal(resp)
	_ = dao.Rc.Put(ctx, key, string(bs), 1*time.Minute)
	lruCache.Add(key, resp)

	return resp, nil
}

func (t *TaskService) MemberProjectList(c context.Context, msg *task.TaskReqMessage) (*task.MemberProjectResponse, error) {
	ctx, cancel := context.WithTimeout(c, 2*time.Second)
	defer cancel()

	projectCode := encrypts.DecryptNoErr(msg.ProjectCode)
	key := fmt.Sprintf("project:member:list:%d", projectCode)

	if val, ok := lruCache.Get(key); ok {
		return val.(*task.MemberProjectResponse), nil
	}

	var resp *task.MemberProjectResponse
	val, err := dao.Rc.Get(ctx, key)
	if err == nil && val != "" {
		if json.Unmarshal([]byte(val), &resp) == nil {
			lruCache.Add(key, resp)
			return resp, nil
		}
	}

	lockKey := fmt.Sprintf("lock:project:member:%d", projectCode)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(3*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("缓存击穿-获取锁失败", zap.Int64("project", projectCode))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	if val, _ := dao.Rc.Get(ctx, key); val != "" {
		json.Unmarshal([]byte(val), &resp)
		lruCache.Add(key, resp)
		return resp, nil
	}

	projectMembers, total, err := t.projectRepo.FindProjectMemberByPid(ctx, projectCode)
	if err != nil {
		zap.L().Error("FindProjectMemberByPid error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	if len(projectMembers) == 0 {
		empty := &task.MemberProjectResponse{List: nil, Total: 0}
		bs, _ := json.Marshal(empty)
		_ = dao.Rc.Put(ctx, key, bs, 5*time.Minute)
		lruCache.Add(key, empty)
		return empty, nil
	}

	var mIds []int64
	pmMap := make(map[int64]*data.ProjectMember)
	for _, v := range projectMembers {
		mIds = append(mIds, v.MemberCode)
		pmMap[v.MemberCode] = v
	}

	memberMessageList, err := rpc.LoginServiceClient.FindMemInfoByIds(ctx, &login.UserMessage{MIds: mIds})
	if err != nil {
		zap.L().Error("FindMemInfoByIds error", zap.Error(err))
		return nil, err
	}

	var list []*task.MemberProjectMessage
	for _, v := range memberMessageList.List {
		pm := pmMap[v.Id]
		mpm := &task.MemberProjectMessage{
			MemberCode: v.Id,
			Name:       v.Name,
			Avatar:     v.Avatar,
			Email:      v.Email,
			Code:       v.Code,
			IsOwner:    0,
		}
		if pm != nil && pm.IsOwner == v.Id {
			mpm.IsOwner = 1
		}
		list = append(list, mpm)
	}

	resp = &task.MemberProjectResponse{
		List:  list,
		Total: total,
	}

	bs, _ := json.Marshal(resp)
	_ = dao.Rc.Put(ctx, key, bs, 5*time.Minute)
	lruCache.Add(key, resp)

	return resp, nil
}

func (t *TaskService) TaskList(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskListResponse, error) {
	stageCode := encrypts.DecryptNoErr(msg.StageCode)
	memberId := msg.MemberId
	cacheKey := fmt.Sprintf("task:list:stage:%d:member:%d", stageCode, memberId)

	if val, ok := lruCache.Get(cacheKey); ok {
		return val.(*task.TaskListResponse), nil
	}

	var resp *task.TaskListResponse
	cacheVal, err := dao.Rc.Get(ctx, cacheKey)
	if err == nil && cacheVal != "" {
		if err := json.Unmarshal([]byte(cacheVal), &resp); err == nil {
			lruCache.Add(cacheKey, resp)
			return resp, nil
		}
	}

	lockKey := fmt.Sprintf("lock:task:list:%d", stageCode)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(5*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("任务列表缓存击穿，获取锁失败", zap.Int64("stage", stageCode))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	if doubleVal, _ := dao.Rc.Get(ctx, cacheKey); doubleVal != "" {
		_ = json.Unmarshal([]byte(doubleVal), &resp)
		lruCache.Add(cacheKey, resp)
		return resp, nil
	}

	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	taskList, err := t.taskRepo.FindTaskByStageCode(c, int(stageCode))

	if err != nil {
		zap.L().Error("FindTaskByStageCode error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	var taskDisplayList []*data.TaskDisplay
	var mIds []int64
	var taskIds []int64
	taskIdMap := make(map[int64]*data.Task)

	for _, v := range taskList {
		taskIdMap[v.Id] = v
		taskIds = append(taskIds, v.Id)
		mIds = append(mIds, v.AssignTo)
	}

	taskMemberMap := make(map[int64]*data.TaskMember)
	if len(taskIds) > 0 {
		taskMembers, err := t.taskRepo.FindTaskMemberByTaskIds(c, taskIds, memberId)
		if err == nil {
			for _, tm := range taskMembers {
				taskMemberMap[tm.TaskCode] = tm
			}
		}
	}

	for _, v := range taskList {
		display := v.ToTaskDisplay()

		if v.Private == 1 {
			if _, ok := taskMemberMap[v.Id]; ok {
				display.CanRead = model.CanRead
			} else {
				display.CanRead = model.NoCanRead
			}
		}

		taskDisplayList = append(taskDisplayList, display)
	}

	if len(taskDisplayList) == 0 {
		emptyResp := &task.TaskListResponse{List: nil}
		bs, _ := json.Marshal(emptyResp)
		_ = dao.Rc.Put(ctx, cacheKey, bs, 3*time.Minute)
		lruCache.Add(cacheKey, emptyResp)
		return emptyResp, nil
	}

	userMsg := &login.UserMessage{MIds: mIds}
	messageList, err := rpc.LoginServiceClient.FindMemInfoByIds(ctx, userMsg)
	if err != nil {
		zap.L().Error("FindMemInfoByIds error", zap.Error(err))
		return nil, err
	}

	memberMap := make(map[int64]*login.MemberMessage)
	for _, v := range messageList.List {
		memberMap[v.Id] = v
	}

	for _, display := range taskDisplayList {
		assignTo := encrypts.DecryptNoErr(display.AssignTo)
		if member, ok := memberMap[assignTo]; ok {
			display.Executor = data.Executor{
				Name:   member.Name,
				Avatar: member.Avatar,
			}
		}
	}

	var taskMessageList []*task.TaskMessage
	_ = copier.Copy(&taskMessageList, taskDisplayList)
	resp = &task.TaskListResponse{List: taskMessageList}

	bs, _ := json.Marshal(resp)
	_ = dao.Rc.Put(ctx, cacheKey, bs, 3*time.Minute)
	lruCache.Add(cacheKey, resp)

	return resp, nil
}

func (t *TaskService) SaveTask(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskMessage, error) {
	if msg == nil {
		return nil, errs.GrpcError(model.BusinessError)
	}
	if msg.Name == "" {
		return nil, errs.GrpcError(model.TaskNameNotNull)
	}

	guard := dao.IdemGuard()
	if guard == nil {
		return t.saveTaskInner(ctx, msg)
	}

	idemKey, idemTTL := taskIdemKey(ctx, msg)
	if idemKey == "" {
		return t.saveTaskInner(ctx, msg)
	}

	outcome, err := guard.DoWithTTL(ctx, idemKey, idemTTL, func(ctx context.Context) (string, error) {
		res, innerErr := t.saveTaskInner(ctx, msg)
		if innerErr != nil {
			return "", innerErr
		}
		body, mErr := json.Marshal(res)
		if mErr != nil {
			return "", mErr
		}
		return string(body), nil
	})
	if err != nil {
		return nil, err
	}
	if outcome == nil || outcome.Result == "" {
		return nil, errs.GrpcError(model.BusinessError)
	}

	var result task.TaskMessage
	if jErr := json.Unmarshal([]byte(outcome.Result), &result); jErr != nil {
		zap.L().Error("decode idempotent task result failed", zap.Error(jErr))
		return nil, errs.GrpcError(model.BusinessError)
	}
	return &result, nil
}

const formIdemWindow = 5 * time.Second

func taskIdemKey(ctx context.Context, msg *task.TaskReqMessage) (string, time.Duration) {
	if requestId := interceptor.RequestIdFromContext(ctx); requestId != "" {
		scope := fmt.Sprintf("%d:%d:%s", tenant.IdFromContext(ctx), msg.MemberId, requestId)
		return idem.Key("task.create:req", scope), dao.IdemTTL()
	}

	projectCode := encrypts.DecryptNoErr(msg.ProjectCode)
	stageCode := encrypts.DecryptNoErr(msg.StageCode)
	if projectCode <= 0 || msg.MemberId <= 0 {
		return "", 0
	}

	fingerprint := encrypts.Md5(fmt.Sprintf("%s|%d|%d|%s", msg.Name, projectCode, stageCode, msg.AssignTo))
	return idem.Key("task.create:form", fmt.Sprintf("%d:%s", msg.MemberId, fingerprint)), formIdemWindow
}

func (t *TaskService) invalidateTaskCache() {
	bg, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if lruCache != nil {
		lruCache.Purge()
	}
	if workTimeLru != nil {
		workTimeLru.Purge()
	}
	cachekv.InvalidatePattern(bg, dao.Rc, taskCachePatterns...)
	cachekv.InvalidateGroup(bg, dao.Rc, taskCacheGroup)
	kafka.PublishGroupInvalidate(bg, taskCacheGroup)
}

func (t *TaskService) saveTaskInner(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskMessage, error) {
	lockKey := fmt.Sprintf("lock:task:save:%s:%s", msg.ProjectCode, msg.StageCode)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(3*time.Second),
		redsync.WithTries(1),
	)

	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("获取分布式锁失败", zap.String("key", lockKey))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, err := mutex.UnlockContext(ctx)
		if err != nil {
			zap.L().Error("释放锁失败", zap.Error(err))
		}
	}()

	if msg.Name == "" {
		return nil, errs.GrpcError(model.TaskNameNotNull)
	}

	stageCode := encrypts.DecryptNoErr(msg.StageCode)
	taskStages, err := t.taskStagesRepo.FindById(ctx, int(stageCode))
	if err != nil {
		zap.L().Error("project task SaveTask taskStagesRepo.FindById error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if taskStages == nil {
		return nil, errs.GrpcError(model.TaskStagesNotNull)
	}

	projectCode := encrypts.DecryptNoErr(msg.ProjectCode)
	project, err := t.projectRepo.FindProjectById(ctx, projectCode)
	if err != nil {
		zap.L().Error("project task SaveTask projectRepo.FindProjectById error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if project == nil || project.Deleted == model.Deleted {
		return nil, errs.GrpcError(model.ProjectAlreadyDeleted)
	}

	maxIdNum, err := t.taskRepo.FindTaskMaxIdNum(ctx, projectCode)
	if err != nil {
		zap.L().Error("project task SaveTask taskRepo.FindTaskMaxIdNum error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if maxIdNum == nil {
		a := 0
		maxIdNum = &a
	}

	maxSort, err := t.taskRepo.FindTaskSort(ctx, projectCode, stageCode)
	if err != nil {
		zap.L().Error("project task SaveTask taskRepo.FindTaskSort error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if maxSort == nil {
		a := 0
		maxSort = &a
	}

	assignTo := encrypts.DecryptNoErr(msg.AssignTo)
	ts := &data.Task{
		Name:        msg.Name,
		CreateTime:  time.Now().UnixMilli(),
		CreateBy:    msg.MemberId,
		AssignTo:    assignTo,
		ProjectCode: projectCode,
		StageCode:   int(stageCode),
		IdNum:       *maxIdNum + 1,
		Private:     project.OpenTaskPrivate,
		Sort:        *maxSort + 65536,
		BeginTime:   time.Now().UnixMilli(),
		EndTime:     time.Now().Add(2 * 24 * time.Hour).UnixMilli(),
	}
	display := ts.ToTaskDisplay()
	err = t.transaction.Action(func(conn database.DbConn) error {
		err = t.taskRepo.SaveTask(ctx, conn, ts)
		if err != nil {
			zap.L().Error("project task SaveTask taskRepo.SaveTask error", zap.Error(err))
			return errs.GrpcError(model.DBError)
		}

		tm := &data.TaskMember{
			MemberCode: assignTo,
			TaskCode:   ts.Id,
			JoinTime:   time.Now().UnixMilli(),
			IsOwner:    model.Owner,
		}
		if assignTo == msg.MemberId {
			tm.IsExecutor = model.Executor
		}
		err = t.taskRepo.SaveTaskMember(ctx, conn, tm)
		if err != nil {
			zap.L().Error("project task SaveTask taskRepo.SaveTaskMember error", zap.Error(err))
			return errs.GrpcError(model.DBError)
		}

		body, _ := json.Marshal(display)
		outbox := &outbox.Event{
			Topic:     "task_exchange",
			MsgKey:    fmt.Sprintf("task:%d", ts.Id),
			Payload:   body,
			CreatedAt: time.Now(),
		}
		if err := t.outboxRepo.Save(ctx, conn.(*gorms.GormConn).Tx(ctx), outbox); err != nil {
			zap.L().Error("project task SaveTask outbox save error", zap.Error(err))
			return errs.GrpcError(model.DBError)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	gorms.MarkWrite(ctx)

	createProjectLog(ctx, t.projectLogRepo, ts.ProjectCode, ts.Id, ts.Name, ts.AssignTo, "create", "task")

	if t.delayService != nil {
		if _, dErr := t.delayService.ScheduleTaskReminder(ctx, ts); dErr != nil {
			zap.L().Error("schedule task reminder failed",
				zap.Int64("task_id", ts.Id), zap.Error(dErr))
		}
	}

	go t.invalidateTaskCache()

	member, err := rpc.LoginServiceClient.FindMemInfoById(ctx, &login.UserMessage{MemId: msg.MemberId})
	if err != nil {
		return nil, err
	}

	display.Executor = data.Executor{
		Name:   member.Name,
		Avatar: member.Avatar,
		Code:   member.Code,
	}

	tm := &task.TaskMessage{}
	copier.Copy(tm, display)

	dao.RecordAudit(ctx, "create", "task", fmt.Sprintf("%d", ts.Id), msg.MemberId, nil, tm, "创建任务")

	return tm, nil
}

func (t *TaskService) SaveTaskSource(ctx context.Context, msg *task.TaskSourceReqMessage) (*task.TaskSourceResponse, error) {
	taskId := dao.ParseTaskCode(msg.TaskCode)
	if taskId <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}
	if msg.SourceType == "" || msg.LinkCode == "" {
		return nil, errs.GrpcError(model.ParamsError)
	}

	linkCode := dao.ParseTaskCode(msg.LinkCode)
	if linkCode <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}

	link := &data.SourceLink{
		SourceType:       msg.SourceType,
		SourceCode:       taskId,
		LinkType:         msg.LinkType,
		LinkCode:         linkCode,
		OrganizationCode: dao.ParseTaskCode(msg.OrganizationCode),
		CreateBy:         msg.MemberId,
		CreateTime:       time.Now().UnixMilli(),
		Sort:             0,
	}
	if err := t.sourceLinkRepo.Save(ctx, link); err != nil {
		zap.L().Error("SaveTaskSource error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	dao.RecordAudit(ctx, "create", "task_source", fmt.Sprintf("%d", link.Id), msg.MemberId, nil, link, "关联任务资源")

	return &task.TaskSourceResponse{}, nil
}

func (t *TaskService) DeleteTaskSource(ctx context.Context, msg *task.TaskSourceReqMessage) (*task.TaskSourceResponse, error) {
	if msg.SourceId <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}

	link, err := t.sourceLinkRepo.FindById(ctx, msg.SourceId)
	if err != nil {
		return nil, errs.GrpcError(model.DBError)
	}
	if link == nil {
		return &task.TaskSourceResponse{}, nil
	}
	if link.CreateBy != msg.MemberId {
		taskId := dao.ParseTaskCode(msg.TaskCode)
		if taskId <= 0 || link.SourceCode != taskId {
			return nil, errs.GrpcError(model.BusinessError)
		}
	}

	rows, err := t.sourceLinkRepo.DeleteById(ctx, msg.SourceId)
	if err != nil {
		zap.L().Error("DeleteTaskSource error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if rows == 0 {
		return nil, errs.GrpcError(model.TaskNotFound)
	}

	dao.RecordAudit(ctx, "delete", "task_source", fmt.Sprintf("%d", msg.SourceId), msg.MemberId, link, nil, "取消任务资源关联")

	return &task.TaskSourceResponse{}, nil
}
func (t *TaskService) TaskStagesList(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskListResponse, error) {
	return t.TaskList(ctx, msg)
}

func (t *TaskService) TaskEdit(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskMessage, error) {
	taskId := dao.ParseTaskCode(msg.TaskCode)
	if taskId <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}

	current, err := t.taskRepo.FindTaskById(ctx, taskId)
	if err != nil {
		zap.L().Error("TaskEdit FindTaskById error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if current == nil || current.Deleted != 0 {
		return nil, errs.GrpcError(model.TaskNotFound)
	}

	updates := map[string]interface{}{}
	if msg.Name != "" {
		updates["name"] = msg.Name
	}
	if msg.Description != "" {
		updates["description"] = msg.Description
	}
	if msg.Pri > 0 {
		updates["pri"] = msg.Pri
	}
	if msg.TaskTag != "" {
		updates["task_tag"] = msg.TaskTag
	}
	if assignTo := encrypts.DecryptNoErr(msg.AssignTo); assignTo > 0 {
		updates["assign_to"] = assignTo
	}
	if msg.EndTime > 0 {
		updates["end_time"] = msg.EndTime
	}
	if msg.Schedule > 0 {
		updates["schedule"] = msg.Schedule
	}
	if stageCode := encrypts.DecryptNoErr(msg.StageCode); stageCode > 0 {
		stage, sErr := t.taskStagesRepo.FindById(ctx, int(stageCode))
		if sErr != nil {
			zap.L().Error("TaskEdit FindById stage error", zap.Error(sErr))
			return nil, errs.GrpcError(model.DBError)
		}
		if stage == nil || stage.Deleted != 0 || stage.ProjectCode != current.ProjectCode {
			return nil, errs.GrpcError(model.TaskStagesNotNull)
		}
		updates["stage_code"] = stageCode
	}
	if len(updates) == 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}
	updates["version_code"] = current.VersionCode + 1

	rows, err := t.taskRepo.UpdateTaskFieldsWithVersion(ctx, taskId, current.VersionCode, updates)
	if err != nil {
		zap.L().Error("TaskEdit UpdateTaskFields error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if rows == 0 {
		return nil, errs.GrpcError(model.OptimisticLockError)
	}

	createProjectLog(ctx, t.projectLogRepo, current.ProjectCode, taskId, current.Name, msg.MemberId, "edit", "task")
	t.invalidateTaskCache()
	dao.RecordAudit(ctx, "edit", "task", fmt.Sprintf("%d", taskId), msg.MemberId, nil, updates, "编辑任务")

	return t.ReadTask(ctx, msg)
}

func (t *TaskService) TaskDone(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskMessage, error) {
	taskId := dao.ParseTaskCode(msg.TaskCode)
	if taskId <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}

	current, err := t.taskRepo.FindTaskById(ctx, taskId)
	if err != nil || current == nil || current.Deleted != 0 {
		return nil, errs.GrpcError(model.TaskNotFound)
	}

	executeStatus := msg.ExecuteStatus
	if executeStatus <= 0 {
		if msg.Done == 1 {
			executeStatus = int32(data.Done)
		} else {
			executeStatus = int32(data.Wait)
		}
	}

	updates := map[string]interface{}{
		"done":           msg.Done,
		"execute_status": executeStatus,
	}
	if msg.Done == 1 {
		updates["done_time"] = time.Now().UnixMilli()
		updates["done_by"] = msg.MemberId
	} else {
		updates["done_time"] = int64(0)
		updates["done_by"] = int64(0)
	}

	if _, err := t.taskRepo.UpdateTaskFields(ctx, taskId, updates); err != nil {
		zap.L().Error("TaskDone UpdateTaskFields error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	createProjectLog(ctx, t.projectLogRepo, current.ProjectCode, taskId, current.Name, msg.MemberId, "taskdone", "task")
	t.invalidateTaskCache()
	dao.RecordAudit(ctx, "taskdone", "task", fmt.Sprintf("%d", taskId), msg.MemberId, nil, updates, "更改任务状态")

	return t.ReadTask(ctx, msg)
}

func (t *TaskService) TaskDelete(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskMessage, error) {
	taskId := dao.ParseTaskCode(msg.TaskCode)
	if taskId <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}

	current, err := t.taskRepo.FindTaskById(ctx, taskId)
	if err != nil || current == nil {
		return nil, errs.GrpcError(model.TaskNotFound)
	}
	if current.Deleted != 0 {
		return &task.TaskMessage{Id: taskId, Name: current.Name}, nil
	}

	if _, err := t.taskRepo.UpdateTaskFields(ctx, taskId, map[string]interface{}{
		"deleted":      1,
		"deleted_time": time.Now().UnixMilli(),
	}); err != nil {
		zap.L().Error("TaskDelete UpdateTaskFields error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	if t.delayService != nil {
		if cErr := t.delayService.CancelTaskReminder(ctx, taskId); cErr != nil {
			zap.L().Warn("cancel reminder after delete failed", zap.Int64("task_id", taskId), zap.Error(cErr))
		}
	}

	createProjectLog(ctx, t.projectLogRepo, current.ProjectCode, taskId, current.Name, msg.MemberId, "delete", "task")
	t.invalidateTaskCache()
	dao.RecordAudit(ctx, "delete", "task", fmt.Sprintf("%d", taskId), msg.MemberId, nil, nil, "删除任务")

	return &task.TaskMessage{Id: taskId, Name: current.Name}, nil
}

func createProjectLog(ctx context.Context, logRepo repo.ProjectLogRepo, projectCode int64, taskCode int64, taskName string, toMemberCode int64, logType string, actionType string) {
	remark := ""
	if logType == "create" {
		remark = "创建了任务"
	}
	pl := &data.ProjectLog{
		MemberCode:  toMemberCode,
		SourceCode:  taskCode,
		Content:     taskName,
		Remark:      remark,
		ProjectCode: projectCode,
		CreateTime:  time.Now().UnixMilli(),
		Type:        logType,
		ActionType:  actionType,
		Icon:        "plus",
		IsComment:   0,
		IsRobot:     0,
	}
	if err := logRepo.SaveProjectLog(ctx, pl); err != nil {
		zap.L().Error("save project log failed", zap.Int64("source_code", pl.SourceCode), zap.Error(err))
	}
}

func (t *TaskService) TaskSort(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskSortResponse, error) {
	preTaskCode := encrypts.DecryptNoErr(msg.PreTaskCode)
	toStageCode := encrypts.DecryptNoErr(msg.ToStageCode)

	if preTaskCode > 0 && preTaskCode == encrypts.DecryptNoErr(msg.NextTaskCode) {
		return &task.TaskSortResponse{}, nil
	}

	lockKey := fmt.Sprintf("lock:task:sort:stage:%d", toStageCode)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(5*time.Second),
		redsync.WithTries(2),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("排序被并发操作，获取锁失败", zap.String("lockKey", lockKey))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	err := t.sortTask(ctx, preTaskCode, msg.NextTaskCode, toStageCode)
	if err != nil {
		return nil, err
	}

	go t.invalidateTaskCache()

	return &task.TaskSortResponse{}, nil
}

func (t *TaskService) sortTask(ctx context.Context, preTaskCode int64, nextTaskCode string, toStageCode int64) error {
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ts, err := t.taskRepo.FindTaskByIdWithVersion(c, preTaskCode)
	if err != nil {
		zap.L().Error("FindTaskByIdWithVersion error", zap.Error(err))
		return errs.GrpcError(model.DBError)
	}
	if ts == nil {
		return errs.GrpcError(model.TaskNotFound)
	}

	err = t.transaction.Action(func(conn database.DbConn) error {
		stage, sErr := t.taskStagesRepo.FindById(c, int(toStageCode))
		if sErr != nil {
			return errs.GrpcError(model.DBError)
		}
		if stage == nil || stage.Deleted != 0 || stage.ProjectCode != ts.ProjectCode {
			return errs.GrpcError(model.TaskStagesNotNull)
		}

		ts.StageCode = int(toStageCode)

		if nextTaskCode != "" {
			nextTaskCode := encrypts.DecryptNoErr(nextTaskCode)
			next, err := t.taskRepo.FindTaskById(c, nextTaskCode)
			if err != nil {
				zap.L().Error("project task TaskSort taskRepo.FindTaskById error", zap.Error(err))
				return errs.GrpcError(model.DBError)
			}
			if next == nil {
				return errs.GrpcError(model.TaskNotFound)
			}
			if next.ProjectCode != ts.ProjectCode {
				return errs.GrpcError(model.ParamsError)
			}

			prepre, err := t.taskRepo.FindTaskByStageCodeLtSort(c, next.StageCode, next.Sort)
			if err != nil {
				zap.L().Error("project task TaskSort taskRepo.FindTaskByStageCodeLtSort error", zap.Error(err))
				return errs.GrpcError(model.DBError)
			}
			if prepre != nil {
				ts.Sort = (prepre.Sort + next.Sort) / 2
			}
		} else {
			maxSort, err := t.taskRepo.FindTaskSort(c, ts.ProjectCode, int64(ts.StageCode))
			if err != nil {
				zap.L().Error("project task TaskSort taskRepo.FindTaskSort error", zap.Error(err))
				return errs.GrpcError(model.DBError)
			}

			if maxSort == nil {
				a := 0
				maxSort = &a
			}
			ts.Sort = *maxSort + 65536
		}
		if ts.Sort < 50 {
			return errNeedResetSort
		}

		rows, err := t.taskRepo.UpdateTaskSortWithVersion(c, conn, ts)
		if err != nil {
			zap.L().Error("UpdateTaskSortWithVersion error", zap.Error(err))
			return errs.GrpcError(model.DBError)
		}

		if rows == 0 {
			zap.L().Warn("乐观锁更新失败，数据已被修改", zap.Int64("taskId", ts.Id))
			return errs.GrpcError(model.OptimisticLockError)
		}
		return nil
	})

	if errors.Is(err, errNeedResetSort) {
		if rErr := t.resetSort(ctx, toStageCode); rErr != nil {
			zap.L().Error("project task TaskSort resetSort error", zap.Error(rErr))
			return errs.GrpcError(model.DBError)
		}
		return t.sortTask(ctx, preTaskCode, nextTaskCode, toStageCode)
	}

	return err
}

var errNeedResetSort = errors.New("task sort needs reset")

func (t *TaskService) resetSort(ctx context.Context, stageCode int64) error {
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	list, err := t.taskRepo.FindTaskByStageCode(c, int(stageCode))
	if err != nil {
		return err
	}

	return t.transaction.Action(func(conn database.DbConn) error {
		iSort := 65536
		for index, v := range list {
			v.Sort = (index + 1) * iSort
			if err := t.taskRepo.UpdateTaskSort(c, conn, v); err != nil {
				return err
			}
		}
		return nil
	})
}

func (t *TaskService) MyTaskList(ctx context.Context, msg *task.TaskReqMessage) (*task.MyTaskListResponse, error) {
	var tsList []*data.Task
	var err error
	var total int64
	if msg.TaskType == 1 {
		tsList, total, err = t.taskRepo.FindTaskByAssignTo(ctx, msg.MemberId, int(msg.Type), msg.Page, msg.PageSize)
		if err != nil {
			zap.L().Error("project task MyTaskList taskRepo.FindTaskByAssignTo error", zap.Error(err))
			return nil, errs.GrpcError(model.DBError)
		}
	}
	if msg.TaskType == 2 {

		tsList, total, err = t.taskRepo.FindTaskByMemberCode(ctx, msg.MemberId, int(msg.Type), msg.Page, msg.PageSize)
		if err != nil {
			zap.L().Error("project task MyTaskList taskRepo.FindTaskByMemberCode error", zap.Error(err))
			return nil, errs.GrpcError(model.DBError)
		}
	}
	if msg.TaskType == 3 {
		tsList, total, err = t.taskRepo.FindTaskByCreateBy(ctx, msg.MemberId, int(msg.Type), msg.Page, msg.PageSize)
		if err != nil {
			zap.L().Error("project task MyTaskList taskRepo.FindTaskByCreateBy error", zap.Error(err))
			return nil, errs.GrpcError(model.DBError)
		}
	}
	if tsList == nil || len(tsList) <= 0 {
		return &task.MyTaskListResponse{List: nil, Total: 0}, nil
	}
	var pids []int64
	var mids []int64
	for _, v := range tsList {
		pids = append(pids, v.ProjectCode)
		mids = append(mids, v.AssignTo)
	}

	pListChan := make(chan []*data.Project)
	defer close(pListChan)
	mListChan := make(chan *login.MemberMessageList)
	defer close(mListChan)
	go func() {
		pList, _ := t.projectRepo.FindProjectByIds(ctx, pids)
		pListChan <- pList
	}()
	go func() {
		mList, _ := rpc.LoginServiceClient.FindMemInfoByIds(ctx, &login.UserMessage{
			MIds: mids,
		})
		mListChan <- mList
	}()
	pList := <-pListChan
	projectMap := data.ToProjectMap(pList)
	mList := <-mListChan

	mMap := make(map[int64]*login.MemberMessage)
	if mList != nil {
		for _, v := range mList.List {
			if v == nil {
				continue
			}
			mMap[v.Id] = v
		}
	}
	var mtdList []*data.MyTaskDisplay
	for _, v := range tsList {
		name := ""
		avatar := ""
		if memberMessage := mMap[v.AssignTo]; memberMessage != nil {
			name = memberMessage.Name
			avatar = memberMessage.Avatar
		}
		mtd := v.ToMyTaskDisplay(projectMap[v.ProjectCode], name, avatar)
		mtdList = append(mtdList, mtd)
	}
	var myMsgs []*task.MyTaskMessage
	copier.Copy(&myMsgs, mtdList)
	return &task.MyTaskListResponse{List: myMsgs, Total: total}, nil
}

func (t *TaskService) ReadTask(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskMessage, error) {
	taskCode := encrypts.DecryptNoErr(msg.TaskCode)
	memberId := msg.MemberId
	key := fmt.Sprintf("task:detail:%d:%d", taskCode, memberId)

	if val, ok := lruCache.Get(key); ok {
		if res, ok := val.(*task.TaskMessage); ok {
			return res, nil
		}
	}
	var res *task.TaskMessage
	val, err := dao.Rc.Get(ctx, key)
	if err == nil && val != "" {
		if json.Unmarshal([]byte(val), &res) == nil {
			lruCache.Add(key, res)
			return res, nil
		}
	}

	lockKey := fmt.Sprintf("lock:task:detail:%d", taskCode)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(3*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("缓存击穿防护，获取锁失败", zap.Int64("task", taskCode))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	if val, err := dao.Rc.Get(ctx, key); err == nil && val != "" {
		_ = json.Unmarshal([]byte(val), &res)
		lruCache.Add(key, res)
		return res, nil
	}

	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	taskInfo, err := t.taskRepo.FindTaskById(c, taskCode)
	if err != nil {
		zap.L().Error("project task ReadTask taskRepo FindTaskById error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if taskInfo == nil {
		return &task.TaskMessage{}, nil
	}

	var pj *data.Project
	var taskStages *data.TaskStages
	var memberMessage *login.MemberMessage

	err = t.transaction.Action(func(_ database.DbConn) error {
		pj, _ = t.projectRepo.FindProjectById(c, taskInfo.ProjectCode)

		taskStages, _ = t.taskStagesRepo.FindById(c, taskInfo.StageCode)

		memberMessage, _ = rpc.LoginServiceClient.FindMemInfoById(c, &login.UserMessage{MemId: taskInfo.AssignTo})
		return nil
	})

	display := taskInfo.ToTaskDisplay()

	if taskInfo.Private == 1 {
		taskMember, err := t.taskRepo.FindTaskMemberByTaskId(ctx, taskInfo.Id, memberId)
		if err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		if taskMember != nil {
			display.CanRead = model.CanRead
		} else {
			display.CanRead = model.NoCanRead
		}
	}

	if pj != nil {
		display.ProjectName = pj.Name
	}
	if taskStages != nil {
		display.StageName = taskStages.Name
	}
	if memberMessage != nil {
		display.Executor = data.Executor{
			Name:   memberMessage.Name,
			Avatar: memberMessage.Avatar,
		}
	}

	taskMessage := &task.TaskMessage{}
	_ = copier.Copy(taskMessage, display)

	bs, _ := json.Marshal(taskMessage)
	_ = dao.Rc.Put(ctx, key, string(bs), 5*time.Minute)
	lruCache.Add(key, taskMessage)

	return taskMessage, nil
}

func (t *TaskService) ListTaskMember(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskMemberList, error) {
	taskCode := encrypts.DecryptNoErr(msg.TaskCode)
	page := msg.Page
	pageSize := msg.PageSize

	cacheKey := fmt.Sprintf("task:member:list:%d:%d:%d", taskCode, page, pageSize)

	if val, ok := lruCache.Get(cacheKey); ok {
		return val.(*task.TaskMemberList), nil
	}

	var resp *task.TaskMemberList
	cacheVal, err := dao.Rc.Get(ctx, cacheKey)
	if err == nil && cacheVal != "" {
		if json.Unmarshal([]byte(cacheVal), &resp) == nil {
			lruCache.Add(cacheKey, resp)
			return resp, nil
		}
	}

	lockKey := fmt.Sprintf("lock:task:member:%d", taskCode)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(3*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("任务成员缓存击穿，获取锁失败", zap.Int64("task", taskCode))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	if doubleVal, _ := dao.Rc.Get(ctx, cacheKey); doubleVal != "" {
		_ = json.Unmarshal([]byte(doubleVal), &resp)
		lruCache.Add(cacheKey, resp)
		return resp, nil
	}

	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	taskMemberPage, total, err := t.taskRepo.FindTaskMemberPage(c, taskCode, page, pageSize)

	if err != nil {
		zap.L().Error("FindTaskMemberPage error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	var mids []int64
	for _, v := range taskMemberPage {
		mids = append(mids, v.MemberCode)
	}

	messageList, err := rpc.LoginServiceClient.FindMemInfoByIds(ctx, &login.UserMessage{MIds: mids})
	if err != nil {
		zap.L().Error("FindMemInfoByIds error", zap.Error(err))
		return nil, err
	}

	mMap := make(map[int64]*login.MemberMessage, len(messageList.List))
	for _, v := range messageList.List {
		mMap[v.Id] = v
	}

	var taskMemberMessages []*task.TaskMemberMessage
	for _, v := range taskMemberPage {
		tm := &task.TaskMemberMessage{}
		tm.Code = encrypts.EncryptNoErr(v.MemberCode)
		tm.Id = v.Id
		message := mMap[v.MemberCode]
		tm.Name = message.Name
		tm.Avatar = message.Avatar
		tm.IsExecutor = int32(v.IsExecutor)
		tm.IsOwner = int32(v.IsOwner)
		taskMemberMessages = append(taskMemberMessages, tm)
	}

	resp = &task.TaskMemberList{
		List:  taskMemberMessages,
		Total: total,
	}

	bs, _ := json.Marshal(resp)
	_ = dao.Rc.Put(ctx, cacheKey, bs, 3*time.Minute)
	lruCache.Add(cacheKey, resp)

	return resp, nil
}

func (t *TaskService) TaskLog(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskLogList, error) {
	taskCode := encrypts.DecryptNoErr(msg.TaskCode)
	all := msg.All
	comment := int(msg.Comment)
	page := int(msg.Page)
	pageSize := int(msg.PageSize)

	cacheKey := fmt.Sprintf("task:log:%d:all:%v:comment:%d:page:%d:size:%d",
		taskCode, all, comment, page, pageSize)

	if val, ok := lruCache.Get(cacheKey); ok {
		return val.(*task.TaskLogList), nil
	}

	var resp *task.TaskLogList
	cacheVal, err := dao.Rc.Get(ctx, cacheKey)
	if err == nil && cacheVal != "" {
		if json.Unmarshal([]byte(cacheVal), &resp) == nil {
			lruCache.Add(cacheKey, resp)
			return resp, nil
		}
	}

	lockKey := fmt.Sprintf("lock:task:log:%d", taskCode)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(3*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("任务日志缓存击穿", zap.Int64("task", taskCode))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	if val, _ := dao.Rc.Get(ctx, cacheKey); val != "" {
		json.Unmarshal([]byte(val), &resp)
		lruCache.Add(cacheKey, resp)
		return resp, nil
	}

	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var list []*data.ProjectLog
	var total int64

	if all == 1 {
		list, total, err = t.projectLogRepo.FindLogByTaskCode(c, taskCode, comment)
	} else {
		list, total, err = t.projectLogRepo.FindLogByTaskCodePage(c, taskCode, comment, page, pageSize)
	}

	if err != nil {
		zap.L().Error("查询任务日志失败", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	if total == 0 {
		empty := &task.TaskLogList{}
		bs, _ := json.Marshal(empty)
		_ = dao.Rc.Put(ctx, cacheKey, bs, 3*time.Minute)
		lruCache.Add(cacheKey, empty)
		return empty, nil
	}

	var mIdList []int64
	for _, v := range list {
		mIdList = append(mIdList, v.MemberCode)
	}

	messageList, err := rpc.LoginServiceClient.FindMemInfoByIds(c, &login.UserMessage{MIds: mIdList})
	if err != nil {
		zap.L().Error("批量查询用户信息失败", zap.Error(err))
		return nil, err
	}

	mMap := make(map[int64]*login.MemberMessage)
	for _, v := range messageList.List {
		mMap[v.Id] = v
	}

	var displayList []*data.ProjectLogDisplay
	for _, v := range list {
		display := v.ToDisplay()
		member, ok := mMap[v.MemberCode]
		if ok {
			display.Member = data.Member{
				Name:   member.Name,
				Id:     member.Id,
				Avatar: member.Avatar,
				Code:   member.Code,
			}
		}
		displayList = append(displayList, display)
	}

	go func() {
		data := map[string]interface{}{
			"taskCode":   taskCode,
			"total":      total,
			"queryTime":  time.Now().UnixMilli(),
			"logType":    "task_query_log",
			"memberList": mIdList,
		}
		bs, _ := json.Marshal(data)

		elkClient := elk.GetClient()
		elkClient.Index("task-service-log", string(bs))
	}()

	var logMessages []*task.TaskLog
	copier.Copy(&logMessages, displayList)
	resp = &task.TaskLogList{
		List:  logMessages,
		Total: total,
	}

	bs, _ := json.Marshal(resp)
	_ = dao.Rc.Put(ctx, cacheKey, bs, 3*time.Minute)
	lruCache.Add(cacheKey, resp)

	return resp, nil
}

func (t *TaskService) TaskWorkTimeList(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskWorkTimeResponse, error) {
	taskCode := encrypts.DecryptNoErr(msg.TaskCode)
	cacheKey := fmt.Sprintf("task:worktime:list:%d", taskCode)

	if val, ok := lruCache.Get(cacheKey); ok {
		return val.(*task.TaskWorkTimeResponse), nil
	}

	var resp *task.TaskWorkTimeResponse
	cacheVal, err := dao.Rc.Get(ctx, cacheKey)
	if err == nil && cacheVal != "" {
		if json.Unmarshal([]byte(cacheVal), &resp) == nil {
			lruCache.Add(cacheKey, resp)
			return resp, nil
		}
	}

	lockKey := fmt.Sprintf("lock:task:worktime:%d", taskCode)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(3*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("工时查询并发冲突", zap.Int64("taskCode", taskCode))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	if val, _ := dao.Rc.Get(ctx, cacheKey); val != "" {
		_ = json.Unmarshal([]byte(val), &resp)
		lruCache.Add(cacheKey, resp)
		return resp, nil
	}

	list, err := t.taskWorkTimeDomain.TaskWorkTimeList(ctx, taskCode)

	if err != nil {
		zap.L().Error("task worktime list error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	workStat, err := t.taskWorkTimeDomain.TaskWorkTimeStat(ctx, taskCode)

	if err != nil {
		zap.L().Error("task worktime stat error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	var l []*task.TaskWorkTime
	copier.Copy(&l, list)

	resp = &task.TaskWorkTimeResponse{
		List:  l,
		Total: int64(len(l)),
		Stats: &task.WorkTimeStat{
			TotalWorkTime: workStat.TotalWorkTime,
			AvgWorkTime:   workStat.AvgWorkTime,
			MaxWorkTime:   workStat.MaxWorkTime,
			MinWorkTime:   workStat.MinWorkTime,
		},
	}

	bs, _ := json.Marshal(resp)
	_ = dao.Rc.Put(ctx, cacheKey, bs, 3*time.Minute)
	lruCache.Add(cacheKey, resp)

	return resp, nil
}

func (t *TaskService) SaveTaskWorkTime(ctx context.Context, msg *task.TaskReqMessage) (*task.SaveTaskWorkTimeResponse, error) {
	taskCode := encrypts.DecryptNoErr(msg.TaskCode)
	memberId := msg.MemberId

	lockKey := fmt.Sprintf("lock:task:save:worktime:%d:%d", taskCode, memberId)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(5*time.Second),
		redsync.WithTries(1),
	)

	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("保存工时-重复提交被拦截", zap.Int64("task", taskCode), zap.Int64("member", memberId))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	tmt := &data.TaskWorkTime{}
	tmt.BeginTime = msg.BeginTime
	tmt.Num = int(msg.Num)
	tmt.Content = msg.Content
	tmt.TaskCode = taskCode
	tmt.MemberCode = memberId
	tmt.CreateTime = time.Now().UnixMilli()

	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := t.taskWorkTimeRepo.Save(c, tmt)
	if err != nil {
		zap.L().Error("保存工时失败", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	zap.L().Info("create work time",
		zap.Int64("taskCode", taskCode),
		zap.Int64("memberCode", memberId),
		zap.Int32("workTime", msg.Num),
		zap.String("type", "create_work_time"),
	)

	cacheKey := fmt.Sprintf("task:worktime:list:%d", taskCode)
	dao.Rc.Delete(ctx, []string{cacheKey})

	t.invalidateTaskCache()

	return &task.SaveTaskWorkTimeResponse{}, nil
}

func (t *TaskService) SaveTaskFile(ctx context.Context, msg *task.TaskFileReqMessage) (*task.TaskFileResponse, error) {
	taskCode := encrypts.DecryptNoErr(msg.TaskCode)

	f := &data.File{
		PathName:         msg.PathName,
		Title:            msg.FileName,
		Extension:        msg.Extension,
		Size:             int(msg.Size),
		ObjectType:       "",
		OrganizationCode: encrypts.DecryptNoErr(msg.OrganizationCode),
		TaskCode:         encrypts.DecryptNoErr(msg.TaskCode),
		ProjectCode:      encrypts.DecryptNoErr(msg.ProjectCode),
		CreateBy:         msg.MemberId,
		CreateTime:       time.Now().UnixMilli(),
		Downloads:        0,
		Extra:            "",
		Deleted:          model.NoDeleted,
		FileType:         msg.FileType,
		FileUrl:          msg.FileUrl,
		DeletedTime:      0,
	}
	err := t.fileRepo.Save(ctx, f)
	if err != nil {
		zap.L().Error("project task SaveTaskFile fileRepo.Save error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	sl := &data.SourceLink{
		SourceType:       "file",
		SourceCode:       f.Id,
		LinkType:         "task",
		LinkCode:         taskCode,
		OrganizationCode: encrypts.DecryptNoErr(msg.OrganizationCode),
		CreateBy:         msg.MemberId,
		CreateTime:       time.Now().UnixMilli(),
		Sort:             0,
	}
	err = t.sourceLinkRepo.Save(ctx, sl)
	if err != nil {
		zap.L().Error("project task SaveTaskFile sourceLinkRepo.Save error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	return &task.TaskFileResponse{}, nil
}

func (t *TaskService) TaskSources(ctx context.Context, msg *task.TaskReqMessage) (*task.TaskSourceResponse, error) {
	taskCode := encrypts.DecryptNoErr(msg.TaskCode)
	cacheKey := fmt.Sprintf("task:sources:list:%d", taskCode)

	if val, ok := lruCache.Get(cacheKey); ok {
		return val.(*task.TaskSourceResponse), nil
	}

	var resp *task.TaskSourceResponse
	cacheVal, err := dao.Rc.Get(ctx, cacheKey)
	if err == nil && cacheVal != "" {
		if json.Unmarshal([]byte(cacheVal), &resp) == nil {
			lruCache.Add(cacheKey, resp)
			return resp, nil
		}
	}

	lockKey := fmt.Sprintf("lock:task:sources:%d", taskCode)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(3*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("task sources cache break lock", zap.Int64("taskCode", taskCode))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	if val, _ := dao.Rc.Get(ctx, cacheKey); val != "" {
		json.Unmarshal([]byte(val), &resp)
		lruCache.Add(cacheKey, resp)
		return resp, nil
	}

	sourceLinks, err := t.sourceLinkRepo.FindByTaskCode(ctx, taskCode)
	if err != nil {
		zap.L().Error("TaskSources FindByTaskCode error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if len(sourceLinks) == 0 {
		empty := &task.TaskSourceResponse{}
		bs, _ := json.Marshal(empty)
		_ = dao.Rc.Put(ctx, cacheKey, bs, 5*time.Minute)
		lruCache.Add(cacheKey, empty)
		return empty, nil
	}

	var fIdList []int64
	for _, v := range sourceLinks {
		fIdList = append(fIdList, v.SourceCode)
	}

	files, err := t.fileRepo.FindByIds(ctx, fIdList)
	if err != nil {
		zap.L().Error("TaskSources FindByIds error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	fMap := make(map[int64]*data.File)
	for _, v := range files {
		fMap[v.Id] = v
	}

	var list []*data.SourceLinkDisplay
	for _, v := range sourceLinks {
		list = append(list, v.ToDisplay(fMap[v.SourceCode]))
	}

	var slMsg []*task.TaskSourceMessage
	_ = copier.Copy(&slMsg, list)

	resp = &task.TaskSourceResponse{List: slMsg}

	bs, _ := json.Marshal(resp)
	_ = dao.Rc.Put(ctx, cacheKey, bs, 5*time.Minute)
	lruCache.Add(cacheKey, resp)

	return resp, nil
}

func (t *TaskService) CreateComment(ctx context.Context, msg *task.TaskReqMessage) (*task.CreateCommentResponse, error) {
	taskCode := encrypts.DecryptNoErr(msg.TaskCode)
	memberId := msg.MemberId
	content := msg.CommentContent

	if content == "" {
		return nil, errs.GrpcError(model.ParamsError)
	}

	lockKey := fmt.Sprintf("lock:task:comment:%d:%d", taskCode, memberId)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(5*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("评论重复提交被拦截", zap.Int64("task", taskCode), zap.Int64("member", memberId))
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	taskById, err := t.taskRepo.FindTaskById(ctx, taskCode)
	if err != nil {
		zap.L().Error("project task CreateComment fileRepo.FindTaskById error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if taskById == nil {
		return nil, errs.GrpcError(model.TaskNotFound)
	}

	pl := &data.ProjectLog{
		MemberCode:   msg.MemberId,
		Content:      msg.CommentContent,
		Remark:       msg.CommentContent,
		Type:         "createComment",
		CreateTime:   time.Now().UnixMilli(),
		SourceCode:   taskCode,
		ActionType:   "task",
		ToMemberCode: 0,
		IsComment:    model.Comment,
		ProjectCode:  taskById.ProjectCode,
		Icon:         "plus",
		IsRobot:      0,
	}

	if err := t.projectLogRepo.SaveProjectLog(ctx, pl); err != nil {
		zap.L().Error("save comment failed", zap.Int64("task_id", taskCode), zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	zap.L().Info("task comment created",
		zap.Int64("taskCode", taskCode),
		zap.Int64("memberId", memberId),
		zap.Int64("projectCode", taskById.ProjectCode),
	)

	t.invalidateTaskCache()

	return &task.CreateCommentResponse{}, nil
}
