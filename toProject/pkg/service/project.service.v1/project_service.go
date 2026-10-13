package project_service_v1

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-redsync/redsync/v4"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
	"test.com/api/pkg/model/pro"
	"test.com/others/encrypts"
	"test.com/others/errs"
	"test.com/others/min"
	"test.com/others/tenant"
	"test.com/others/tms"
	"test.com/rpcOrgrpc/project"
	"test.com/rpcOrgrpc/user/login"
	"test.com/toProject/config"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database"
	"test.com/toProject/internal/database/tran"
	"test.com/toProject/internal/domain"
	"test.com/toProject/internal/repo"
	"test.com/toProject/internal/rpc"
	"test.com/toProject/pkg/model"
)

var projectDetailLRU, _ = lru.NewARC(10000)
var minioClient, _ = min.New(
	"localhost:9009",
	"",
	"",
	false,
)

type ProjectService struct {
	project.UnimplementedProjectServiceServer
	cache                  repo.Cache
	transaction            tran.Transaction
	menuRepo               repo.MenuRepo
	projectRepo            repo.ProjectRepo
	projectTemplateRepo    repo.ProjectTemplateRepo
	taskStagesTemplateRepo repo.TaskStagesTemplateRepo
	taskStagesRepo         repo.TaskStagesRepo
	projectLogRepo         repo.ProjectLogRepo
	taskRepo               repo.TaskRepo
	nodeDomain             *domain.ProjectNodeDomain
	taskDomain             *domain.TaskDomain
	minioClient            *min.MinioClient
}

func New() *ProjectService {
	return &ProjectService{
		cache:                  dao.Rc,
		transaction:            dao.NewTransaction(),
		menuRepo:               dao.NewMenuDao(),
		projectRepo:            dao.NewProjectDao(),
		projectTemplateRepo:    dao.NewProjectTemplateDao(),
		taskStagesTemplateRepo: dao.NewTaskStagesTemplateDao(),
		taskStagesRepo:         dao.NewTaskStagesDao(),
		projectLogRepo:         dao.NewProjectLogDao(),
		taskRepo:               dao.NewTaskDao(),
		nodeDomain:             domain.NewProjectNodeDomain(),
		taskDomain:             domain.NewTaskDomain(),
		minioClient:            minioClient,
	}
}

func (p *ProjectService) Index(ctx context.Context, _ *project.IndexMessage) (*project.IndexResponse, error) {
	cacheKey := fmt.Sprintf("project:index:menus:%d", tenant.IdFromContext(ctx))

	cacheData, err := dao.Rc.Get(ctx, cacheKey)
	if err == nil && cacheData != "" {

		var pms []*data.ProjectMenu
		_ = json.Unmarshal([]byte(cacheData), &pms)

		childs := data.CovertChild(pms)
		var mms []*project.MenuMessage
		_ = copier.Copy(&mms, childs)

		go func() {
			zap.L().Info("ProjectService.Index",
				zap.Int("menu_count", len(childs)),
				zap.String("cache", "hit"),
			)
		}()

		return &project.IndexResponse{Menus: mms}, nil
	}

	pms, err := p.menuRepo.FindMenus(ctx)
	if err != nil {
		zap.L().Error("Index db FindMenus error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	bytesData, _ := json.Marshal(pms)
	_ = dao.Rc.Put(ctx, cacheKey, string(bytesData), 5*time.Minute)

	childs := data.CovertChild(pms)
	var mms []*project.MenuMessage
	copier.Copy(&mms, childs)

	go func() {
		zap.L().Info("ProjectService.Index",
			zap.Int("menu_count", len(childs)),
			zap.String("cache", "miss"),
		)
	}()

	return &project.IndexResponse{Menus: mms}, nil
}

func (p *ProjectService) FindProjectByMemId(ctx context.Context, msg *project.ProjectRpcMessage) (*project.MyProjectResponse, error) {
	memberId := msg.MemberId
	page := msg.Page
	pageSize := msg.PageSize
	selectBy := msg.SelectBy

	cacheKey := fmt.Sprintf("project:mylist:%d:%s:%d:%d",
		memberId, selectBy, page, pageSize)

	cacheData, err := dao.Rc.Get(ctx, cacheKey)
	if err == nil && cacheData != "" {
		var resp project.MyProjectResponse
		_ = json.Unmarshal([]byte(cacheData), &resp)
		return &resp, nil
	}

	var pms []*data.ProjectAndMember
	var total int64

	switch selectBy {
	case "", "my":
		pms, total, err = p.projectRepo.FindProjectByMemId(ctx, memberId, "and deleted=0 ", page, pageSize)
	case "archive":
		pms, total, err = p.projectRepo.FindProjectByMemId(ctx, memberId, "and archive=1 ", page, pageSize)
	case "deleted":
		pms, total, err = p.projectRepo.FindProjectByMemId(ctx, memberId, "and deleted=1 ", page, pageSize)
	case "collect":
		pms, total, err = p.projectRepo.FindCollectProjectByMemId(ctx, memberId, page, pageSize)
		for _, v := range pms {
			v.Collected = 1
		}
	}
	if err != nil {
		zap.L().Error("FindProjectByMemId db error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	if selectBy != "collect" {
		collectList, _, err := p.projectRepo.FindCollectProjectByMemId(ctx, memberId, page, pageSize)
		if err == nil {
			collectMap := make(map[int64]bool)
			for _, cm := range collectList {
				collectMap[cm.ProjectCode] = true
			}

			for _, pm := range pms {
				if collectMap[pm.ProjectCode] {
					pm.Collected = 1
				}
			}
		}
	}

	var pmm []*project.ProjectMessage
	_ = copier.Copy(&pmm, pms)
	pamMap := data.ToMap(pms)

	for _, v := range pmm {
		pam := pamMap[v.Id]
		v.Code, _ = encrypts.EncryptInt64(v.ProjectCode, model.AESKey)
		v.AccessControlType = pam.GetAccessControlType()
		v.OrganizationCode, _ = encrypts.EncryptInt64(pam.OrganizationCode, model.AESKey)
		v.JoinTime = tms.FormatByMilli(pam.JoinTime)
		v.OwnerName = msg.MemberName
		v.Order = int32(pam.Sort)
		v.CreateTime = tms.FormatByMilli(pam.CreateTime)
	}

	resp := &project.MyProjectResponse{
		Pm:    pmm,
		Total: total,
	}

	bytesResp, _ := json.Marshal(resp)
	_ = dao.Rc.Put(ctx, cacheKey, string(bytesResp), 5*time.Minute)

	return resp, nil
}

func (ps *ProjectService) FindProjectTemplate(ctx context.Context, msg *project.ProjectRpcMessage) (*project.ProjectTemplateResponse, error) {
	funcName := "FindProjectTemplate"

	if msg.Page < 1 || msg.PageSize < 1 || msg.PageSize > 100 {
		zap.L().Error("FindProjectTemplate 参数非法")
		return nil, errs.GrpcError(model.ParamsError)
	}

	cacheKey := fmt.Sprintf("project:template:list:%s:%d:%d:%d", msg.OrganizationCode, msg.ViewType, msg.Page, msg.PageSize)
	var cacheData string
	var ptMsgs []*project.ProjectTemplateMessage
	var total int64

	cacheData, err := ps.cache.Get(ctx, cacheKey)
	if err == nil && cacheData != "" {
		type cacheStruct struct {
			List  []*project.ProjectTemplateMessage
			Total int64
		}
		var cs cacheStruct
		_ = json.Unmarshal([]byte(cacheData), &cs)
		return &project.ProjectTemplateResponse{Ptm: cs.List, Total: cs.Total}, nil
	}

	orgCodeStr, err := encrypts.Decrypt(msg.OrganizationCode, model.AESKey)
	if err != nil {
		zap.L().Error("组织解密失败", zap.Error(err))
		return nil, errs.GrpcError(model.ParamsError)
	}
	organizationCode, _ := strconv.ParseInt(orgCodeStr, 10, 64)

	var templateList []data.ProjectTemplate
	switch msg.ViewType {
	case -1:
		templateList, total, err = ps.projectTemplateRepo.FindProjectTemplateAll(ctx, organizationCode, msg.Page, msg.PageSize)
	case 0:
		templateList, total, err = ps.projectTemplateRepo.FindProjectTemplateCustom(ctx, msg.MemberId, organizationCode, msg.Page, msg.PageSize)
	case 1:
		templateList, total, err = ps.projectTemplateRepo.FindProjectTemplateSystem(ctx, msg.Page, msg.PageSize)
	default:
		return nil, errs.GrpcError(model.ParamsError)
	}

	if err != nil {
		zap.L().Error("查询模板失败", zap.Error(err))

		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", funcName),
		)
		return nil, errs.GrpcError(model.DBError)
	}

	if len(templateList) == 0 {
		ps.setTemplateCache(ctx, cacheKey, []*project.ProjectTemplateMessage{}, 0)
		return &project.ProjectTemplateResponse{Ptm: []*project.ProjectTemplateMessage{}, Total: 0}, nil
	}

	ids := data.ToProjectTemplateIds(templateList)
	stages, err := ps.taskStagesTemplateRepo.FindInProTemIds(ctx, ids)
	if err != nil {
		zap.L().Error("查询任务阶段失败", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	stagesMap := data.CovertProjectMap(stages)
	var allList []*data.ProjectTemplateAll
	for _, t := range templateList {
		allList = append(allList, t.Convert(stagesMap[t.Id]))
	}

	_ = copier.Copy(&ptMsgs, allList)
	ps.setTemplateCache(ctx, cacheKey, ptMsgs, total)

	zap.L().Info("查询项目模板成功", zap.String("func", funcName))

	return &project.ProjectTemplateResponse{Ptm: ptMsgs, Total: total}, nil
}

func (ps *ProjectService) setTemplateCache(ctx context.Context, key string, list []*project.ProjectTemplateMessage, total int64) {
	type T struct {
		List  []*project.ProjectTemplateMessage
		Total int64
	}
	bs, _ := json.Marshal(T{List: list, Total: total})
	_ = ps.cache.Put(ctx, key, string(bs), time.Minute)
}

func (ps *ProjectService) SaveProject(ctx context.Context, msg *project.ProjectRpcMessage) (*project.SaveProjectMessage, error) {
	organizationCodeStr, _ := encrypts.Decrypt(msg.OrganizationCode, model.AESKey)
	organizationCode, _ := strconv.ParseInt(organizationCodeStr, 10, 64)
	templateCodeStr, _ := encrypts.Decrypt(msg.TemplateCode, model.AESKey)
	templateCode, _ := strconv.ParseInt(templateCodeStr, 10, 64)

	if t, tErr := dao.FindTenantById(ctx, tenant.IdFromContext(ctx)); tErr != nil {
		zap.L().Error("查询租户配额失败", zap.Error(tErr))
		return nil, errs.GrpcError(model.DBError)
	} else if t != nil && t.ProjectQuota > 0 {
		used, cErr := ps.projectRepo.CountByTenant(ctx)
		if cErr != nil {
			zap.L().Error("统计租户项目数失败", zap.Error(cErr))
			return nil, errs.GrpcError(model.DBError)
		}
		if used >= int64(t.ProjectQuota) {
			return nil, errs.GrpcError(errs.NewError(403001, "租户项目配额已用尽"))
		}
	}

	stageTemplateList, err := ps.taskStagesTemplateRepo.FindByProjectTemplateId(ctx, int(templateCode))
	if err != nil {
		zap.L().Error("查询项目模板阶段失败", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	pr := &data.Project{
		Name:              msg.Name,
		Description:       msg.Description,
		TemplateCode:      int(templateCode),
		CreateTime:        time.Now().UnixMilli(),
		Cover:             "https://img2.baidu.com/it/u=792555388,2449797505&fm=253&fmt=auto&app=138&f=JPEG?w=667&h=500",
		Deleted:           model.NoDeleted,
		Archive:           model.NoArchive,
		OrganizationCode:  organizationCode,
		AccessControlType: model.Open,
		TaskBoardTheme:    model.Simple,
	}

	err = ps.transaction.Action(func(conn database.DbConn) error {
		err := ps.projectRepo.SaveProject(conn, ctx, pr)
		if err != nil {
			zap.L().Error("保存项目失败", zap.Error(err))
			return errs.GrpcError(model.DBError)
		}
		pm := &data.ProjectMember{
			ProjectCode: pr.Id,
			MemberCode:  msg.MemberId,
			JoinTime:    time.Now().UnixMilli(),
			IsOwner:     msg.MemberId,
			Authorize:   "",
		}

		err = ps.projectRepo.SaveProjectMember(conn, ctx, pm)
		if err != nil {
			zap.L().Error("保存项目成员失败", zap.Error(err))
			return errs.GrpcError(model.DBError)
		}

		for index, v := range stageTemplateList {
			taskStage := &data.TaskStages{
				ProjectCode: pr.Id,
				Name:        v.Name,
				Sort:        index + 1,
				Description: "",
				CreateTime:  time.Now().UnixMilli(),
				Deleted:     model.NoDeleted,
			}
			err := ps.taskStagesRepo.SaveTaskStages(ctx, conn, taskStage)
			if err != nil {
				zap.L().Error("保存任务阶段失败", zap.Error(err))
				return errs.GrpcError(model.DBError)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	ps.invalidateProjectCache(msg.MemberId)

	code, _ := encrypts.EncryptInt64(pr.Id, model.AESKey)
	rsp := &project.SaveProjectMessage{
		Id:               pr.Id,
		Code:             code,
		OrganizationCode: organizationCodeStr,
		Name:             pr.Name,
		Cover:            pr.Cover,
		CreateTime:       tms.FormatByMilli(pr.CreateTime),
		TaskBoardTheme:   pr.TaskBoardTheme,
	}
	return rsp, nil
}

func (ps *ProjectService) FindProjectDetail(c context.Context, msg *project.ProjectRpcMessage) (*project.ProjectDetailMessage, error) {
	projectCodeStr, err := encrypts.Decrypt(msg.ProjectCode, model.AESKey)
	if err != nil {
		zap.L().Error("解密projectCode失败", zap.Error(err))
		return nil, errs.GrpcError(model.ParamsError)
	}

	projectCode, err := strconv.ParseInt(projectCodeStr, 10, 64)
	if err != nil || projectCode <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}
	memberId := msg.MemberId

	key := fmt.Sprintf("project:detail:%d:member:%d", projectCode, memberId)
	nullKey := key + ":null"

	if v, ok := projectDetailLRU.Get(key); ok {
		return v.(*project.ProjectDetailMessage), nil
	}

	cacheStr, err := dao.Rc.Get(c, key)
	if err == nil && cacheStr != "" {
		var detail project.ProjectDetailMessage
		if json.Unmarshal([]byte(cacheStr), &detail) == nil {
			projectDetailLRU.Add(key, &detail)
			return &detail, nil
		}
	}

	nullVal, _ := dao.Rc.Get(c, nullKey)
	if nullVal == "1" {
		return nil, errs.GrpcError(model.NotFoundError)
	}

	projectAndMember, err := ps.projectRepo.FindProjectByPIdAndMemId(c, projectCode, memberId)
	if err != nil {
		zap.L().Error("project FindProjectDetail FindProjectByPIdAndMemId error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if projectAndMember == nil {

		dao.Rc.Put(c, nullKey, "1", 60*time.Second)
		return nil, errs.GrpcError(model.ParamsError)
	}

	ownerId := projectAndMember.IsOwner
	member, err := rpc.LoginServiceClient.FindMemInfoById(c, &login.UserMessage{MemId: ownerId})
	if err != nil {
		zap.L().Error("project rpc FindProjectDetail FindMemInfoById error", zap.Error(err))
		return nil, err
	}

	isCollect, err := ps.projectRepo.FindCollectByPidAndMemId(c, projectCode, memberId)
	if err != nil {
		zap.L().Error("project FindProjectDetail FindCollectByPidAndMemId error", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if isCollect {
		projectAndMember.Collected = model.Collected
	}

	var detailMsg = &project.ProjectDetailMessage{}
	copier.Copy(detailMsg, projectAndMember)

	detailMsg.OwnerAvatar = member.Avatar
	detailMsg.OwnerName = member.Name
	detailMsg.Code, _ = encrypts.EncryptInt64(projectAndMember.Id, model.AESKey)
	detailMsg.AccessControlType = projectAndMember.GetAccessControlType()
	detailMsg.OrganizationCode, _ = encrypts.EncryptInt64(projectAndMember.OrganizationCode, model.AESKey)
	detailMsg.Order = int32(projectAndMember.Sort)
	detailMsg.CreateTime = tms.FormatByMilli(projectAndMember.CreateTime)

	bs, _ := json.Marshal(detailMsg)

	expire := 5*time.Minute + time.Duration(rand.Intn(60))*time.Second
	dao.Rc.Put(c, key, string(bs), expire)
	projectDetailLRU.Add(key, detailMsg)

	return detailMsg, nil
}

func (ps *ProjectService) UpdateDeletedProject(ctx context.Context, msg *project.ProjectRpcMessage) (*project.DeletedProjectResponse, error) {
	projectCodeStr, err := encrypts.Decrypt(msg.ProjectCode, model.AESKey)
	if err != nil {
		zap.L().Error("解密projectCode失败", zap.Error(err))

		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", "UpdateDeletedProject"),
		)
		return nil, errs.GrpcError(model.ParamsError)
	}

	projectCode, err := strconv.ParseInt(projectCodeStr, 10, 64)
	if err != nil || projectCode <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}
	memberId := msg.MemberId

	lockKey := fmt.Sprintf("lock:project:operate:%d", projectCode)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(3*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("获取分布式锁失败", zap.Int64("projectId", projectCode), zap.Error(err))
		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", "UpdateDeletedProject"),
		)
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.Unlock()
	}()

	err = ps.projectRepo.UpdateDeletedProject(ctx, projectCode, msg.Deleted)
	if err != nil {
		zap.L().Error("软删除项目失败", zap.Error(err))
		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", "UpdateDeletedProject"),
		)
		return nil, errs.GrpcError(model.ParamsError)
	}

	zap.L().Info("项目软删除成功", zap.String("func", "UpdateDeletedProject"))

	ps.invalidateProjectCache(memberId)

	return &project.DeletedProjectResponse{}, nil
}

func (ps *ProjectService) UpdateCollectProject(ctx context.Context, msg *project.ProjectRpcMessage) (*project.CollectProjectResponse, error) {
	projectCodeStr, err := encrypts.Decrypt(msg.ProjectCode, model.AESKey)
	if err != nil {
		zap.L().Error("解密projectCode失败", zap.Error(err))
		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", "UpdateCollectProject"),
		)
		return nil, errs.GrpcError(model.ParamsError)
	}

	projectCode, err := strconv.ParseInt(projectCodeStr, 10, 64)
	if err != nil || projectCode <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}
	memberId := msg.MemberId
	collectType := msg.CollectType

	lockKey := fmt.Sprintf("lock:project:collect:%d:member:%d", projectCode, memberId)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(3*time.Second),
		redsync.WithTries(1),
	)

	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("获取收藏锁失败", zap.Int64("projectId", projectCode), zap.Error(err))
		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", "UpdateCollectProject"),
		)
		return nil, errs.GrpcError(model.BusinessError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	if collectType == "collect" {
		pc := &data.ProjectCollection{
			ProjectCode: projectCode,
			MemberCode:  memberId,
			CreateTime:  time.Now().UnixMilli(),
		}
		err = ps.projectRepo.SaveProjectCollect(ctx, pc)
	} else if collectType == "cancel" {
		err = ps.projectRepo.DeleteProjectCollect(ctx, memberId, projectCode)
	} else {
		zap.L().Error("收藏类型不合法", zap.String("type", collectType))
		return nil, errs.GrpcError(model.ParamsError)
	}

	if err != nil {
		zap.L().Error("收藏操作DB失败", zap.Error(err))
		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", "UpdateCollectProject"),
		)
		return nil, errs.GrpcError(model.DBError)
	}

	zap.L().Info("项目收藏操作成功", zap.String("func", "UpdateCollectProject"))

	ps.invalidateProjectCache(memberId,
		fmt.Sprintf("project:collect:%d:member:%d", projectCode, memberId),
		fmt.Sprintf("project:detail:%d:member:%d", projectCode, memberId))

	return &project.CollectProjectResponse{}, nil
}

func (ps *ProjectService) UpdateProject(ctx context.Context, msg *project.UpdateProjectMessage) (*project.UpdateProjectResponse, error) {
	projectCodeStr, err := encrypts.Decrypt(msg.ProjectCode, model.AESKey)
	if err != nil {
		zap.L().Error("解密projectCode失败", zap.Error(err))

		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", "UpdateProject"),
		)
		return nil, errs.GrpcError(model.ParamsError)
	}

	projectCode, err := strconv.ParseInt(projectCodeStr, 10, 64)
	if err != nil || projectCode <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}
	memberId := msg.MemberId

	proj := &data.Project{
		Id:                 projectCode,
		Name:               msg.Name,
		Description:        msg.Description,
		Cover:              msg.Cover,
		TaskBoardTheme:     msg.TaskBoardTheme,
		Prefix:             msg.Prefix,
		Private:            int(msg.Private),
		OpenPrefix:         int(msg.OpenPrefix),
		OpenBeginTime:      int(msg.OpenBeginTime),
		OpenTaskPrivate:    int(msg.OpenTaskPrivate),
		Schedule:           msg.Schedule,
		AutoUpdateSchedule: int(msg.AutoUpdateSchedule),
	}

	err = ps.projectRepo.UpdateProject(ctx, proj)
	if err != nil {
		zap.L().Error("更新项目失败", zap.Error(err))

		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", "UpdateProject"),
		)
		return nil, errs.GrpcError(model.DBError)
	}

	zap.L().Info("项目编辑成功", zap.String("func", "UpdateProject"))

	ps.invalidateProjectCache(memberId)

	return &project.UpdateProjectResponse{}, nil
}

func (ps *ProjectService) invalidateProjectCache(memberId int64, detailKeys ...string) {
	bg, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if projectDetailLRU != nil {
		projectDetailLRU.Purge()
	}

	keys := make([]string, 0, len(detailKeys)+8)
	for _, k := range detailKeys {
		if k != "" {
			keys = append(keys, k)
		}
	}
	if memberId > 0 {
		if ks, err := dao.Rc.Keys(bg, fmt.Sprintf("project:mylist:%d:*", memberId)); err == nil {
			keys = append(keys, ks...)
		}
		if ks, err := dao.Rc.Keys(bg, fmt.Sprintf("project:detail:*:member:%d", memberId)); err == nil {
			keys = append(keys, ks...)
		}
	}
	if len(keys) > 0 {
		dao.Rc.Delete(bg, keys)
	}
}

func (ps *ProjectService) GetLogBySelfProject(ctx context.Context, msg *project.ProjectRpcMessage) (*project.ProjectLogResponse, error) {
	memberId := msg.MemberId
	page := msg.Page
	pageSize := msg.PageSize

	if memberId <= 0 || page < 1 || pageSize < 1 {
		zap.L().Error("参数非法", zap.Int64("memberId", memberId))
		return nil, errs.GrpcError(model.ParamsError)
	}

	projectLogs, total, err := ps.projectLogRepo.FindLogByMemberCode(ctx, msg.MemberId, msg.Page, msg.PageSize)
	if err != nil {
		zap.L().Error("查询项目日志失败", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	if len(projectLogs) == 0 {
		return &project.ProjectLogResponse{List: []*project.ProjectLogMessage{}, Total: 0}, nil
	}

	pIdList := make([]int64, 0, len(projectLogs))
	mIdList := make([]int64, 0, len(projectLogs))
	taskIdList := make([]int64, 0, len(projectLogs))

	for _, v := range projectLogs {
		pIdList = append(pIdList, v.ProjectCode)
		mIdList = append(mIdList, v.MemberCode)
		taskIdList = append(taskIdList, v.SourceCode)
	}

	projects, err := ps.projectRepo.FindProjectByIds(ctx, pIdList)
	if err != nil {
		zap.L().Error("批量查询项目失败", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	pMap := make(map[int64]*data.Project)
	for _, v := range projects {
		pMap[v.Id] = v
	}

	messageList, _ := rpc.LoginServiceClient.FindMemInfoByIds(ctx, &login.UserMessage{MIds: mIdList})
	mMap := make(map[int64]*login.MemberMessage)
	if messageList != nil {
		for _, v := range messageList.List {
			if v == nil {
				continue
			}
			mMap[v.Id] = v
		}
	}

	tasks, err := ps.taskRepo.FindTaskByIds(ctx, taskIdList)
	if err != nil {
		zap.L().Error("批量查询任务失败", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	tMap := make(map[int64]*data.Task)
	for _, v := range tasks {
		tMap[v.Id] = v
	}

	list := make([]*data.IndexProjectLogDisplay, 0, len(projectLogs))
	for _, v := range projectLogs {
		display := v.ToIndexDisplay()

		if p, ok := pMap[v.ProjectCode]; ok {
			display.ProjectName = p.Name
		}
		if m, ok := mMap[v.MemberCode]; ok {
			display.MemberName = m.Name
			display.MemberAvatar = m.Avatar
		}
		if t, ok := tMap[v.SourceCode]; ok {
			display.TaskName = t.Name
		}

		list = append(list, display)
	}

	var msgList []*project.ProjectLogMessage
	copier.Copy(&msgList, list)

	return &project.ProjectLogResponse{List: msgList, Total: total}, nil
}

func (ps *ProjectService) FindProjectByMemberId(ctx context.Context, msg *project.ProjectRpcMessage) (*project.FindProjectByMemberIdResponse, error) {
	isProjectCode := false
	var projectId int64
	if msg.ProjectCode != "" {
		projectId = dao.ParseTaskCode(msg.ProjectCode)
		isProjectCode = true
	}
	isTaskCode := false
	var taskId int64
	if msg.TaskCode != "" {
		taskId = dao.ParseTaskCode(msg.TaskCode)
		isTaskCode = true
	}
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if isProjectCode && isTaskCode {
		taskProjectCode, ok, bError := ps.taskDomain.FindProjectIdByTaskId(ctx, taskId)
		if bError != nil {
			return nil, bError
		}
		if !ok || taskProjectCode != projectId {
			return &project.FindProjectByMemberIdResponse{
				Project:  nil,
				IsOwner:  false,
				IsMember: false,
			}, nil
		}
	}
	if !isProjectCode && isTaskCode {
		projectCode, ok, bError := ps.taskDomain.FindProjectIdByTaskId(ctx, taskId)
		if bError != nil {
			return nil, bError
		}
		if !ok {
			return &project.FindProjectByMemberIdResponse{
				Project:  nil,
				IsOwner:  false,
				IsMember: false,
			}, nil
		}
		projectId = projectCode
		isProjectCode = true
	}
	if isProjectCode {

		pm, err := ps.projectRepo.FindProjectByPIdAndMemId(c, projectId, msg.MemberId)
		if err != nil {
			return nil, model.DBError
		}
		if pm == nil {
			return &project.FindProjectByMemberIdResponse{
				Project:  nil,
				IsOwner:  false,
				IsMember: false,
			}, nil
		}
		projectMessage := &project.ProjectMessage{}
		copier.Copy(projectMessage, pm)
		isOwner := false
		if pm.IsOwner == 1 {
			isOwner = true
		}
		return &project.FindProjectByMemberIdResponse{
			Project:  projectMessage,
			IsOwner:  isOwner,
			IsMember: true,
		}, nil
	}
	return &project.FindProjectByMemberIdResponse{}, nil
}

func (ps *ProjectService) UploadCover(ctx context.Context, msg *project.CoverUploadRequest) (*project.CoverUploadResponse, error) {
	funcName := "UploadCover"
	const bucketName = "project"

	head := msg.FileData
	if len(head) > 512 {
		head = head[:512]
	}
	contentType := http.DetectContentType(head)
	if !strings.HasPrefix(contentType, "image/") {
		return nil, errs.GrpcError(model.OnlyImage)
	}

	coverKey := generateCoverKey(msg.MemberId, msg.FileData)

	_, err := ps.minioClient.Put(
		ctx,
		bucketName,
		coverKey,
		msg.FileData,
		int64(len(msg.FileData)),
		contentType,
	)
	if err != nil {
		zap.L().Error("[ProjectService] MinIO上传失败", zap.Error(err))

		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", funcName),
		)
		return nil, errs.GrpcError(model.UploadImageFail)
	}

	zap.L().Info("模板封面上传成功", zap.String("func", funcName))

	return &project.CoverUploadResponse{
		CoverKey: coverKey,
	}, nil
}

func generateCoverKey(memberId int64, fileData []byte) string {
	hash := md5.Sum(fileData)
	hashStr := hex.EncodeToString(hash[:8])
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("cover_%d_%d_%s", timestamp, memberId, hashStr)
}

func (ps *ProjectService) SaveTemplate(ctx context.Context, msg *project.SaveTemplateRequest) (*project.SaveTemplateResponse, error) {
	funcName := "SaveTemplate"

	if msg.Name == "" || msg.Description == "" || msg.CoverKey == "" {
		return nil, errs.GrpcError(model.ParamsError)
	}

	lockKey := fmt.Sprintf("lock:template:save:%d", msg.MemberId)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(5*time.Second),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Error("[SaveTemplate] 获取分布式锁失败", zap.Error(err))
		return nil, errs.GrpcError(model.FrequentErr)
	}
	defer mutex.UnlockContext(ctx)

	orgCode, err := ps.projectTemplateRepo.FindPersonalOrg(ctx, msg.MemberId)
	if err != nil {
		zap.L().Error("[SaveTemplate] 获取个人组织失败", zap.Error(err))
		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", funcName),
		)
		return nil, errs.GrpcError(model.GetOrgErr)
	}

	template := &pro.ProjectTemplate{
		Name:             msg.Name,
		Description:      msg.Description,
		OrganizationCode: orgCode,
		Cover:            msg.CoverKey,
		MemberCode:       strconv.FormatInt(msg.MemberId, 10),
		IsSystem:         0,
		Sort:             0,
		CreateTime:       strconv.FormatInt(time.Now().UnixMilli(), 10),
	}

	err = ps.projectTemplateRepo.SaveTemplate(ctx, template)
	if err != nil {
		zap.L().Error("[SaveTemplate] 保存模板失败", zap.Error(err))
		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", funcName),
		)
		return nil, errs.GrpcError(model.SaveTempErr)
	}

	zap.L().Info("项目模板保存成功", zap.String("func", funcName))

	return &project.SaveTemplateResponse{
		Code: 200,
		Msg:  "保存成功",
	}, nil
}

func (ps *ProjectService) DeleteTemplate(ctx context.Context, msg *project.DeleteTemplateRequest) (*project.DeleteTemplateResponse, error) {
	funcName := "DeleteTemplate"

	if msg.Code == "" {
		zap.L().Error("["+funcName+"] 参数错误：模板编码为空", zap.String("code", msg.Code))
		return nil, errs.GrpcError(model.TempEmpCode)
	}

	lockKey := fmt.Sprintf("lock:template:delete:%s", msg.Code)
	mutex := config.RedSync().NewMutex(
		lockKey,
		redsync.WithExpiry(3*time.Second),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Error("["+funcName+"] 获取分布式锁失败", zap.Error(err), zap.String("code", msg.Code))
		return nil, errs.GrpcError(model.FrequentErr)
	}
	defer mutex.UnlockContext(ctx)

	err := ps.projectTemplateRepo.DeleteTemplate(ctx, msg.Code)
	if err != nil {
		zap.L().Error("["+funcName+"] 删除模板失败", zap.Error(err), zap.String("code", msg.Code))

		zap.L().Error("error",
			zap.Error(err),
			zap.String("func", funcName),
		)
		return nil, errs.GrpcError(model.FailToDel)
	}

	zap.L().Info("模板删除成功", zap.String("func", funcName))

	zap.L().Info("["+funcName+"] 模板删除成功", zap.String("code", msg.Code))
	return &project.DeleteTemplateResponse{
		Code: 200,
		Msg:  "删除成功",
	}, nil
}

func (ps *ProjectService) GetRecycleBinList(ctx context.Context, msg *project.RecycleBinListRequest) (*project.RecycleBinListResponse, error) {
	if msg.Page < 1 || msg.PageSize < 1 || msg.PageSize > 100 {
		zap.L().Error("[ProjectService] GetRecycleBinList 参数非法")
		return nil, errs.GrpcError(model.ParamsError)
	}

	cacheKey := fmt.Sprintf("project:recycle:list:%d:%d:%d", tenant.IdFromContext(ctx), msg.Page, msg.PageSize)
	var respList []*data.Project
	var total int64

	type recycleCache struct {
		List  []*data.Project `json:"list"`
		Total int64           `json:"total"`
	}

	cacheData, err := ps.cache.Get(ctx, cacheKey)
	if err == nil && cacheData != "" {
		zap.L().Debug("[ProjectService] GetRecycleBinList 回收站列表 缓存命中")

		var cached recycleCache
		if jErr := json.Unmarshal([]byte(cacheData), &cached); jErr != nil {
			_, _ = ps.cache.Del(ctx, cacheKey)
		} else {
			var pbList []*project.ProjectMessage
			_ = copier.Copy(&pbList, cached.List)
			return &project.RecycleBinListResponse{
				List:  pbList,
				Total: cached.Total,
			}, nil
		}
	}

	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	respList, total, err = ps.projectRepo.FindRecycleBinList(c, msg.Page, msg.PageSize)
	if err != nil {
		zap.L().Error("[ProjectService] GetRecycleBinList 查询回收站列表失败", zap.Error(err))
		return nil, model.DBError
	}

	if len(respList) > 0 {
		jsonBytes, _ := json.Marshal(recycleCache{List: respList, Total: total})
		_ = ps.cache.Put(ctx, cacheKey, string(jsonBytes), time.Minute*1)
	}

	var pbList []*project.ProjectMessage
	_ = copier.Copy(&pbList, respList)

	return &project.RecycleBinListResponse{
		List:  pbList,
		Total: total,
	}, nil
}
