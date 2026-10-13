package task_service_v1

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"
	"test.com/others/encrypts"
	"test.com/others/errs"
	"test.com/others/tms"
	"test.com/rpcOrgrpc/task"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/pkg/model"
)

var taskActionDao = dao.NewTaskActionDao()

func (t *TaskService) decodeTaskCodes(codes []string) []int64 {
	ids := make([]int64, 0, len(codes))
	for _, code := range codes {
		if id := dao.ParseTaskCode(code); id > 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

func (t *TaskService) TaskBatchAction(ctx context.Context, msg *task.TaskBatchActionReq) (*task.TaskActionResponse, error) {
	ids := t.decodeTaskCodes(msg.TaskCodes)
	if len(ids) == 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}

	tasks, err := t.taskRepo.FindTaskByIds(ctx, ids)
	if err != nil {
		return nil, errs.GrpcError(model.DBError)
	}
	if len(tasks) == 0 {
		return nil, errs.GrpcError(model.TaskNotFound)
	}
	projectID := tasks[0].ProjectCode
	for _, item := range tasks {
		if item.ProjectCode != projectID {
			return nil, errs.GrpcError(model.ParamsError)
		}
	}

	var fields map[string]interface{}
	switch msg.Action {
	case "assign", "batch_assign":
		assignTo := dao.ParseTaskCode(msg.MemberCode)
		if assignTo <= 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		fields = map[string]interface{}{"assign_to": assignTo}
	case "like":
		fields = map[string]interface{}{"like": msg.Value}
	case "star":
		fields = map[string]interface{}{"star": msg.Value}
	case "set_private":
		fields = map[string]interface{}{"private": msg.Value}
	case "set_tag":
		fields = map[string]interface{}{"task_tag": msg.Tag}
	case "recycle", "recycle_batch":
		fields = map[string]interface{}{"deleted": 1, "deleted_time": time.Now().UnixMilli()}
	case "recovery":
		fields = map[string]interface{}{"deleted": 0, "deleted_time": 0}
	default:
		return nil, errs.GrpcError(model.ParamsError)
	}

	affected, err := taskActionDao.BatchUpdateTasks(ctx, ids, fields)
	if err != nil {
		zap.L().Error("TaskBatchAction error", zap.String("action", msg.Action), zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	if msg.Action == "recycle" || msg.Action == "recycle_batch" {
		if t.delayService != nil {
			for _, id := range ids {
				if cErr := t.delayService.CancelTaskReminder(ctx, id); cErr != nil {
					zap.L().Warn("cancel reminder after recycle failed", zap.Int64("task_id", id), zap.Error(cErr))
				}
			}
		}
	}

	t.invalidateTaskCache()
	dao.RecordAudit(ctx, msg.Action, "task", fmt.Sprintf("%v", ids), msg.MemberId, nil, fields, "任务批量操作")

	return &task.TaskActionResponse{Affected: affected}, nil
}

func (t *TaskService) TaskMemberAction(ctx context.Context, msg *task.TaskMemberActionReq) (*task.TaskActionResponse, error) {
	if msg.Action == "search" {
		list, err := taskActionDao.ListMemberAccounts(ctx, msg.Keyword, 20)
		if err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		out := make([]string, 0, len(list))
		for _, m := range list {
			out = append(out, fmt.Sprintf("%s|%s|%s|%s", encrypts.EncryptNoErr(m.MemberCode), m.Name, m.Mobile, m.Avatar))
		}
		return &task.TaskActionResponse{List: out, Affected: int64(len(out))}, nil
	}

	taskId := dao.ParseTaskCode(msg.TaskCode)
	if taskId <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}
	codes := t.decodeTaskCodes(msg.MemberCodes)
	if len(codes) == 0 {
		codes = t.decodeTaskCodes([]string{msg.Keyword})
	}
	if len(codes) == 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}

	var affected int64
	var err error
	switch msg.Action {
	case "invite", "invite_batch":
		affected, err = taskActionDao.AddTaskMembers(ctx, taskId, codes)
	case "remove":
		affected, err = taskActionDao.RemoveTaskMembers(ctx, taskId, codes)
	default:
		return nil, errs.GrpcError(model.ParamsError)
	}
	if err != nil {
		zap.L().Error("TaskMemberAction error", zap.String("action", msg.Action), zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	t.invalidateTaskCache()
	dao.RecordAudit(ctx, msg.Action, "task_member", strconv.FormatInt(taskId, 10), msg.MemberId, nil, codes, "任务成员操作")

	return &task.TaskActionResponse{Affected: affected}, nil
}

func (t *TaskService) TaskStagesAction(ctx context.Context, msg *task.TaskStagesActionReq) (*task.TaskStagesResponse, error) {
	projectCode := dao.ParseTaskCode(msg.ProjectCode)

	switch msg.Action {
	case "save":
		if projectCode <= 0 || msg.Name == "" {
			return nil, errs.GrpcError(model.ParamsError)
		}
		stage := &data.TaskStages{
			Name:        msg.Name,
			ProjectCode: projectCode,
			Description: msg.Description,
			Sort:        int(msg.Sort),
			CreateTime:  time.Now().UnixMilli(),
		}
		if stage.Sort <= 0 {
			stage.Sort = 65536
		}
		if err := taskActionDao.SaveStage(ctx, stage); err != nil {
			zap.L().Error("save stage error", zap.Error(err))
			return nil, errs.GrpcError(model.DBError)
		}
		dao.RecordAudit(ctx, "create", "task_stage", fmt.Sprintf("%d", stage.Id), msg.MemberId, nil, stage, "添加任务分组")
	case "edit":
		stageId := dao.ParseTaskCode(msg.StageCode)
		if projectCode <= 0 || stageId <= 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		fields := map[string]interface{}{}
		if msg.Name != "" {
			fields["name"] = msg.Name
		}
		if msg.Description != "" {
			fields["description"] = msg.Description
		}
		rows, err := taskActionDao.UpdateStage(ctx, projectCode, stageId, fields)
		if err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		if rows == 0 {
			return nil, errs.GrpcError(model.TaskStagesNotNull)
		}
		dao.RecordAudit(ctx, "edit", "task_stage", strconv.FormatInt(stageId, 10), msg.MemberId, nil, fields, "编辑任务分组")
	case "delete":
		stageId := dao.ParseTaskCode(msg.StageCode)
		if projectCode <= 0 || stageId <= 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		rows, err := taskActionDao.DeleteStage(ctx, projectCode, stageId)
		if err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		if rows == 0 {
			return nil, errs.GrpcError(model.TaskStagesNotNull)
		}
		dao.RecordAudit(ctx, "delete", "task_stage", strconv.FormatInt(stageId, 10), msg.MemberId, nil, nil, "删除任务分组")
	case "sort":
		stageId := dao.ParseTaskCode(msg.StageCode)
		if projectCode <= 0 || stageId <= 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		if _, err := taskActionDao.UpdateStage(ctx, projectCode, stageId, map[string]interface{}{"sort": msg.Sort}); err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
	case "index":
	default:
		return nil, errs.GrpcError(model.ParamsError)
	}

	t.invalidateTaskCache()

	list, err := taskActionDao.ListStages(ctx, projectCode)
	if err != nil {
		return nil, errs.GrpcError(model.DBError)
	}

	out := make([]*task.TaskStagesMessage, 0, len(list))
	for _, s := range list {
		out = append(out, &task.TaskStagesMessage{
			Id:          int32(s.Id),
			Name:        s.Name,
			ProjectCode: strconv.FormatInt(s.ProjectCode, 10),
			Sort:        int32(s.Sort),
			Description: s.Description,
			CreateTime:  tms.FormatByMilli(s.CreateTime),
			Deleted:     int32(s.Deleted),
		})
	}

	return &task.TaskStagesResponse{Total: int64(len(out)), List: out}, nil
}

func (t *TaskService) TaskStagesTemplateAction(ctx context.Context, msg *task.TaskStagesTemplateActionReq) (*task.TaskStagesResponse, error) {
	templateCode := dao.ParseTaskCode(msg.TemplateCode)
	listTemplateCode := templateCode

	switch msg.Action {
	case "save":
		if templateCode <= 0 || len(msg.Names) == 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		if _, err := taskActionDao.SaveTemplateStages(ctx, templateCode, msg.Names); err != nil {
			zap.L().Error("save template stages error", zap.Error(err))
			return nil, errs.GrpcError(model.DBError)
		}
		dao.RecordAudit(ctx, "create", "task_stages_template", strconv.FormatInt(templateCode, 10), msg.MemberId, nil, msg.Names, "创建任务分组模板")
	case "edit":
		stageId := dao.ParseTaskCode(msg.TemplateCode)
		if stageId <= 0 || len(msg.Names) == 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		row, err := taskActionDao.FindTemplateStageById(ctx, stageId)
		if err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		if row == nil {
			return nil, errs.GrpcError(model.TaskStagesNotNull)
		}
		listTemplateCode = int64(row.ProjectTemplateCode)
		if _, err := taskActionDao.UpdateTemplateStage(ctx, stageId, msg.Names[0]); err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
	case "delete":
		stageId := dao.ParseTaskCode(msg.TemplateCode)
		if stageId <= 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		row, err := taskActionDao.FindTemplateStageById(ctx, stageId)
		if err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		if row == nil {
			return nil, errs.GrpcError(model.TaskStagesNotNull)
		}
		listTemplateCode = int64(row.ProjectTemplateCode)
		if _, err := taskActionDao.DeleteTemplateStage(ctx, stageId); err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		dao.RecordAudit(ctx, "delete", "task_stages_template", strconv.FormatInt(stageId, 10), msg.MemberId, nil, nil, "删除任务分组模板")
	case "index":
	default:
		return nil, errs.GrpcError(model.ParamsError)
	}

	list, err := taskActionDao.ListTemplateStages(ctx, listTemplateCode)
	if err != nil {
		return nil, errs.GrpcError(model.DBError)
	}
	out := make([]*task.TaskStagesMessage, 0, len(list))
	for _, s := range list {
		out = append(out, &task.TaskStagesMessage{
			Id:          int32(s.Id),
			Name:        s.Name,
			ProjectCode: strconv.Itoa(s.ProjectTemplateCode),
			Sort:        int32(s.Sort),
			CreateTime:  tms.FormatByMilli(s.CreateTime),
		})
	}
	return &task.TaskStagesResponse{Total: int64(len(out)), List: out}, nil
}

func (t *TaskService) TaskTagAction(ctx context.Context, msg *task.TaskTagActionReq) (*task.TaskActionResponse, error) {
	projectCode := dao.ParseTaskCode(msg.ProjectCode)
	resp := &task.TaskActionResponse{}

	switch msg.Action {
	case "save":
		if projectCode <= 0 || msg.Name == "" {
			return nil, errs.GrpcError(model.ParamsError)
		}
		tag := &data.TaskTag{
			ProjectCode: projectCode,
			Name:        msg.Name,
			Color:       msg.Color,
			Sort:        int(msg.Sort),
			CreateBy:    msg.MemberId,
			CreateTime:  time.Now().UnixMilli(),
		}
		if err := taskActionDao.SaveTag(ctx, tag); err != nil {
			zap.L().Error("save tag error", zap.Error(err))
			return nil, errs.GrpcError(model.DBError)
		}
		dao.RecordAudit(ctx, "create", "task_tag", fmt.Sprintf("%d", tag.Id), msg.MemberId, nil, tag, "创建任务标签")
	case "edit":
		tagId := dao.ParseTaskCode(msg.TagId)
		if projectCode <= 0 || tagId <= 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		fields := map[string]interface{}{}
		if msg.Name != "" {
			fields["name"] = msg.Name
		}
		if msg.Color != "" {
			fields["color"] = msg.Color
		}
		if msg.Sort > 0 {
			fields["sort"] = msg.Sort
		}
		if _, err := taskActionDao.UpdateTag(ctx, projectCode, tagId, fields); err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		dao.RecordAudit(ctx, "edit", "task_tag", strconv.FormatInt(tagId, 10), msg.MemberId, nil, fields, "编辑任务标签")
	case "delete":
		tagId := dao.ParseTaskCode(msg.TagId)
		if projectCode <= 0 || tagId <= 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		if _, err := taskActionDao.DeleteTag(ctx, projectCode, tagId); err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		dao.RecordAudit(ctx, "delete", "task_tag", strconv.FormatInt(tagId, 10), msg.MemberId, nil, nil, "删除任务标签")
	case "tasks":
		if projectCode <= 0 || msg.Name == "" {
			return nil, errs.GrpcError(model.ParamsError)
		}
		tasks, err := taskActionDao.ListTasksByTag(ctx, projectCode, msg.Name)
		if err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		for _, item := range tasks {
			resp.List = append(resp.List, fmt.Sprintf("%d|%s", item.Id, item.Name))
		}
		resp.Affected = int64(len(resp.List))
		return resp, nil
	case "index":
		if projectCode <= 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
	default:
		return nil, errs.GrpcError(model.ParamsError)
	}

	list, err := taskActionDao.ListTags(ctx, projectCode)
	if err != nil {
		return nil, errs.GrpcError(model.DBError)
	}
	for _, tag := range list {
		resp.Tags = append(resp.Tags, &task.TaskTagMessage{
			Id:         tag.Id,
			Code:       encrypts.EncryptNoErr(tag.Id),
			Name:       tag.Name,
			Color:      tag.Color,
			Sort:       int32(tag.Sort),
			CreateTime: tms.FormatByMilli(tag.CreateTime),
		})
	}
	resp.Affected = int64(len(resp.Tags))
	return resp, nil
}

func (t *TaskService) TaskWorkTimeAction(ctx context.Context, msg *task.TaskWorkTimeActionReq) (*task.TaskActionResponse, error) {
	taskId := dao.ParseTaskCode(msg.TaskCode)
	if taskId <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}

	resp := &task.TaskActionResponse{}

	switch msg.Action {
	case "index", "workflow_index":
	case "edit", "workflow_edit":
		if msg.WorkTimeId <= 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		fields := map[string]interface{}{}
		if msg.Content != "" {
			fields["content"] = msg.Content
		}
		if msg.Num > 0 {
			fields["num"] = msg.Num
		}
		if msg.BeginTime > 0 {
			fields["begin_time"] = msg.BeginTime
		}
		affected, err := taskActionDao.UpdateWorkTime(ctx, taskId, msg.WorkTimeId, fields)
		if err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		if affected == 0 {
			return nil, errs.GrpcError(model.TaskNotFound)
		}
		resp.Affected = affected
		dao.RecordAudit(ctx, "edit", "task_work_time", strconv.FormatInt(msg.WorkTimeId, 10), msg.MemberId, nil, fields, "编辑任务流转")
		t.invalidateTaskCache()
		return resp, nil
	case "delete", "workflow_delete":
		if msg.WorkTimeId <= 0 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		affected, err := taskActionDao.DeleteWorkTime(ctx, taskId, msg.WorkTimeId)
		if err != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		if affected == 0 {
			return nil, errs.GrpcError(model.TaskNotFound)
		}
		resp.Affected = affected
		dao.RecordAudit(ctx, "delete", "task_work_time", strconv.FormatInt(msg.WorkTimeId, 10), msg.MemberId, nil, nil, "删除任务流转")
		t.invalidateTaskCache()
		return resp, nil
	case "save", "workflow_save":
		tmt := &data.TaskWorkTime{
			TaskCode:   taskId,
			MemberCode: msg.MemberId,
			Content:    msg.Content,
			Num:        int(msg.Num),
			BeginTime:  msg.BeginTime,
			CreateTime: time.Now().UnixMilli(),
		}
		if err := t.taskWorkTimeRepo.Save(ctx, tmt); err != nil {
			zap.L().Error("save work time error", zap.Error(err))
			return nil, errs.GrpcError(model.DBError)
		}
		resp.Affected = 1
		t.invalidateTaskCache()
		return resp, nil
	default:
		return nil, errs.GrpcError(model.ParamsError)
	}

	list, total, err := taskActionDao.ListWorkTime(ctx, taskId, msg.Page, msg.PageSize)
	if err != nil {
		return nil, errs.GrpcError(model.DBError)
	}
	for _, item := range list {
		resp.WorkTimes = append(resp.WorkTimes, &task.TaskWorkTime{
			Id:         item.Id,
			MemberCode: strconv.FormatInt(item.MemberCode, 10),
			Code:       encrypts.EncryptNoErr(item.Id),
			Content:    item.Content,
			Num:        int32(item.Num),
			BeginTime:  tms.FormatByMilli(item.BeginTime),
			CreateTime: tms.FormatByMilli(item.CreateTime),
		})
	}
	resp.Affected = total
	return resp, nil
}
