package delay_task_v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"test.com/others/delay"
	"test.com/others/event"
	"test.com/others/kk"
	"test.com/others/tenant"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
)

const (
	TypeTaskRemind = "task_remind"

	TypeInviteExpire = "invite_expire"
)

const (
	TaskRemindLeadTime = 24 * time.Hour

	DefaultInviteTTL = 7 * 24 * time.Hour

	notifyTopic = "delay_notify"
)

type TaskRemindPayload struct {
	TaskId      int64  `json:"task_id"`
	TaskCode    int64  `json:"task_code"`
	ProjectCode int64  `json:"project_code"`
	TaskName    string `json:"task_name"`
	AssignTo    int64  `json:"assign_to"`
	EndTime     int64  `json:"end_time"`
	TenantId    int64  `json:"tenant_id"`
}

type InviteExpirePayload struct {
	InviteId    string `json:"invite_id"`
	ProjectCode int64  `json:"project_code"`
	InviterId   int64  `json:"inviter_id"`
	ExpireAt    int64  `json:"expire_at"`
	TenantId    int64  `json:"tenant_id"`
}

func tenantOf(ctx context.Context, hint int64) int64 {
	if hint > 0 {
		return hint
	}
	return tenant.IdFromContext(ctx)
}

func withTenant(ctx context.Context, tenantId int64) context.Context {
	return tenant.WithContext(ctx, tenant.Info{Id: tenantId})
}

func TaskReminderId(taskId int64) string {
	return fmt.Sprintf("%s:%d", TypeTaskRemind, taskId)
}

func InviteExpireId(inviteId string) string {
	return fmt.Sprintf("%s:%s", TypeInviteExpire, inviteId)
}

var currentService *DelayTaskService

func CurrentService() *DelayTaskService {
	return currentService
}

type DelayTaskService struct {
	store     delay.Store
	taskRepo  repo.DelayTaskRepo
	taskQuery repo.TaskRepo
	notifyDao *dao.NotificationDao
	kafka     *kk.KafkaWriter
}

func New(store delay.Store, scheduler *delay.Scheduler, kafkaWriter *kk.KafkaWriter) *DelayTaskService {
	s := &DelayTaskService{
		store:     store,
		taskRepo:  dao.NewDelayTaskDao(),
		taskQuery: dao.NewTaskDao(),
		notifyDao: dao.NewNotificationDao(),
		kafka:     kafkaWriter,
	}

	if scheduler != nil {
		scheduler.Register(TypeTaskRemind, s.withLedgerResult(s.handleTaskRemind))
		scheduler.Register(TypeInviteExpire, s.withLedgerResult(s.handleInviteExpire))
	}
	currentService = s
	return s
}

func (s *DelayTaskService) withLedgerResult(h delay.HandlerFunc) delay.HandlerFunc {
	return func(ctx context.Context, due delay.Due) error {
		err := h(ctx, due)
		if err == nil {
			_ = s.taskRepo.UpdateStatus(ctx, due.Id, delay.StatusDone, "", due.Attempts)
		}
		return err
	}
}

func (s *DelayTaskService) ScheduleTaskReminder(ctx context.Context, t *data.Task) (created bool, err error) {
	if t == nil || t.Id <= 0 {
		return false, nil
	}
	if t.EndTime <= 0 {

		return false, nil
	}
	if t.Done == 1 {

		return false, nil
	}

	runAt := time.UnixMilli(t.EndTime).Add(-TaskRemindLeadTime)

	if runAt.Before(time.Now()) {
		return false, nil
	}

	payload, mErr := json.Marshal(TaskRemindPayload{
		TaskId:      t.Id,
		TaskCode:    t.Id,
		ProjectCode: t.ProjectCode,
		TaskName:    t.Name,
		AssignTo:    t.AssignTo,
		EndTime:     t.EndTime,
		TenantId:    tenantOf(ctx, t.TenantId),
	})
	if mErr != nil {
		return false, fmt.Errorf("marshal task remind payload: %w", mErr)
	}

	taskId := TaskReminderId(t.Id)
	if err := s.store.Enqueue(ctx, &delay.Task{
		Id:      taskId,
		Type:    TypeTaskRemind,
		Payload: payload,
		RunAt:   runAt.UnixMilli(),
	}); err != nil {
		return false, err
	}

	if err := s.taskRepo.Upsert(ctx, &data.DelayTask{
		TaskId:      taskId,
		Type:        TypeTaskRemind,
		Payload:     string(payload),
		RunAt:       runAt.UnixMilli(),
		Status:      delay.StatusPending,
		MemberId:    t.AssignTo,
		ProjectCode: t.ProjectCode,
	}); err != nil {
		zap.L().Warn("delay task ledger upsert failed", zap.String("task_id", taskId), zap.Error(err))
	}

	zap.L().Info("task reminder scheduled",
		zap.String("delay_task_id", taskId),
		zap.Int64("task_id", t.Id),
		zap.Time("run_at", runAt))
	return true, nil
}

func (s *DelayTaskService) CancelTaskReminder(ctx context.Context, taskId int64) error {
	id := TaskReminderId(taskId)
	ok, err := s.store.Cancel(ctx, id)
	if err != nil {
		return err
	}
	if !ok {

		zap.L().Info("cancel task reminder: not pending", zap.String("delay_task_id", id))
		return nil
	}
	_ = s.taskRepo.UpdateStatus(ctx, id, delay.StatusCanceled, "canceled by business", 0)
	zap.L().Info("task reminder canceled", zap.String("delay_task_id", id))
	return nil
}

func (s *DelayTaskService) ScheduleInviteExpire(ctx context.Context, inviteId string, projectCode, inviterId int64, ttl time.Duration) (int64, error) {
	if inviteId == "" {
		return 0, errors.New("inviteId is required")
	}
	if ttl <= 0 {
		ttl = DefaultInviteTTL
	}
	expireAt := time.Now().Add(ttl)

	payload, mErr := json.Marshal(InviteExpirePayload{
		InviteId:    inviteId,
		ProjectCode: projectCode,
		InviterId:   inviterId,
		ExpireAt:    expireAt.UnixMilli(),
		TenantId:    tenantOf(ctx, 0),
	})
	if mErr != nil {
		return 0, fmt.Errorf("marshal invite expire payload: %w", mErr)
	}

	taskId := InviteExpireId(inviteId)
	if err := s.store.Enqueue(ctx, &delay.Task{
		Id:      taskId,
		Type:    TypeInviteExpire,
		Payload: payload,
		RunAt:   expireAt.UnixMilli(),
	}); err != nil {
		return 0, err
	}

	if err := s.taskRepo.Upsert(ctx, &data.DelayTask{
		TaskId:      taskId,
		Type:        TypeInviteExpire,
		Payload:     string(payload),
		RunAt:       expireAt.UnixMilli(),
		Status:      delay.StatusPending,
		MemberId:    inviterId,
		ProjectCode: projectCode,
	}); err != nil {
		zap.L().Warn("invite expire ledger upsert failed", zap.String("task_id", taskId), zap.Error(err))
	}

	zap.L().Info("invite expire scheduled",
		zap.String("delay_task_id", taskId),
		zap.Time("expire_at", expireAt))
	return expireAt.UnixMilli(), nil
}

func (s *DelayTaskService) CancelInviteExpire(ctx context.Context, inviteId string) error {
	id := InviteExpireId(inviteId)
	ok, err := s.store.Cancel(ctx, id)
	if err != nil {
		return err
	}
	if ok {
		_ = s.taskRepo.UpdateStatus(ctx, id, delay.StatusCanceled, "invite accepted or revoked", 0)
	}
	return nil
}

type DelayTaskStatus struct {
	TaskId   string `json:"task_id"`
	Status   string `json:"status"`
	Source   string `json:"source"`
	Attempts int    `json:"attempts"`
	LastErr  string `json:"last_error,omitempty"`
	RunAt    int64  `json:"run_at,omitempty"`
}

func (s *DelayTaskService) QueryStatus(ctx context.Context, taskId string) (*DelayTaskStatus, error) {
	if taskId == "" {
		return nil, errors.New("taskId is required")
	}

	status, err := s.store.Pending(ctx, taskId)
	if err != nil {
		zap.L().Warn("query delay task from redis failed", zap.String("task_id", taskId), zap.Error(err))
	}
	if status != "" {
		return &DelayTaskStatus{TaskId: taskId, Status: status, Source: "redis"}, nil
	}

	row, err := s.taskRepo.FindByTaskId(ctx, taskId)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return &DelayTaskStatus{
		TaskId:   row.TaskId,
		Status:   row.Status,
		Source:   "db",
		Attempts: row.Attempts,
		LastErr:  row.LastError,
		RunAt:    row.RunAt,
	}, nil
}

func (s *DelayTaskService) List(ctx context.Context, taskType, status string, page, pageSize int64) ([]*data.DelayTask, int64, error) {
	return s.taskRepo.List(ctx, taskType, status, page, pageSize)
}

func (s *DelayTaskService) QueueStats(ctx context.Context) (pending, inflight int64, err error) {
	pending, err = s.store.PendingCount(ctx)
	if err != nil {
		return 0, 0, err
	}
	inflight, err = s.store.InflightCount(ctx)
	if err != nil {
		return pending, 0, err
	}
	return pending, inflight, nil
}

func (s *DelayTaskService) RebuildReminders(ctx context.Context, limit int) (int, error) {
	tasks, err := s.taskRepo.ListPendingReminders(ctx, limit)
	if err != nil {
		return 0, err
	}

	created := 0
	for _, t := range tasks {
		ok, sErr := s.ScheduleTaskReminder(ctx, t)
		if sErr != nil {
			zap.L().Error("rebuild reminder failed", zap.Int64("task_id", t.Id), zap.Error(sErr))
			continue
		}
		if ok {
			created++
		}
	}
	zap.L().Info("rebuild reminders done", zap.Int("scanned", len(tasks)), zap.Int("created", created))
	return created, nil
}

func (s *DelayTaskService) handleTaskRemind(ctx context.Context, due delay.Due) error {
	var payload TaskRemindPayload
	if err := json.Unmarshal(due.Payload, &payload); err != nil {

		return fmt.Errorf("unmarshal task remind payload: %w", err)
	}
	if payload.TaskId <= 0 {
		return errors.New("task remind payload has empty task_id")
	}
	ctx = withTenant(ctx, payload.TenantId)

	task, err := s.taskQuery.FindTaskById(ctx, payload.TaskId)
	if err != nil {
		return fmt.Errorf("load task %d: %w", payload.TaskId, err)
	}
	if task == nil {
		zap.L().Info("task remind skipped: task not found", zap.Int64("task_id", payload.TaskId))
		return nil
	}
	if task.Done == 1 {
		zap.L().Info("task remind skipped: task already done", zap.Int64("task_id", payload.TaskId))
		return nil
	}
	if task.Deleted == 1 {
		zap.L().Info("task remind skipped: task deleted", zap.Int64("task_id", payload.TaskId))
		return nil
	}

	if task.EndTime > 0 && time.Until(time.UnixMilli(task.EndTime)) > TaskRemindLeadTime+time.Hour {
		zap.L().Info("task remind skipped: end_time moved later",
			zap.Int64("task_id", payload.TaskId), zap.Int64("end_time", task.EndTime))
		return nil
	}

	assignTo := task.AssignTo
	if assignTo <= 0 {
		assignTo = payload.AssignTo
	}
	if assignTo <= 0 {
		zap.L().Info("task remind skipped: no assignee", zap.Int64("task_id", payload.TaskId))
		return nil
	}

	title := "任务即将到期提醒"
	content := fmt.Sprintf("任务《%s》将在 %s 到期，请及时处理。",
		task.Name, time.UnixMilli(task.EndTime).Format("2006-01-02 15:04"))

	evt := event.DelayNotifyEvent{
		TaskId:      due.Id,
		Type:        TypeTaskRemind,
		BizId:       task.Id,
		ProjectCode: task.ProjectCode,
		ToMemberId:  assignTo,
		Title:       title,
		Content:     content,
		Ts:          time.Now().Unix(),
	}
	if err := s.sendEvent(ctx, evt.Topic(), evt); err != nil {

		return fmt.Errorf("send delay notify event: %w", err)
	}

	body, _ := json.Marshal(evt)
	if err := s.notifyDao.SaveNotification(ctx, &data.Notification{
		Type:      TypeTaskRemind,
		Topic:     notifyTopic,
		MsgKey:    due.Id,
		Payload:   string(body),
		CreatedAt: time.Now(),
	}); err != nil {
		return fmt.Errorf("save task remind notification: %w", err)
	}

	zap.L().Info("task remind delivered",
		zap.String("delay_task_id", due.Id),
		zap.Int64("task_id", task.Id),
		zap.Int64("to_member", assignTo))
	return nil
}

func (s *DelayTaskService) handleInviteExpire(ctx context.Context, due delay.Due) error {
	var payload InviteExpirePayload
	if err := json.Unmarshal(due.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal invite expire payload: %w", err)
	}
	if payload.InviteId == "" {
		return errors.New("invite expire payload has empty invite_id")
	}
	ctx = withTenant(ctx, payload.TenantId)

	evt := event.InviteExpiredEvent{
		InviteId:    payload.InviteId,
		ProjectCode: payload.ProjectCode,
		InviterId:   payload.InviterId,
		ExpiredAt:   payload.ExpireAt,
		Ts:          time.Now().Unix(),
	}
	if err := s.sendEvent(ctx, evt.Topic(), evt); err != nil {
		return fmt.Errorf("send invite expired event: %w", err)
	}

	body, _ := json.Marshal(evt)
	if err := s.notifyDao.SaveNotification(ctx, &data.Notification{
		Type:      TypeInviteExpire,
		Topic:     evt.Topic(),
		MsgKey:    payload.InviteId,
		Payload:   string(body),
		CreatedAt: time.Now(),
	}); err != nil {
		return fmt.Errorf("save invite expire notification: %w", err)
	}

	zap.L().Info("invite expired",
		zap.String("delay_task_id", due.Id),
		zap.String("invite_id", payload.InviteId))
	return nil
}

func (s *DelayTaskService) sendEvent(ctx context.Context, topic string, evt interface{}) error {
	if s.kafka == nil {

		zap.L().Warn("kafka writer is nil, skip event publish", zap.String("topic", topic))
		return nil
	}
	body, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return s.kafka.SendSync(ctx, kk.KKData{Topic: topic, Data: body})
}
