package workflow_v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"test.com/others/tenant"
	"test.com/others/wfengine"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
)

type Service struct {
	engine  *wfengine.Engine
	defRepo repo.WorkflowDefinitionRepo
	now     func() time.Time
}

func New(store wfengine.Store, defRepo repo.WorkflowDefinitionRepo) *Service {
	return &Service{
		engine:  wfengine.NewEngine(store, wfengine.Options{}),
		defRepo: defRepo,
		now:     time.Now,
	}
}

func (s *Service) Engine() *wfengine.Engine {
	return s.engine
}

func resolveTenant(ctx context.Context, tenantId int64) int64 {
	if tenantId > 0 {
		return tenantId
	}
	return tenant.IdFromContext(ctx)
}

func (s *Service) SaveDefinition(ctx context.Context, tenantId int64, def *wfengine.Definition, remark string, activate bool) (*data.WorkflowDefinition, error) {
	if def == nil {
		return nil, errors.New("流程定义不能为空")
	}
	if def.Key == "" {
		return nil, errors.New("流程定义 key 不能为空")
	}
	if err := def.Validate(); err != nil {
		return nil, fmt.Errorf("流程定义校验失败: %w", err)
	}

	tenantId = resolveTenant(ctx, tenantId)

	version, err := s.defRepo.NextVersion(ctx, tenantId, def.Key)
	if err != nil {
		return nil, err
	}
	def.Version = version

	body, err := json.Marshal(def)
	if err != nil {
		return nil, fmt.Errorf("流程定义序列化失败: %w", err)
	}

	status := data.WorkflowDefinitionDisabled
	if activate {
		status = data.WorkflowDefinitionEnabled
		if err := s.defRepo.DisableAll(ctx, tenantId, def.Key); err != nil {
			return nil, err
		}
	}

	now := s.now()
	row := &data.WorkflowDefinition{
		TenantId:       tenantId,
		DefKey:         def.Key,
		Name:           def.Name,
		Version:        version,
		DefinitionJson: string(body),
		Status:         status,
		Remark:         remark,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := s.defRepo.Save(ctx, row); err != nil {
		return nil, err
	}
	return row, nil
}

func (s *Service) LoadDefinition(ctx context.Context, tenantId int64, defKey string, version int) (*wfengine.Definition, error) {
	tenantId = resolveTenant(ctx, tenantId)

	var row *data.WorkflowDefinition
	var err error
	if version > 0 {
		row, err = s.defRepo.FindByKeyVersion(ctx, tenantId, defKey, version)
	} else {
		row, err = s.defRepo.FindLatestEnabled(ctx, tenantId, defKey)
	}
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("未找到流程定义 %s(version=%d)", defKey, version)
	}

	var def wfengine.Definition
	if err := json.Unmarshal([]byte(row.DefinitionJson), &def); err != nil {
		return nil, fmt.Errorf("流程定义反序列化失败: %w", err)
	}
	def.Version = row.Version
	return &def, nil
}

func (s *Service) ListDefinitions(ctx context.Context, tenantId int64, defKey string, page, pageSize int64) ([]*data.WorkflowDefinition, int64, error) {
	return s.defRepo.List(ctx, resolveTenant(ctx, tenantId), defKey, page, pageSize)
}

func (s *Service) Start(ctx context.Context, tenantId int64, defKey string, version int, submitterId int64, bizId string, variables map[string]string) (*wfengine.Instance, error) {
	def, err := s.LoadDefinition(ctx, tenantId, defKey, version)
	if err != nil {
		return nil, err
	}

	return s.engine.Start(ctx, def, wfengine.StartRequest{
		BizId:       bizId,
		TenantId:    resolveTenant(ctx, tenantId),
		SubmitterId: submitterId,
		Variables:   variables,
	})
}

func (s *Service) Submit(ctx context.Context, instanceId string, actor int64, comment string) (*wfengine.Instance, error) {
	return s.engine.Submit(ctx, instanceId, actor, comment)
}

func (s *Service) Approve(ctx context.Context, instanceId, taskId string, actor int64, comment string) (*wfengine.Instance, error) {
	return s.engine.Approve(ctx, instanceId, taskId, actor, comment)
}

func (s *Service) Reject(ctx context.Context, instanceId, taskId string, actor int64, comment, targetNodeKey string) (*wfengine.Instance, error) {
	return s.engine.Reject(ctx, instanceId, taskId, actor, comment, targetNodeKey)
}

func (s *Service) Transfer(ctx context.Context, instanceId, taskId string, actor, toMemberId int64, comment string) (*wfengine.Instance, error) {
	return s.engine.Transfer(ctx, instanceId, taskId, actor, toMemberId, comment)
}

func (s *Service) AddSigner(ctx context.Context, instanceId, taskId string, actor int64, before bool, memberIds []int64, comment string) (*wfengine.Instance, error) {
	return s.engine.AddSigner(ctx, instanceId, taskId, actor, before, memberIds, comment)
}

func (s *Service) Withdraw(ctx context.Context, instanceId string, actor int64, comment string) (*wfengine.Instance, error) {
	return s.engine.Withdraw(ctx, instanceId, actor, comment)
}

func (s *Service) Get(ctx context.Context, instanceId string) (*wfengine.Instance, error) {
	return s.engine.Get(ctx, instanceId)
}

func (s *Service) ListByBiz(ctx context.Context, tenantId int64, bizId string) ([]*wfengine.Instance, error) {
	return s.engine.ListByBiz(ctx, resolveTenant(ctx, tenantId), bizId)
}

func (s *Service) PendingTasks(ctx context.Context, approverId int64, tenantId int64) ([]*wfengine.Task, error) {
	return s.engine.ListPendingTasks(ctx, approverId, resolveTenant(ctx, tenantId))
}

func (s *Service) HandleTimeout(ctx context.Context) (int, error) {
	return s.engine.HandleTimeout(ctx, s.now())
}
