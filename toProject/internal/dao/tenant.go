package dao

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"test.com/others/llm"
	"test.com/others/tenant"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
	"test.com/toProject/internal/repo"
)

type tenantDao struct {
	conn *gorms.GormConn
}

func NewTenantDao() repo.TenantRepo {
	return &tenantDao{conn: gorms.New()}
}

func (d *tenantDao) FindById(ctx context.Context, id int64) (*data.Tenant, error) {
	var t data.Tenant
	err := d.conn.Master().WithContext(ctx).Where("id = ?", id).First(&t).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (d *tenantDao) List(ctx context.Context) ([]*data.Tenant, error) {
	var list []*data.Tenant
	err := d.conn.Master().WithContext(ctx).Order("id ASC").Find(&list).Error
	return list, err
}

func (d *tenantDao) Save(ctx context.Context, t *data.Tenant) error {
	now := time.Now()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	t.UpdatedAt = now
	return d.conn.Master().WithContext(ctx).Save(t).Error
}

func (d *tenantDao) AddAIUsage(ctx context.Context, id int64, tokens int64) error {
	if tokens <= 0 {
		return nil
	}
	return d.conn.Master().WithContext(ctx).
		Model(&data.Tenant{}).
		Where("id = ?", id).
		UpdateColumns(map[string]interface{}{
			"ai_used_tokens": gorm.Expr("ai_used_tokens + ?", tokens),
			"updated_at":     time.Now(),
		}).Error
}

var defaultTenantRepo repo.TenantRepo

func tenantRepo() repo.TenantRepo {
	if defaultTenantRepo == nil {
		defaultTenantRepo = NewTenantDao()
	}
	return defaultTenantRepo
}

func AddAIUsage(ctx context.Context, tenantId int64, tokens int64) error {
	if tenantId <= 0 {
		tenantId = tenant.DefaultId
	}
	return tenantRepo().AddAIUsage(ctx, tenantId, tokens)
}

func AIQuotaAvailable(ctx context.Context, tenantId int64) bool {
	if tenantId <= 0 {
		tenantId = tenant.DefaultId
	}

	t, err := tenantRepo().FindById(ctx, tenantId)
	if err != nil || t == nil {
		return true
	}
	if t.Status != 1 {
		return false
	}
	if t.AIQuotaTokens <= 0 {
		return true
	}
	return t.AIUsedTokens < t.AIQuotaTokens
}

func AIQuotaAvailableFor(ctx context.Context) bool {
	return AIQuotaAvailable(ctx, tenant.IdFromContext(ctx))
}

func ListTenants(ctx context.Context) ([]*data.Tenant, error) {
	return tenantRepo().List(ctx)
}

func FindTenantById(ctx context.Context, id int64) (*data.Tenant, error) {
	return tenantRepo().FindById(ctx, id)
}

func SaveTenant(ctx context.Context, t *data.Tenant) error {
	return tenantRepo().Save(ctx, t)
}

func InitAIUsageHook() {
	llm.SetDefaultUsageHook(func(ctx context.Context, model string, inputTokens, outputTokens int) {
		_ = AddAIUsage(ctx, tenant.IdFromContext(ctx), int64(inputTokens+outputTokens))
	})
}
