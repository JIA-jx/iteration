package domain

import (
	"context"

	"test.com/others/errs"
	"test.com/toProject/config"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"

	"go.uber.org/zap"
)

type SystemDomain struct {
	systemRepo repo.SystemRepo
}

func NewSystemDomain() *SystemDomain {
	return &SystemDomain{
		systemRepo: dao.NewSystemDao(),
	}
}

func (d *SystemDomain) GetNotifyList(ctx context.Context) ([]*data.SystemNotify, *errs.MError) {
	zap.L().Info("SystemDomain.GetNotifyList")

	list, err := d.systemRepo.List(ctx)
	if err != nil {
		zap.L().Error("SystemDomain.GetNotifyList.List", zap.Error(err))
		return nil, model.DBError
	}

	return list, nil
}

func (d *SystemDomain) QueryLogs(ctx context.Context, traceId, level string) ([]*data.SystemLog, *errs.MError) {
	zap.L().Info("SystemDomain.QueryLogs",
		zap.String("trace_id", traceId),
		zap.String("level", level),
	)

	elkAddr := config.C.Viper.GetString("elk.address")
	elkIndex := config.C.Viper.GetString("elk.index")

	logs, err := d.systemRepo.Query(ctx, traceId, level, elkAddr, elkIndex)
	if err != nil {
		zap.L().Error("SystemDomain.QueryLogs.Query", zap.Error(err))
		return nil, model.LogError
	}
	return logs, nil
}

func (d *SystemDomain) ReloadConfig(ctx context.Context) *errs.MError {
	zap.L().Info("SystemDomain.ReloadConfig")
	config.C.ReLoadAllConfig()
	return nil
}

func (d *SystemDomain) ClearCache(ctx context.Context) *errs.MError {
	zap.L().Info("SystemDomain.ClearCache")

	err := d.systemRepo.Clear(ctx)
	if err != nil {
		zap.L().Error("SystemDomain.ClearCache.Clear", zap.Error(err))
		return model.CacheClearError
	}

	return nil
}
