package system_service_v1

import (
	"context"
	"github.com/jinzhu/copier"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/system"
	"test.com/toProject/internal/domain"
)

type SystemService struct {
	system.UnimplementedSystemServiceServer
	sysDomain *domain.SystemDomain
}

func New() *SystemService {
	return &SystemService{
		sysDomain: domain.NewSystemDomain(),
	}
}

func (s *SystemService) NotifyList(ctx context.Context, req *system.NotifyListRequest) (*system.NotifyListResponse, error) {
	list, bErr := s.sysDomain.GetNotifyList(ctx)
	if bErr != nil {
		return nil, errs.GrpcError(bErr)
	}

	var respList []*system.NotifyItem
	copier.Copy(&respList, list)

	return &system.NotifyListResponse{
		List: respList,
	}, nil
}

func (s *SystemService) LogQuery(ctx context.Context, req *system.LogQueryRequest) (*system.LogQueryResponse, error) {
	logs, bErr := s.sysDomain.QueryLogs(ctx, req.TraceId, req.Level)
	if bErr != nil {
		return nil, errs.GrpcError(bErr)
	}

	var respLogs []*system.LogItem
	copier.Copy(&respLogs, logs)

	return &system.LogQueryResponse{
		Logs: respLogs,
	}, nil
}

func (s *SystemService) ConfigReload(ctx context.Context, req *system.ConfigReloadRequest) (*system.ConfigReloadResponse, error) {
	bErr := s.sysDomain.ReloadConfig(ctx)
	if bErr != nil {
		return nil, errs.GrpcError(bErr)
	}

	return &system.ConfigReloadResponse{
		Msg: "config reload success",
	}, nil
}

func (s *SystemService) CacheClear(ctx context.Context, req *system.CacheClearRequest) (*system.CacheClearResponse, error) {
	bErr := s.sysDomain.ClearCache(ctx)
	if bErr != nil {
		return nil, errs.GrpcError(bErr)
	}

	return &system.CacheClearResponse{
		Msg: "cache clear success",
	}, nil
}
