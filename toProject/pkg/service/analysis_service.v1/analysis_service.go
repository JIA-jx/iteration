package analysis_service_v1

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/go-redsync/redsync/v4"
	lru "github.com/hashicorp/golang-lru"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"test.com/others/errs"
	"test.com/others/event"
	"test.com/others/kk"
	"test.com/rpcOrgrpc/analysis"
	"test.com/toProject/config"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/database/tran"
	"test.com/toProject/internal/domain"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
)

const (
	cachePrefix        = "analysis:"
	cacheExpire        = 5 * time.Minute
	distributedLockTTL = 10 * time.Second
)

func normalizeProjectCode(raw string) string {
	if id := dao.ParseTaskCode(raw); id > 0 {
		return strconv.FormatInt(id, 10)
	}
	return raw
}

var (
	analysisLRU, _ = lru.NewARC(1000)
	redisClient    = dao.Rc
	locker         = config.RedSync()
)

type AnalysisService struct {
	analysis.UnimplementedAnalysisServiceServer
	cache          repo.Cache
	transaction    tran.Transaction
	kafka          *kk.KafkaWriter
	analysisDomain *domain.AnalysisDomain
	taskDomain     *domain.TaskDomain
	userDomain     *domain.UserRpcDomain
	aiDomain       *domain.AIDomain
}

func (a *AnalysisService) DeliveryRate(ctx context.Context, req *analysis.AnalysisReq) (*analysis.DeliveryRateResponse, error) {
	if req.ProjectCode == "" || req.BeginTime <= 0 || req.EndTime <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}

	cacheKey := fmt.Sprintf("%s:delivery:%s:%d:%d", cachePrefix, req.ProjectCode, req.BeginTime, req.EndTime)

	if val, ok := analysisLRU.Get(cacheKey); ok {
		return val.(*analysis.DeliveryRateResponse), nil
	}

	var resp analysis.DeliveryRateResponse
	dataStr, err := a.cache.Get(ctx, cacheKey)
	if err == nil && dataStr != "" {
		if err := protojson.Unmarshal([]byte(dataStr), &resp); err == nil {
			analysisLRU.Add(cacheKey, &resp)
			return &resp, nil
		}
	}

	mutex := locker.NewMutex(fmt.Sprintf("lock:%s", cacheKey), redsync.WithExpiry(8*time.Second))
	if err := mutex.Lock(); err != nil {
		zap.L().Warn("analysis cache lock busy, wait for cache",
			zap.String("key", cacheKey), zap.Error(err))
		for i := 0; i < 3; i++ {
			time.Sleep(150 * time.Millisecond)
			if dataStr, rErr := a.cache.Get(ctx, cacheKey); rErr == nil && dataStr != "" {
				if uErr := protojson.Unmarshal([]byte(dataStr), &resp); uErr == nil {
					analysisLRU.Add(cacheKey, &resp)
					return &resp, nil
				}
			}
		}
	} else {
		defer func() {
			_, _ = mutex.Unlock()
		}()
	}

	dataStr, err = a.cache.Get(ctx, cacheKey)
	if err == nil && dataStr != "" {
		if err := protojson.Unmarshal([]byte(dataStr), &resp); err == nil {
			analysisLRU.Add(cacheKey, &resp)
			return &resp, nil
		}
	}

	res, err := a.analysisDomain.DeliveryRate(
		ctx, normalizeProjectCode(req.ProjectCode), req.BeginTime, req.EndTime)
	if err != nil {
		return nil, errs.GrpcError(model.BusinessError)
	}

	resp = analysis.DeliveryRateResponse{
		Total:     res.Total,
		Completed: res.Completed,
		Doing:     res.Doing,
		Overdue:   res.Overdue,
		Rate:      res.Rate,
	}

	bs, _ := marshalCache(&resp)
	randomExpire := cacheExpire + time.Duration(rand.Intn(120))*time.Second
	_ = a.cache.Put(ctx, cacheKey, string(bs), randomExpire)

	analysisLRU.Add(cacheKey, &resp)

	go func() {
		evt := event.AnalysisDeliveryEvent{
			ProjectCode: req.ProjectCode,
			Total:       res.Total,
			Completed:   res.Completed,
			Rate:        res.Rate,
			Ts:          time.Now().Unix(),
		}
		data, _ := json.Marshal(evt)
		_ = a.kafka.SendSync(ctx, kk.KKData{
			Topic: evt.Topic(),
			Data:  data,
		})
	}()

	return &resp, nil
}

func (a *AnalysisService) Burndown(ctx context.Context, req *analysis.AnalysisReq) (*analysis.BurndownResponse, error) {
	if req.ProjectCode == "" {
		return nil, errs.GrpcError(model.ParamsError)
	}

	cacheKey := fmt.Sprintf("%s:burndown:%s:%d:%d", cachePrefix, req.ProjectCode, req.BeginTime, req.EndTime)

	if val, ok := analysisLRU.Get(cacheKey); ok {
		return val.(*analysis.BurndownResponse), nil
	}

	var resp analysis.BurndownResponse
	dataStr, err := a.cache.Get(ctx, cacheKey)
	if err == nil && dataStr != "" {
		if err := protojson.Unmarshal([]byte(dataStr), &resp); err == nil {
			analysisLRU.Add(cacheKey, &resp)
			return &resp, nil
		}
	}

	mutex := locker.NewMutex(fmt.Sprintf("lock:%s", cacheKey), redsync.WithExpiry(8*time.Second))
	if err := mutex.Lock(); err != nil {
		zap.L().Warn("analysis cache lock busy, wait for cache",
			zap.String("key", cacheKey), zap.Error(err))
		for i := 0; i < 3; i++ {
			time.Sleep(150 * time.Millisecond)
			if dataStr, rErr := a.cache.Get(ctx, cacheKey); rErr == nil && dataStr != "" {
				if uErr := protojson.Unmarshal([]byte(dataStr), &resp); uErr == nil {
					analysisLRU.Add(cacheKey, &resp)
					return &resp, nil
				}
			}
		}
	} else {
		defer func() {
			_, _ = mutex.Unlock()
		}()
	}

	dataStr, err = a.cache.Get(ctx, cacheKey)
	if err == nil && dataStr != "" {
		if err := protojson.Unmarshal([]byte(dataStr), &resp); err == nil {
			analysisLRU.Add(cacheKey, &resp)
			return &resp, nil
		}
	}

	points, err := a.analysisDomain.Burndown(ctx, normalizeProjectCode(req.ProjectCode), req.BeginTime, req.EndTime)
	if err != nil {
		if err == domain.ErrBurndownRangeTooLong {
			return nil, errs.GrpcError(model.ParamsError)
		}
		return nil, errs.GrpcError(model.BusinessError)
	}

	resp = analysis.BurndownResponse{
		Points: points,
	}

	bs, _ := marshalCache(&resp)
	randomExpire := cacheExpire + time.Duration(rand.Intn(120))*time.Second
	_ = a.cache.Put(ctx, cacheKey, string(bs), randomExpire)

	analysisLRU.Add(cacheKey, &resp)

	go func() {
		evt := event.AnalysisBurndownEvent{
			ProjectCode: req.ProjectCode,
			PointCount:  len(points),
			Ts:          time.Now().Unix(),
		}
		data, _ := json.Marshal(evt)
		_ = a.kafka.SendSync(ctx, kk.KKData{
			Topic: evt.Topic(),
			Data:  data,
		})
	}()

	return &resp, nil
}

func (a *AnalysisService) TeamLoad(ctx context.Context, req *analysis.AnalysisReq) (*analysis.TeamLoadResponse, error) {
	if req.ProjectCode == "" {
		return nil, errs.GrpcError(model.ParamsError)
	}

	cacheKey := fmt.Sprintf("%s:teamload:%s:%d:%d", cachePrefix, req.ProjectCode, req.BeginTime, req.EndTime)

	if val, ok := analysisLRU.Get(cacheKey); ok {
		return val.(*analysis.TeamLoadResponse), nil
	}

	var resp analysis.TeamLoadResponse
	dataStr, err := a.cache.Get(ctx, cacheKey)
	if err == nil && dataStr != "" {
		if err := protojson.Unmarshal([]byte(dataStr), &resp); err == nil {
			analysisLRU.Add(cacheKey, &resp)
			return &resp, nil
		}
	}

	mutex := locker.NewMutex(fmt.Sprintf("lock:%s", cacheKey), redsync.WithExpiry(8*time.Second))
	if err := mutex.Lock(); err != nil {
		zap.L().Warn("analysis cache lock busy, wait for cache",
			zap.String("key", cacheKey), zap.Error(err))
		for i := 0; i < 3; i++ {
			time.Sleep(150 * time.Millisecond)
			if dataStr, rErr := a.cache.Get(ctx, cacheKey); rErr == nil && dataStr != "" {
				if uErr := protojson.Unmarshal([]byte(dataStr), &resp); uErr == nil {
					analysisLRU.Add(cacheKey, &resp)
					return &resp, nil
				}
			}
		}
	} else {
		defer func() {
			_, _ = mutex.Unlock()
		}()
	}

	dataStr, err = a.cache.Get(ctx, cacheKey)
	if err == nil && dataStr != "" {
		if err := protojson.Unmarshal([]byte(dataStr), &resp); err == nil {
			analysisLRU.Add(cacheKey, &resp)
			return &resp, nil
		}
	}

	members, err := a.analysisDomain.TeamLoad(ctx, normalizeProjectCode(req.ProjectCode), req.BeginTime, req.EndTime)
	if err != nil {
		return nil, errs.GrpcError(model.BusinessError)
	}

	for _, m := range members {
		level, suggest := a.aiDomain.EvalMemberLoad(int(m.WorkHour), int(m.TaskCount))
		m.LoadLevel = level
		m.Suggest = suggest
	}

	resp = analysis.TeamLoadResponse{
		Members: members,
	}

	bs, _ := marshalCache(&resp)
	randomExpire := cacheExpire + time.Duration(rand.Intn(120))*time.Second
	_ = a.cache.Put(ctx, cacheKey, string(bs), randomExpire)

	analysisLRU.Add(cacheKey, &resp)

	go func() {
		evt := event.AnalysisTeamLoadEvent{
			ProjectCode: req.ProjectCode,
			MemberCount: len(members),
			Ts:          time.Now().Unix(),
		}
		data, _ := json.Marshal(evt)
		_ = a.kafka.SendSync(ctx, kk.KKData{
			Topic: evt.Topic(),
			Data:  data,
		})
	}()

	return &resp, nil
}

func (a *AnalysisService) AIRecommend(ctx context.Context, req *analysis.AnalysisReq) (*analysis.AIRecommendResponse, error) {
	if req.TaskCode == "" {
		return nil, errs.GrpcError(model.ParamsError)
	}

	cacheKey := fmt.Sprintf("%s:airec:%s", cachePrefix, req.TaskCode)

	if val, ok := analysisLRU.Get(cacheKey); ok {
		return val.(*analysis.AIRecommendResponse), nil
	}

	var resp analysis.AIRecommendResponse
	dataStr, err := a.cache.Get(ctx, cacheKey)
	if err == nil && dataStr != "" {
		if err := protojson.Unmarshal([]byte(dataStr), &resp); err == nil {
			analysisLRU.Add(cacheKey, &resp)
			return &resp, nil
		}
	}

	mutex := locker.NewMutex(fmt.Sprintf("lock:%s", cacheKey), redsync.WithExpiry(15*time.Second))
	if err := mutex.Lock(); err != nil {
		zap.L().Warn("analysis cache lock busy, wait for cache",
			zap.String("key", cacheKey), zap.Error(err))
		for i := 0; i < 3; i++ {
			time.Sleep(150 * time.Millisecond)
			if dataStr, rErr := a.cache.Get(ctx, cacheKey); rErr == nil && dataStr != "" {
				if uErr := protojson.Unmarshal([]byte(dataStr), &resp); uErr == nil {
					analysisLRU.Add(cacheKey, &resp)
					return &resp, nil
				}
			}
		}
	} else {
		defer func() {
			_, _ = mutex.Unlock()
		}()
	}

	dataStr, err = a.cache.Get(ctx, cacheKey)
	if err == nil && dataStr != "" {
		if err := protojson.Unmarshal([]byte(dataStr), &resp); err == nil {
			analysisLRU.Add(cacheKey, &resp)
			return &resp, nil
		}
	}

	taskModel, logs, memberList, err := a.analysisDomain.TaskAIContext(ctx, req.TaskCode)
	if err != nil {
		return nil, errs.GrpcError(model.BusinessError)
	}

	analyzeRes := a.aiDomain.TaskAnalyzeWithFallback(ctx, taskModel, logs)
	if analyzeRes == nil || analyzeRes.Result == nil {
		return nil, errs.GrpcError(model.AIErr)
	}
	aiResult := analyzeRes.Result

	risk, _ := a.aiDomain.CheckTaskRisk(taskModel, aiResult.WorkHour, aiResult.RemainHour)

	var members []string
	for _, m := range memberList {
		members = append(members, strconv.Itoa(int(m.MemberCode)))
	}
	memberStr := strings.Join(members, ",")

	resp = analysis.AIRecommendResponse{
		Suggest:  aiResult.Suggest,
		Risk:     risk,
		Priority: strconv.Itoa(taskModel.Pri),
		Desc:     fmt.Sprintf("%s → 参与成员：%s", aiResult.Analysis, memberStr),
	}

	bs, _ := marshalCache(&resp)
	randomExpire := cacheExpire + time.Duration(rand.Intn(120))*time.Second
	a.cache.Put(ctx, cacheKey, string(bs), randomExpire)
	analysisLRU.Add(cacheKey, &resp)

	go func() {
		memberCodes := make([]int64, 0, len(memberList))
		for _, m := range memberList {
			memberCodes = append(memberCodes, m.MemberCode)
		}
		evt := event.AnalysisAIRecommendEvent{
			TaskCode:  req.TaskCode,
			Project:   taskModel.ProjectCode,
			Suggest:   aiResult.Suggest,
			Risk:      risk,
			Members:   memberCodes,
			CreatedAt: time.Now().Unix(),
		}
		data, _ := json.Marshal(evt)
		_ = a.kafka.SendSync(ctx, kk.KKData{
			Topic: evt.Topic(),
			Data:  data,
		})
	}()

	return &resp, nil
}

func marshalCache(msg proto.Message) ([]byte, error) {
	return protojson.Marshal(msg)
}

func New() *AnalysisService {
	return &AnalysisService{
		cache:          dao.Rc,
		transaction:    dao.NewTransaction(),
		kafka:          kk.GetWriter("127.0.0.1:9092"),
		analysisDomain: domain.NewAnalysisDomain(),
		taskDomain:     domain.NewTaskDomain(),
		userDomain:     domain.NewUserRpcDomain(),
		aiDomain:       domain.NewAIDomain(),
	}
}
