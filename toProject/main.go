package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"test.com/others/outbox"
	"test.com/toProject/config/kafka"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	midd "test.com/api/api/midd"
	srv "test.com/others"
	"test.com/others/audit"
	"test.com/others/delay"
	"test.com/others/elk"
	"test.com/others/tenant"
	"test.com/toProject/config"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/database/gorms"
	"test.com/toProject/internal/interceptor"
	"test.com/toProject/internal/lock"
	"test.com/toProject/internal/worker"
	delay_task_v1 "test.com/toProject/pkg/service/delay_task.v1"
	evm_v1 "test.com/toProject/pkg/service/evm.v1"
	ops_v1 "test.com/toProject/pkg/service/ops.v1"
	task_service_v1 "test.com/toProject/pkg/service/task.service.v1"
	workflow_v1 "test.com/toProject/pkg/service/workflow.v1"
	"test.com/toProject/router"
	"test.com/toProject/tracing"
)

func configureSlaveDB() {
	if !config.C.DbConfig.Separation || len(config.C.DbConfig.Slave) == 0 {
		return
	}

	slave := config.C.DbConfig.Slave[0]
	if slave.Host == "" {
		return
	}

	dsn := gorms.GetDSN(slave.Username, slave.Password, slave.Host, slave.Port, slave.Db)
	gorms.ConfigureSlave(dsn)

	zap.L().Info("slave db configured",
		zap.String("host", slave.Host),
		zap.Int("port", slave.Port),
		zap.String("db", slave.Db))
}

func main() {
	r := gin.Default()
	proxies := []string{"127.0.0.1/8", "::1/128", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	if tp := strings.TrimSpace(os.Getenv("TRUSTED_PROXIES")); tp != "" {
		if strings.EqualFold(tp, "none") {
			proxies = nil
		} else {
			proxies = proxies[:0]
			for _, p := range strings.Split(tp, ",") {
				if v := strings.TrimSpace(p); v != "" {
					proxies = append(proxies, v)
				}
			}
		}
	}
	if err := r.SetTrustedProxies(proxies); err != nil {
		log.Fatalf("设置可信代理失败: %v", err)
	}
	r.Use(interceptor.TenantGinMiddleware(config.C.SC.InternalToken))
	r.Use(interceptor.InternalAuthMiddleware(srv.InternalToken(), true))
	tp, tpErr := tracing.JaegerTraceProvider(config.C.JaegerConfig.Endpoints)
	if tpErr != nil {
		log.Fatal(tpErr)
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	router.InitRouter(r)

	router.InitUserRpc()

	gc := router.RegisterGrpc()

	deregister := router.RegisterEtcdServer()

	elk.InitELK(config.GetElkConfig())

	midd.InitPrometheusOn("9091")

	writer := kafka.InitKafkaWriter()

	configureSlaveDB()

	reader := kafka.NewCacheReader()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	gorms.SetScopeExtractor(buildReadScope)
	gorms.SetReplicationLag(3 * time.Second)

	dao.InitIdem(2 * time.Minute)
	dao.InitAIUsageHook()

	auditRecorder := dao.InitAudit(audit.RecorderOptions{
		Buffer:        8192,
		Batch:         200,
		FlushInterval: time.Second,
	})
	auditRecorder.Start(ctx)

	wg.Add(1)
	go func() {
		defer wg.Done()
		reader.DeleteCache(ctx, writer)
	}()

	outboxWorker := outbox.NewWorker(outbox.NewGormRepo(gorms.GetDB()), writer, outbox.Config{
		Batch:           50,
		Interval:        time.Second,
		MaxRetry:        5,
		LockTimeout:     60 * time.Second,
		SendTimeout:     5 * time.Second,
		BaseBackoff:     time.Minute,
		MaxBackoff:      time.Hour,
		RecoverInterval: 30 * time.Second,
	})
	wg.Add(1)
	go func() {
		defer wg.Done()
		outboxWorker.Run(ctx)
	}()

	registrar := kafka.NewConsumerRegistrar(config.C.KafkaConfig.Brokers, writer)
	registrar.Start(ctx, &wg)

	delayStore := delay.NewRedisStore(dao.Rc.Rdb)
	delayStore.Shards = config.C.DelayConfig.Shards
	delayStore.OwnShards = config.C.DelayConfig.OwnShards

	delayScheduler := delay.NewScheduler(delayStore, delay.Config{
		PollInterval:       time.Second,
		Batch:              50,
		VisibilityTimeout:  30 * time.Second,
		MaxAttempts:        3,
		RetryDelay:         time.Minute,
		Workers:            16,
		DefaultTypeWorkers: 2,
		TypeWorkers: map[string]int{
			delay_task_v1.TypeTaskRemind:   4,
			delay_task_v1.TypeInviteExpire: 2,
		},
		DispatchTimeout: 2 * time.Second,
	})
	delayScheduler.SetLogger(func(format string, args ...interface{}) {
		zap.S().Infof(format, args...)
	})

	delaySvc := delay_task_v1.New(delayStore, delayScheduler, writer)
	task_service_v1.SetDelayService(delaySvc)

	delay_task_v1.NewHandler(delaySvc).Route(r)

	wg.Add(1)
	go func() {
		defer wg.Done()
		delayScheduler.Run(ctx)
	}()
	zap.L().Info("delay scheduler started",
		zap.Strings("types", delayScheduler.RegisteredTypes()))

	reconcileWorker := worker.NewReconcileWorker(
		dao.NewDelayTaskDao(),
		delayStore,
		outbox.NewGormRepo(gorms.GetDB()),
		worker.ReconcileConfig{Interval: 5 * time.Minute, Batch: 500, Repair: true},
	)
	reconcileWorker.Lock = lock.New(dao.Rc, "reconcile", 4*time.Minute)
	wg.Add(1)
	go func() {
		defer wg.Done()
		reconcileWorker.Run(ctx)
	}()

	archiveWorker := worker.NewArchiveWorker(gorms.GetDB(), worker.ArchiveConfig{
		Interval: time.Hour,
		DryRun:   false,
	})
	archiveWorker.Lock = lock.New(dao.Rc, "archive", 50*time.Minute)
	wg.Add(1)
	go func() {
		defer wg.Done()
		archiveWorker.Run(ctx)
	}()

	ops_v1.NewHandler(reconcileWorker, archiveWorker, dao.AuditReader).Route(r)

	workflowSvc := workflow_v1.New(dao.NewWorkflowStore(), dao.NewWorkflowDefinitionDao())
	workflow_v1.NewHandler(workflowSvc).Route(r)

	evm_v1.NewHandler(nil, nil).Route(r)

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := workflowSvc.HandleTimeout(ctx)
				if err != nil {
					zap.L().Warn("workflow timeout scan failed", zap.Error(err))
					continue
				}
				if n > 0 {
					zap.L().Info("workflow timeout tasks handled", zap.Int("count", n))
				}
			}
		}
	}()

	stop := func() {
		deregister()
		time.Sleep(3 * time.Second)
		cancel()
		wg.Wait()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := auditRecorder.Flush(shutdownCtx); err != nil {
			zap.L().Warn("flush audit events failed", zap.Error(err))
		}
		shutdownCancel()
		reader.R.Close()
		writer.Close()
		gc.GracefulStop()
	}
	srv.Run(r, config.C.SC.Name, config.C.SC.Addr, stop)
}

func buildReadScope(ctx context.Context) string {
	tenantId := tenant.IdFromContext(ctx)
	memberId := int64(0)

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-tenant-id"); len(v) > 0 {
			if parsed, err := strconv.ParseInt(v[0], 10, 64); err == nil && parsed > 0 {
				tenantId = parsed
			}
		}
		if v := md.Get("x-member-id"); len(v) > 0 {
			if parsed, err := strconv.ParseInt(v[0], 10, 64); err == nil {
				memberId = parsed
			}
		}
	}

	return fmt.Sprintf("t%d:m%d", tenantId, memberId)
}
