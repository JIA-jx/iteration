package main

import (
	"context"
	"log"
	"sync"
	"test.com/toUser/config/kafka"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	midd "test.com/api/api/midd"
	srv "test.com/others"
	"test.com/others/kk"
	"test.com/others/logs"
	"test.com/others/outbox"
	"test.com/toUser/config"
	"test.com/toUser/internal/database/gorms"
	"test.com/toUser/router"
	"test.com/toUser/tracing"
)

func main() {
	r := gin.New()
	r.Use(
		logs.GinLogger(),
		logs.GinRecovery(false),
	)
	gorms.InitDB()
	tp, tpErr := tracing.JaegerTraceProvider(config.C.JaegerConfig.Endpoints)
	if tpErr != nil {
		log.Fatal(tpErr)
	}

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	router.InitRouter(r)
	gc := router.RegisterGrpc()
	deregister := router.RegisterEtcdServer()
	midd.InitPrometheusOn("8083")

	kw := kk.GetWriter(config.C.KafkaConfig.Brokers[0])
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	outboxWorker := outbox.NewWorker(outbox.NewGormRepo(gorms.GetDB()), kw, outbox.Config{
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

	registrar := kafka.NewConsumerRegistrar(config.C.KafkaConfig.Brokers, kw)
	registrar.Start(ctx, &wg)

	stop := func() {
		deregister()
		time.Sleep(3 * time.Second)
		cancel()
		wg.Wait()
		kw.Close()
		gc.GracefulStop()
	}
	srv.Run(r, config.C.SC.Name, config.C.SC.Addr, stop)
}
