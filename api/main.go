package main

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	_ "test.com/api/api"
	"test.com/api/api/midd"
	"test.com/api/api/midd/ratelimit"
	"test.com/api/api/project"
	"test.com/api/config"
	"test.com/api/pkg/cacheinvalidate"
	"test.com/api/pkg/ws"
	"test.com/api/router"
	"test.com/api/tracing"
	srv "test.com/others"
	"time"
)

func trustedProxies() []string {
	raw := os.Getenv("TRUSTED_PROXIES")
	if strings.TrimSpace(raw) == "" {
		return []string{"127.0.0.1/8", "::1/128", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	}
	if strings.EqualFold(strings.TrimSpace(raw), "none") {
		return nil
	}
	parts := strings.Split(raw, ",")
	list := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			list = append(list, v)
		}
	}
	if len(list) == 0 {
		return nil
	}
	return list
}

func main() {
	midd.InitJWT()
	rlm, err := ratelimit.NewRateLimitMiddleware("./config/config.yaml")
	if err != nil {
		panic("加载限流配置失败: " + err.Error())
	}
	ratelimit.SetDefault(rlm)

	r := gin.New()
	if err := r.SetTrustedProxies(trustedProxies()); err != nil {
		panic("设置可信代理失败: " + err.Error())
	}
	r.Use(gin.Logger())
	r.Use(midd.Recovery())

	r.Use(func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})

	r.Use(midd.IdentityMidd())
	r.Use(midd.RequestIdMidd())

	midd.InitPrometheusOn("9090")
	r.Use(midd.Prometheus())

	midd.StartPprof()

	tp, err := tracing.NewJaegerTraceProvider(config.C.JC.Endpoints)
	if err != nil {
		log.Fatal(err)
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))

	r.Use(rlm.UnauthMiddleware(midd.MemberIdFromToken))
	r.Use(otelgin.Middleware("api"))

	r.StaticFS("/upload", http.Dir("upload"))

	router.InitRouter(r)

	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	pusher := ws.NewPusher(config.C.KC.Brokers, fmt.Sprintf("delay_notify_ws_group_%s_%d", host, os.Getpid()))
	pushCtx, pushCancel := context.WithCancel(context.Background())
	var pushWg sync.WaitGroup
	pushWg.Add(1)
	go func() {
		defer pushWg.Done()
		pusher.Run(pushCtx)
	}()

	cacheConsumer := &cacheinvalidate.Consumer{
		Brokers: config.C.KC.Brokers,
		Group:   fmt.Sprintf("cache_invalidate_api_%s_%d", host, os.Getpid()),
		OnPurge: func(group string) {
			project.PurgeAllNodeCache()
			project.PurgeAllOrgCodeCache()
			fmt.Printf("api caches purged by invalidate message, group=%s\n", group)
		},
	}
	pushWg.Add(1)
	go func() {
		defer pushWg.Done()
		cacheConsumer.Run(pushCtx)
	}()

	stop := func() {
		pushCancel()
		pushWg.Wait()
	}

	srv.Run(r, config.C.SC.Name, config.C.SC.Addr, stop)
}
