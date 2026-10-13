package midd

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"strconv"
	"time"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "gin_http_requests_total"},
		[]string{"path", "method", "status"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gin_http_request_duration",
			Buckets: []float64{5, 10, 20, 50, 100, 200, 500},
		},
		[]string{"path", "method"},
	)
)

func Prometheus() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		path := c.FullPath()
		method := c.Request.Method

		c.Next()

		status := strconv.Itoa(c.Writer.Status())
		duration := time.Since(start).Milliseconds()

		httpRequestsTotal.WithLabelValues(path, method, status).Inc()
		httpRequestDuration.WithLabelValues(path, method).Observe(float64(duration))
	}
}

func InitPrometheusOn(port string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() { _ = http.ListenAndServe(":"+port, mux) }()
}
