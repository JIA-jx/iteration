package llm

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	llmCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "llm_calls_total",
		Help: "llm api calls",
	}, []string{"model", "result"})

	llmTokens = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "llm_tokens_total",
		Help: "llm tokens consumed",
	}, []string{"model", "kind"})

	llmCost = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "llm_cost_total",
		Help: "estimated llm cost in currency units",
	}, []string{"model"})

	llmLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "llm_latency_seconds",
		Help:    "llm call latency",
		Buckets: []float64{0.2, 0.5, 1, 2, 5, 10, 20, 30},
	}, []string{"model"})

	llmRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "llm_retries_total",
		Help: "llm call retries",
	}, []string{"model", "reason"})

	llmCache = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "llm_cache_total",
		Help: "llm result cache hits and misses",
	}, []string{"result"})

	llmRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "llm_rejected_total",
		Help: "llm calls rejected before hitting the network",
	}, []string{"reason"})
)
