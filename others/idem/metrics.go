package idem

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	idemExecuted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "idem_executed_total",
		Help: "idempotency guard executed operations",
	}, []string{"result"})

	idemReplayed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "idem_replayed_total",
		Help: "idempotency guard replayed stored results",
	})

	idemConflict = promauto.NewCounter(prometheus.CounterOpts{
		Name: "idem_conflict_total",
		Help: "idempotency guard rejected concurrent duplicates",
	})

	idemAuthorityFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "idem_authority_failed_total",
		Help: "idempotency authority store failures that degraded to the fast store",
	})
)
