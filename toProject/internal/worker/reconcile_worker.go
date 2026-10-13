package worker

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"test.com/others/delay"
	"test.com/others/outbox"
	"test.com/toProject/internal/lock"
	"test.com/toProject/internal/repo"
)

var (
	reconcileMissingGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "reconcile_delay_missing_in_queue",
		Help: "delay ledger rows that have no corresponding entry in the redis queue",
	})
	reconcileDriftGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "reconcile_delay_status_drift",
		Help: "delay ledger rows whose status disagrees with redis",
	})
	reconcileLedgerGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "reconcile_delay_ledger_pending",
		Help: "delay ledger pending rows observed in the last run",
	})
	reconcileQueueGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "reconcile_delay_queue_pending",
		Help: "redis delay queue pending tasks observed in the last run",
	})
	reconcileRepaired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "reconcile_delay_repaired_total",
		Help: "delay tasks re-enqueued by the reconciliation job",
	})
	reconcileRuns = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reconcile_runs_total",
		Help: "reconciliation job runs",
	}, []string{"result"})
)

type ReconcileReport struct {
	Checked         int
	MissingInQueue  int
	MissingExpired  int
	StatusDrift     int
	Repaired        int
	LedgerPending   int64
	QueuePending    int64
	OutboxExhausted int64
	OutboxPending   int64
	OutboxStuck     int64
	Duration        time.Duration
}

type ReconcileConfig struct {
	Interval time.Duration
	Batch    int
	Repair   bool
}

func (c *ReconcileConfig) withDefaults() {
	if c.Interval <= 0 {
		c.Interval = 5 * time.Minute
	}
	if c.Batch <= 0 {
		c.Batch = 500
	}
}

type ReconcileWorker struct {
	ledger     repo.DelayTaskRepo
	delayStore *delay.RedisStore
	outboxRepo outbox.Repo
	cfg        ReconcileConfig
	Lock       *lock.RedisLock

	lastReport ReconcileReport
}

func NewReconcileWorker(ledger repo.DelayTaskRepo, store *delay.RedisStore, outboxRepo outbox.Repo, cfg ReconcileConfig) *ReconcileWorker {
	cfg.withDefaults()
	return &ReconcileWorker{
		ledger:     ledger,
		delayStore: store,
		outboxRepo: outboxRepo,
		cfg:        cfg,
	}
}

func (w *ReconcileWorker) LastReport() ReconcileReport {
	return w.lastReport
}

func (w *ReconcileWorker) Run(ctx context.Context) {
	zap.L().Info("reconcile worker started",
		zap.Duration("interval", w.cfg.Interval),
		zap.Int("batch", w.cfg.Batch),
		zap.Bool("repair", w.cfg.Repair))

	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			zap.L().Info("reconcile worker stopped")
			return
		case <-ticker.C:
			w.runOnce(ctx)
		}
	}
}

func (w *ReconcileWorker) runOnce(ctx context.Context) {
	if w.Lock != nil {
		ok, err := w.Lock.TryAcquire(ctx)
		if err != nil {
			zap.L().Warn("reconcile lock acquire failed, run anyway", zap.Error(err))
		} else if !ok {
			reconcileRuns.WithLabelValues("skipped_locked").Inc()
			return
		} else {
			defer w.Lock.Release(ctx)
		}
	}

	report, err := w.Reconcile(ctx)
	if err != nil {
		reconcileRuns.WithLabelValues("error").Inc()
		zap.L().Error("reconcile run failed", zap.Error(err))
		return
	}

	reconcileRuns.WithLabelValues("ok").Inc()
	if report.MissingInQueue > 0 || report.StatusDrift > 0 || report.OutboxExhausted > 0 {
		zap.L().Warn("reconcile found inconsistencies",
			zap.Int("missing_in_queue", report.MissingInQueue),
			zap.Int("status_drift", report.StatusDrift),
			zap.Int("repaired", report.Repaired),
			zap.Int64("outbox_exhausted", report.OutboxExhausted))
	}
}

func (w *ReconcileWorker) Reconcile(ctx context.Context) (ReconcileReport, error) {
	start := time.Now()
	report := ReconcileReport{}

	if w.delayStore == nil && w.outboxRepo == nil {
		return report, nil
	}

	if w.delayStore != nil {
		if err := w.reconcileDelay(ctx, &report); err != nil {
			w.lastReport = report
			return report, err
		}
	}

	if w.outboxRepo != nil {
		stats, err := w.outboxRepo.Stats(ctx)
		if err != nil {
			w.lastReport = report
			return report, err
		}
		report.OutboxPending = stats[outbox.StatusPending]
		report.OutboxExhausted = stats[outbox.StatusExhausted]
		report.OutboxStuck = stats[outbox.StatusProcessing]
	}

	report.Duration = time.Since(start)
	w.lastReport = report
	return report, nil
}

func (w *ReconcileWorker) reconcileDelay(ctx context.Context, report *ReconcileReport) error {
	rows, total, err := w.ledger.List(ctx, "", delay.StatusPending, 1, int64(w.cfg.Batch))
	if err != nil {
		return err
	}
	for page := int64(2); len(rows) < w.cfg.Batch && len(rows) < int(total); page++ {
		more, _, mErr := w.ledger.List(ctx, "", delay.StatusPending, page, 200)
		if mErr != nil || len(more) == 0 {
			break
		}
		rows = append(rows, more...)
	}

	report.LedgerPending = total

	queuePending, err := w.delayStore.PendingCount(ctx)
	if err == nil {
		report.QueuePending = queuePending
	}

	for _, row := range rows {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		report.Checked++

		inQueue, qErr := w.delayStore.InQueue(ctx, row.TaskId)
		if qErr != nil {
			return qErr
		}

		state, sErr := w.delayStore.Pending(ctx, row.TaskId)
		if sErr != nil {
			return sErr
		}

		if state == delay.StatusInflight {
			continue
		}

		if state != "" && state != delay.StatusPending {
			report.StatusDrift++
			if w.cfg.Repair {
				_ = w.ledger.UpdateStatus(ctx, row.TaskId, state, row.LastError, row.Attempts)
			}
			continue
		}

		if !inQueue {
			inflight, iErr := w.delayStore.Inflight(ctx, row.TaskId)
			if iErr == nil && inflight {
				continue
			}

			if row.RunAt <= time.Now().UnixMilli() {
				report.MissingExpired++
				continue
			}

			report.MissingInQueue++
			if !w.cfg.Repair {
				continue
			}

			if err := w.delayStore.Enqueue(ctx, &delay.Task{
				Id:      row.TaskId,
				Type:    row.Type,
				Payload: []byte(row.Payload),
				RunAt:   row.RunAt,
			}); err != nil {
				zap.L().Warn("reconcile re-enqueue failed",
					zap.String("task_id", row.TaskId), zap.Error(err))
				continue
			}
			reconcileRepaired.Inc()
			report.Repaired++
			zap.L().Info("reconcile re-enqueued missing delay task",
				zap.String("task_id", row.TaskId), zap.Int64("run_at", row.RunAt))
		}
	}

	reconcileMissingGauge.Set(float64(report.MissingInQueue))
	reconcileDriftGauge.Set(float64(report.StatusDrift))
	reconcileLedgerGauge.Set(float64(report.LedgerPending))
	reconcileQueueGauge.Set(float64(report.QueuePending))
	return nil
}
