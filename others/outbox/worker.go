package outbox

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"test.com/others/kk"
)

var (
	outboxSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_sent_total",
		Help: "发件箱成功投递的消息数",
	}, []string{"topic"})

	outboxRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_retried_total",
		Help: "发件箱投递失败后重新入队的次数",
	}, []string{"topic"})

	outboxExhausted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_exhausted_total",
		Help: "发件箱投递重试耗尽的次数",
	}, []string{"topic"})

	outboxRecovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_recovered_total",
		Help: "领取超时被回收重投的发件箱记录数",
	})

	outboxPendingGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_pending",
		Help: "待投递的发件箱记录数",
	})

	outboxProcessingGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_processing",
		Help: "处于投递中的发件箱记录数",
	})

	outboxExhaustedGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_exhausted",
		Help: "重试耗尽未投递的发件箱记录数",
	})
)

type Config struct {
	Batch           int
	Interval        time.Duration
	MaxRetry        int
	LockTimeout     time.Duration
	SendTimeout     time.Duration
	BaseBackoff     time.Duration
	MaxBackoff      time.Duration
	RecoverInterval time.Duration
}

func (c *Config) withDefaults() {
	if c.Batch <= 0 {
		c.Batch = 50
	}
	if c.Interval <= 0 {
		c.Interval = time.Second
	}
	if c.MaxRetry <= 0 {
		c.MaxRetry = 5
	}
	if c.LockTimeout <= 0 {
		c.LockTimeout = 60 * time.Second
	}
	if c.SendTimeout <= 0 {
		c.SendTimeout = 5 * time.Second
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = time.Minute
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = time.Hour
	}
	if c.RecoverInterval <= 0 {
		c.RecoverInterval = 30 * time.Second
	}
}

type Worker struct {
	repo       Repo
	writer     *kk.KafkaWriter
	cfg        Config
	instanceId string
	seq        uint64

	activeMu    sync.Mutex
	activeToken string
	activeBatch []*Event
}

func NewWorker(r Repo, w *kk.KafkaWriter, cfg Config) *Worker {
	cfg.withDefaults()
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown-host"
	}
	return &Worker{
		repo:       r,
		writer:     w,
		cfg:        cfg,
		instanceId: fmt.Sprintf("%s-%d-%d", host, os.Getpid(), atomic.AddUint64(&workerSeq, 1)),
	}
}

var workerSeq uint64

func (w *Worker) Run(ctx context.Context) {
	zap.L().Info("outbox worker started",
		zap.String("instance", w.instanceId),
		zap.Int("batch", w.cfg.Batch),
		zap.Duration("interval", w.cfg.Interval),
		zap.Duration("lock_timeout", w.cfg.LockTimeout),
		zap.Int("max_retry", w.cfg.MaxRetry))

	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	recoverTicker := time.NewTicker(w.cfg.RecoverInterval)
	defer recoverTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.releaseActive("worker shutdown")
			w.refreshGauges(context.Background())
			zap.L().Info("outbox worker stopped", zap.String("instance", w.instanceId))
			return
		case <-recoverTicker.C:
			w.recoverStale(ctx)
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

func (w *Worker) trackActive(lockToken string, events []*Event) {
	w.activeMu.Lock()
	defer w.activeMu.Unlock()
	w.activeToken = lockToken
	w.activeBatch = events
}

func (w *Worker) clearActive() {
	w.activeMu.Lock()
	defer w.activeMu.Unlock()
	w.activeToken = ""
	w.activeBatch = nil
}

func (w *Worker) releaseActive(reason string) {
	w.activeMu.Lock()
	events := w.activeBatch
	lockToken := w.activeToken
	w.activeBatch = nil
	w.activeToken = ""
	w.activeMu.Unlock()

	if len(events) > 0 {
		zap.L().Info("outbox releasing unfinished batch on shutdown",
			zap.String("instance", w.instanceId),
			zap.String("reason", reason),
			zap.Int("count", len(events)))
		w.releaseRemaining(events, lockToken, reason)
	}
}

func (w *Worker) processBatch(ctx context.Context) {
	lockToken := fmt.Sprintf("%s-%d", w.instanceId, atomic.AddUint64(&w.seq, 1))

	events, err := w.repo.ClaimPending(ctx, lockToken, w.cfg.Batch, w.cfg.MaxRetry)
	if err != nil {
		zap.L().Error("outbox claim pending failed", zap.Error(err))
		return
	}
	if len(events) == 0 {
		w.refreshGauges(ctx)
		return
	}

	w.processBatchWithToken(ctx, lockToken, events)
}

func (w *Worker) processBatchWithToken(ctx context.Context, lockToken string, events []*Event) {
	w.trackActive(lockToken, events)
	defer w.clearActive()

	for i, e := range events {
		select {
		case <-ctx.Done():
			zap.L().Info("outbox worker shutting down, stop sending",
				zap.Int64("unsent_id", e.Id),
				zap.Int("remaining", len(events)-i))
			w.releaseRemaining(events[i:], lockToken, "worker shutdown before send")
			return
		default:
		}

		sendCtx, cancel := context.WithTimeout(ctx, w.cfg.SendTimeout)
		sendErr := w.writer.SendSync(sendCtx, kk.KKData{
			Ctx:   sendCtx,
			Topic: e.Topic,
			Data:  e.Payload,
		})
		cancel()

		statusCtx, statusCancel := context.WithTimeout(context.Background(), 3*time.Second)

		if sendErr == nil {
			if err := w.repo.MarkSent(statusCtx, e.Id, lockToken); err != nil {
				if errors.Is(err, ErrLockLost) {
					zap.L().Warn("outbox lock lost before mark sent, skip",
						zap.Int64("id", e.Id), zap.String("topic", e.Topic))
				} else {
					zap.L().Error("outbox mark sent failed",
						zap.Int64("id", e.Id), zap.String("topic", e.Topic), zap.Error(err))
				}
			} else {
				outboxSent.WithLabelValues(e.Topic).Inc()
			}
			statusCancel()
			continue
		}

		zap.L().Warn("outbox send failed",
			zap.Int64("id", e.Id),
			zap.String("topic", e.Topic),
			zap.Int("retry_cnt", e.RetryCnt),
			zap.Error(sendErr))

		if isPermanent(sendErr) {
			outboxExhausted.WithLabelValues(e.Topic).Inc()
			if err := w.repo.MarkExhausted(statusCtx, e.Id, lockToken, sendErr.Error()); err != nil {
				if !errors.Is(err, ErrLockLost) {
					zap.L().Error("outbox mark exhausted failed", zap.Int64("id", e.Id), zap.Error(err))
				}
			}
			statusCancel()
			continue
		}

		backoff := w.backoffFor(e.RetryCnt)
		if err := w.repo.MarkRetry(statusCtx, e.Id, lockToken, w.cfg.MaxRetry, int64(backoff.Seconds()), sendErr.Error()); err != nil {
			if errors.Is(err, ErrLockLost) {
				zap.L().Warn("outbox lock lost before mark retry, skip", zap.Int64("id", e.Id))
			} else {
				zap.L().Error("outbox mark retry failed", zap.Int64("id", e.Id), zap.Error(err))
			}
		} else {
			outboxRetried.WithLabelValues(e.Topic).Inc()
		}
		statusCancel()
	}

	w.refreshGauges(ctx)
}

func (w *Worker) releaseRemaining(events []*Event, lockToken, reason string) {
	statusCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, e := range events {
		backoff := w.backoffFor(e.RetryCnt)
		err := w.repo.MarkRetry(statusCtx, e.Id, lockToken, w.cfg.MaxRetry, int64(backoff.Seconds()), reason)
		if errors.Is(err, ErrLockLost) {
			zap.L().Warn("outbox lock lost before shutdown release, skip",
				zap.Int64("id", e.Id), zap.String("topic", e.Topic))
			continue
		}
		if err != nil {
			zap.L().Error("outbox release on shutdown failed",
				zap.Int64("id", e.Id), zap.String("topic", e.Topic), zap.Error(err))
			continue
		}
		outboxRetried.WithLabelValues(e.Topic).Inc()
		zap.L().Info("outbox event released back to pending on shutdown",
			zap.Int64("id", e.Id),
			zap.String("topic", e.Topic),
			zap.Duration("backoff", backoff))
	}
}

func (w *Worker) backoffFor(retryCnt int) time.Duration {
	if retryCnt < 0 {
		retryCnt = 0
	}
	backoff := w.cfg.BaseBackoff * time.Duration(int64(1)<<uint(retryCnt))
	if backoff <= 0 || backoff > w.cfg.MaxBackoff {
		backoff = w.cfg.MaxBackoff
	}
	jitter := time.Duration(float64(backoff) * 0.3 * rand.Float64())
	return backoff + jitter
}

func (w *Worker) recoverStale(ctx context.Context) {
	n, err := w.repo.RecoverStale(ctx, int64(w.cfg.LockTimeout.Seconds()), w.cfg.MaxRetry)
	if err != nil {
		zap.L().Error("outbox recover stale failed", zap.Error(err))
		return
	}
	if n > 0 {
		outboxRecovered.Add(float64(n))
		zap.L().Warn("outbox recovered stale processing events", zap.Int64("count", n))
		w.refreshGauges(ctx)
	}
}

func (w *Worker) refreshGauges(ctx context.Context) {
	stats, err := w.repo.Stats(ctx)
	if err != nil {
		return
	}
	outboxPendingGauge.Set(float64(stats[StatusPending]))
	outboxProcessingGauge.Set(float64(stats[StatusProcessing]))
	outboxExhaustedGauge.Set(float64(stats[StatusExhausted]))
}

func isPermanent(err error) bool {
	if err == nil {
		return false
	}
	var tooLarge kafka.MessageTooLargeError
	if errors.As(err, &tooLarge) {
		return true
	}
	return false
}
