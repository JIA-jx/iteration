package delay

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	delayExecuted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "delay_task_executed_total",
			Help: "延时任务成功执行次数",
		},
		[]string{"type"},
	)
	delayRetried = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "delay_task_retried_total",
			Help: "延时任务失败后重新入队次数",
		},
		[]string{"type"},
	)
	delayAbandoned = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "delay_task_abandoned_total",
			Help: "延时任务重试耗尽被放弃次数",
		},
		[]string{"type"},
	)
	delayRecovered = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "delay_task_recovered_total",
			Help: "处理超时被重新入队的延时任务数",
		},
	)
	delayNoHandler = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "delay_task_no_handler_total",
			Help: "没有注册处理器的延时任务数（配置错误，应告警）",
		},
		[]string{"type"},
	)
	delayPendingGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "delay_task_pending",
			Help: "待触发的延时任务数",
		},
	)

	delayInflightGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "delay_task_inflight",
			Help: "调度器正在执行的延时任务数",
		},
	)

	delayReleased = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "delay_task_released_total",
			Help: "因背压放回队列（未消耗重试次数）的延时任务数",
		},
		[]string{"reason"},
	)
)

type Config struct {
	PollInterval time.Duration

	Batch int

	VisibilityTimeout time.Duration

	MaxAttempts int

	RetryDelay time.Duration

	RecoverBatch int

	Workers int

	DefaultTypeWorkers int

	TypeWorkers map[string]int

	DispatchTimeout time.Duration
}

func (c *Config) withDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.Batch <= 0 {
		c.Batch = 50
	}
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 30 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = time.Minute
	}
	if c.RecoverBatch <= 0 {
		c.RecoverBatch = 100
	}
	if c.Workers <= 0 {
		c.Workers = 8
	}
	if c.DefaultTypeWorkers <= 0 {
		c.DefaultTypeWorkers = 2
	}
	if c.DispatchTimeout <= 0 {
		c.DispatchTimeout = 2 * time.Second
	}
}

type Scheduler struct {
	store Store
	cfg   Config
	logf  func(format string, args ...interface{})

	mu       sync.RWMutex
	handlers map[TaskType]Handler

	tasks chan Due

	semMu sync.Mutex
	sems  map[TaskType]chan struct{}

	wg       sync.WaitGroup
	inflight int64
	started  int32
}

func NewScheduler(store Store, cfg Config) *Scheduler {
	if store == nil {
		panic("delay.NewScheduler: store is nil")
	}
	cfg.withDefaults()
	return &Scheduler{
		store:    store,
		cfg:      cfg,
		logf:     func(string, ...interface{}) {},
		handlers: make(map[TaskType]Handler),
		tasks:    make(chan Due, cfg.Workers*2),
		sems:     make(map[TaskType]chan struct{}),
	}
}

func (s *Scheduler) SetLogger(logf func(format string, args ...interface{})) {
	if logf != nil {
		s.logf = logf
	}
}

func (s *Scheduler) Register(taskType string, h Handler) {
	if taskType == "" || h == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[TaskType(taskType)] = h
}

func (s *Scheduler) RegisteredTypes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	types := make([]string, 0, len(s.handlers))
	for t := range s.handlers {
		types = append(types, string(t))
	}
	return types
}

func (s *Scheduler) Run(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		s.logf("[delay] scheduler already consumed, ignore duplicate Run")
		return
	}

	s.logf("[delay] scheduler started, poll=%s batch=%d visTimeout=%s maxAttempts=%d workers=%d typeWorkers=%d types=%v",
		s.cfg.PollInterval, s.cfg.Batch, s.cfg.VisibilityTimeout, s.cfg.MaxAttempts,
		s.cfg.Workers, s.cfg.DefaultTypeWorkers, s.RegisteredTypes())

	for i := 0; i < s.cfg.Workers; i++ {
		s.wg.Add(1)
		go s.worker(ctx, i)
	}

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():

			s.logf("[delay] scheduler stopping, draining %d queued task(s)", len(s.tasks))
			close(s.tasks)
			for task := range s.tasks {
				if err := s.store.Release(context.Background(), task.Id, task.Owner); err != nil && !errors.Is(err, ErrNotOwner) {
					s.logf("[delay] release on shutdown failed id=%s: %v", task.Id, err)
				}
				delayReleased.WithLabelValues("shutdown").Inc()
			}
			s.wg.Wait()
			s.logf("[delay] scheduler stopped")
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	now := time.Now().UnixMilli()

	if n, err := s.store.Recover(ctx, now, s.cfg.RecoverBatch); err != nil {
		s.logf("[delay] recover failed: %v", err)
	} else if n > 0 {
		delayRecovered.Add(float64(n))
		s.logf("[delay] recovered %d stale inflight task(s)", n)
	}

	free := cap(s.tasks) - len(s.tasks)
	if free <= 0 {
		delayReleased.WithLabelValues("queue_full").Inc()
		s.refreshGauge(ctx)
		return
	}

	limit := s.cfg.Batch
	if limit > free {
		limit = free
	}

	dues, err := s.store.Claim(ctx, now, limit, s.cfg.VisibilityTimeout.Milliseconds())
	if err != nil {
		s.logf("[delay] claim failed: %v", err)
		return
	}
	if len(dues) == 0 {
		s.refreshGauge(ctx)
		return
	}

	for _, task := range dues {
		select {
		case s.tasks <- task:
		default:
			delayReleased.WithLabelValues("queue_full").Inc()
			if err := s.store.Release(context.Background(), task.Id, task.Owner); err != nil && !errors.Is(err, ErrNotOwner) {
				s.logf("[delay] release failed id=%s: %v", task.Id, err)
			}
		}
	}

	s.refreshGauge(ctx)
}

func (s *Scheduler) worker(ctx context.Context, idx int) {
	defer s.wg.Done()

	for task := range s.tasks {

		select {
		case <-ctx.Done():
			s.releaseTask(task, "shutdown")
			continue
		default:
		}
		s.execute(ctx, task, idx)
	}
}

func (s *Scheduler) releaseTask(task Due, reason string) {
	err := s.store.Release(context.Background(), task.Id, task.Owner)
	if errors.Is(err, ErrNotOwner) {
		s.logf("[delay] id=%s already reclaimed by visibility timeout, skip release", task.Id)
		return
	}
	if err != nil {
		s.logf("[delay] release failed id=%s: %v", task.Id, err)
	}
	delayReleased.WithLabelValues(reason).Inc()
}

func (s *Scheduler) semFor(t TaskType) chan struct{} {
	s.semMu.Lock()
	defer s.semMu.Unlock()

	if c, ok := s.sems[t]; ok {
		return c
	}

	n := s.cfg.DefaultTypeWorkers
	if v, ok := s.cfg.TypeWorkers[string(t)]; ok && v > 0 {
		n = v
	}
	c := make(chan struct{}, n)
	s.sems[t] = c
	return c
}

func (s *Scheduler) execute(ctx context.Context, task Due, workerIdx int) {
	s.mu.RLock()
	handler, ok := s.handlers[TaskType(task.Type)]
	s.mu.RUnlock()

	if !ok {

		delayNoHandler.WithLabelValues(task.Type).Inc()
		s.logf("[delay] no handler registered for type=%s id=%s, abandon", task.Type, task.Id)
		if err := s.store.Abandon(ctx, task.Id, task.Owner, fmt.Sprintf("no handler for type %s", task.Type)); err != nil {
			s.logStoreMiss(task, "abandon", err)
		}
		return
	}

	sem := s.semFor(TaskType(task.Type))
	select {
	case sem <- struct{}{}:
	case <-time.After(s.cfg.DispatchTimeout):
		s.logf("[delay] type=%s worker=%d saturated, release id=%s", task.Type, workerIdx, task.Id)
		s.releaseTask(task, "type_saturated")
		return
	case <-ctx.Done():
		s.releaseTask(task, "shutdown")
		return
	}
	defer func() { <-sem }()

	if ctx.Err() != nil {
		s.releaseTask(task, "shutdown")
		return
	}

	atomic.AddInt64(&s.inflight, 1)
	delayInflightGauge.Set(float64(atomic.LoadInt64(&s.inflight)))
	defer func() {
		atomic.AddInt64(&s.inflight, -1)
		delayInflightGauge.Set(float64(atomic.LoadInt64(&s.inflight)))
	}()

	err := s.runHandler(ctx, handler, task)

	if ctx.Err() != nil {
		s.releaseTask(task, "shutdown")
		return
	}

	if err == nil {
		if cErr := s.store.Complete(context.Background(), task.Id, task.Owner); cErr != nil {
			if errors.Is(cErr, ErrNotOwner) {
				s.logf("[delay] task result discarded, id=%s already reclaimed by visibility timeout", task.Id)
			} else {
				s.logf("[delay] complete failed id=%s: %v", task.Id, cErr)
			}
		}
		delayExecuted.WithLabelValues(task.Type).Inc()
		s.logf("[delay] task done type=%s id=%s attempts=%d", task.Type, task.Id, task.Attempts)
		return
	}

	if task.Attempts >= s.cfg.MaxAttempts {
		delayAbandoned.WithLabelValues(task.Type).Inc()
		s.logf("[delay] task abandoned (attempts=%d/%d) type=%s id=%s err=%v",
			task.Attempts, s.cfg.MaxAttempts, task.Type, task.Id, err)
		if aErr := s.store.Abandon(context.Background(), task.Id, task.Owner, truncateErr(err)); aErr != nil {
			s.logStoreMiss(task, "abandon", aErr)
		}
		return
	}

	backoff := s.cfg.RetryDelay * time.Duration(int64(1)<<uint(task.Attempts-1))
	jitter := time.Duration(float64(backoff) * 0.3 * rand.Float64())
	if fErr := s.store.Fail(context.Background(), task.Id, task.Owner, truncateErr(err), (backoff + jitter).Milliseconds()); fErr != nil {
		if errors.Is(fErr, ErrNotOwner) {
			s.logf("[delay] task requeue skipped, id=%s already reclaimed and requeued by visibility timeout", task.Id)
			return
		}
		s.logf("[delay] task requeue failed id=%s: %v", task.Id, fErr)
		return
	}
	delayRetried.WithLabelValues(task.Type).Inc()
	s.logf("[delay] task failed, retry in %s type=%s id=%s attempts=%d err=%v",
		backoff+jitter, task.Type, task.Id, task.Attempts, err)
}

func (s *Scheduler) logStoreMiss(task Due, op string, err error) {
	if errors.Is(err, ErrNotOwner) {
		s.logf("[delay] %s skipped, id=%s already reclaimed by visibility timeout", op, task.Id)
		return
	}
	s.logf("[delay] %s failed id=%s: %v", op, task.Id, err)
}

func (s *Scheduler) runHandler(ctx context.Context, h Handler, task Due) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
			s.logf("[delay] handler panic type=%s id=%s: %v", task.Type, task.Id, r)
		}
	}()

	runCtx, cancel := context.WithTimeout(ctx, s.cfg.VisibilityTimeout*9/10)
	defer cancel()

	return h(runCtx, task)
}

func (s *Scheduler) refreshGauge(ctx context.Context) {
	if n, err := s.store.PendingCount(ctx); err == nil {
		delayPendingGauge.Set(float64(n))
	}
}

func truncateErr(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(truncateRunes(err.Error(), 400))
}

func truncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	return string([]rune(s)[:maxRunes]) + "...(truncated)"
}
