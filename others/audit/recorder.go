package audit

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"test.com/others/tenant"
)

var (
	auditWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "audit_event_written_total",
		Help: "audit events persisted",
	})
	auditDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "audit_event_dropped_total",
		Help: "audit events dropped because the buffer was full",
	})
	auditFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "audit_event_write_failed_total",
		Help: "audit batch write failures",
	})
)

var TraceIDFromContext = func(ctx context.Context) string { return "" }

type Recorder struct {
	writer        Writer
	ch            chan *Event
	batch         int
	flushInterval time.Duration
	dropped       int64
	written       int64
	flushMu       sync.Mutex
	flushReq      chan chan struct{}
	loopDone      chan struct{}
	wg            sync.WaitGroup
	started       int32
}

type RecorderOptions struct {
	Buffer        int
	Batch         int
	FlushInterval time.Duration
}

func NewRecorder(w Writer, opts RecorderOptions) *Recorder {
	if opts.Buffer <= 0 {
		opts.Buffer = 4096
	}
	if opts.Batch <= 0 {
		opts.Batch = 100
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = time.Second
	}
	return &Recorder{
		writer:        w,
		ch:            make(chan *Event, opts.Buffer),
		batch:         opts.Batch,
		flushInterval: opts.FlushInterval,
		flushReq:      make(chan chan struct{}),
		loopDone:      make(chan struct{}),
	}
}

func (r *Recorder) Start(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&r.started, 0, 1) {
		return
	}
	r.wg.Add(1)
	go r.loop(ctx)
}

func (r *Recorder) loop(ctx context.Context) {
	defer r.wg.Done()
	defer close(r.loopDone)

	ticker := time.NewTicker(r.flushInterval)
	defer ticker.Stop()

	buf := make([]*Event, 0, r.batch)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		r.write(buf)
		buf = buf[:0]
	}

	drain := func() {
		for {
			select {
			case e := <-r.ch:
				buf = append(buf, e)
				if len(buf) >= r.batch {
					flush()
				}
			default:
				flush()
				return
			}
		}
	}

	for {
		stopped := false
		var ack chan struct{}
		select {
		case <-ctx.Done():
			stopped = true
		case e := <-r.ch:
			r.flushMu.Lock()
			buf = append(buf, e)
			if len(buf) >= r.batch {
				flush()
			}
			r.flushMu.Unlock()
		case ack = <-r.flushReq:
			r.flushMu.Lock()
			drain()
			r.flushMu.Unlock()
		case <-ticker.C:
			r.flushMu.Lock()
			flush()
			r.flushMu.Unlock()
		}

		if stopped {
			r.flushMu.Lock()
			drain()
			r.flushMu.Unlock()
		}

		if ack != nil {
			close(ack)
		}
		if stopped {
			return
		}
	}
}

func (r *Recorder) write(buf []*Event) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.writer.Write(ctx, buf...); err != nil {
		auditFailed.Inc()
		zap.L().Error("audit batch write failed", zap.Int("count", len(buf)), zap.Error(err))
		return
	}
	auditWritten.Add(float64(len(buf)))
	atomic.AddInt64(&r.written, int64(len(buf)))
}

func (r *Recorder) Record(ctx context.Context, e *Event) {
	if e == nil || r.writer == nil {
		return
	}

	if e.TenantId <= 0 {
		e.TenantId = tenant.IdFromContext(ctx)
	}
	if e.TraceId == "" {
		e.TraceId = TraceIDFromContext(ctx)
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}

	select {
	case r.ch <- e:
		return
	default:
	}

	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case r.ch <- e:
		return
	case <-timer.C:
		auditDropped.Inc()
		atomic.AddInt64(&r.dropped, 1)
		zap.L().Warn("audit buffer full, event dropped",
			zap.String("action", e.Action),
			zap.String("resource", e.Resource),
			zap.String("resource_id", e.ResourceId))
	}
}

func (r *Recorder) Dropped() int64 {
	return atomic.LoadInt64(&r.dropped)
}

func (r *Recorder) Written() int64 {
	return atomic.LoadInt64(&r.written)
}

func (r *Recorder) Flush(ctx context.Context) error {
	if atomic.LoadInt32(&r.started) == 0 {
		return r.flushDirect(ctx)
	}

	ack := make(chan struct{})
	select {
	case r.flushReq <- ack:
	case <-r.loopDone:
		return r.flushDirect(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Recorder) flushDirect(ctx context.Context) error {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()

	buf := make([]*Event, 0, r.batch)
	for {
		select {
		case e := <-r.ch:
			buf = append(buf, e)
			if len(buf) >= r.batch {
				if err := r.writer.Write(ctx, buf...); err != nil {
					return err
				}
				buf = buf[:0]
			}
		default:
			if len(buf) == 0 {
				return nil
			}
			return r.writer.Write(ctx, buf...)
		}
	}
}
