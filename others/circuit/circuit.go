package circuit

import (
	"context"
	"errors"
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

var (
	ErrOpen          = errors.New("circuit: breaker is open")
	ErrTooManyProbes = errors.New("circuit: too many half-open probes")
)

type Counts struct {
	Requests     int64
	Successes    int64
	Failures     int64
	ConsecutiveF int64
	Rejected     int64
	OpenedAt     time.Time
	StateChanges int64
}

type Options struct {
	Name          string
	Window        int
	MinRequests   int
	FailureRate   float64
	MaxFailures   int
	OpenTimeout   time.Duration
	HalfOpenMax   int
	IsSuccessful  func(err error) bool
	OnStateChange func(name string, from, to State)
}

func (o *Options) withDefaults() {
	if o.Window <= 0 {
		o.Window = 20
	}
	if o.MinRequests <= 0 {
		o.MinRequests = 10
	}
	if o.FailureRate <= 0 {
		o.FailureRate = 0.5
	}
	if o.MaxFailures <= 0 {
		o.MaxFailures = 5
	}
	if o.OpenTimeout <= 0 {
		o.OpenTimeout = 10 * time.Second
	}
	if o.HalfOpenMax <= 0 {
		o.HalfOpenMax = 1
	}
	if o.IsSuccessful == nil {
		o.IsSuccessful = func(err error) bool { return err == nil }
	}
}

type Breaker struct {
	opts Options

	mu           sync.Mutex
	state        State
	openedAt     time.Time
	halfOpenSend int

	window    []bool
	windowIdx int
	windowLen int

	counts Counts
	now    func() time.Time
}

func New(opts Options) *Breaker {
	opts.withDefaults()
	return &Breaker{
		opts:   opts,
		state:  StateClosed,
		window: make([]bool, opts.Window),
		now:    time.Now,
	}
}

func (b *Breaker) Name() string {
	return b.opts.Name
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) Counts() Counts {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.counts
	c.OpenedAt = b.openedAt
	return c
}

func (b *Breaker) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := b.beforeRequest(); err != nil {
		return err
	}
	err := fn(ctx)
	b.afterRequest(err)
	return err
}

func (b *Breaker) DoValue(ctx context.Context, fn func(ctx context.Context) (string, error)) (string, error) {
	if err := b.beforeRequest(); err != nil {
		return "", err
	}
	v, err := fn(ctx)
	b.afterRequest(err)
	return v, err
}

func (b *Breaker) beforeRequest() error {
	b.mu.Lock()

	b.counts.Requests++

	from, to, changed := b.state, b.state, false

	if b.state == StateOpen {
		if b.now().Sub(b.openedAt) < b.opts.OpenTimeout {
			b.counts.Rejected++
			b.mu.Unlock()
			return ErrOpen
		}
		from, to, changed = b.transition(StateHalfOpen)
		b.halfOpenSend = 0
	}

	if b.state == StateHalfOpen {
		if b.halfOpenSend >= b.opts.HalfOpenMax {
			b.counts.Rejected++
			b.mu.Unlock()
			if changed {
				b.notify(from, to)
			}
			return ErrTooManyProbes
		}
		b.halfOpenSend++
	}

	b.mu.Unlock()

	if changed {
		b.notify(from, to)
	}
	return nil
}

func (b *Breaker) afterRequest(err error) {
	ok := b.opts.IsSuccessful(err)

	b.mu.Lock()

	if ok {
		b.counts.Successes++
		b.counts.ConsecutiveF = 0
	} else {
		b.counts.Failures++
		b.counts.ConsecutiveF++
	}

	b.pushWindow(ok)

	from, to, changed := b.state, b.state, false

	switch b.state {
	case StateClosed:
		if b.shouldOpen() {
			from, to, changed = b.transition(StateOpen)
		}
	case StateHalfOpen:
		if !ok {
			b.counts.ConsecutiveF = b.counts.Failures
			from, to, changed = b.transition(StateOpen)
			break
		}
		if b.halfOpenSuccesses() {
			from, to, changed = b.transition(StateClosed)
		}
	}

	b.mu.Unlock()

	if changed {
		b.notify(from, to)
	}
}

func (b *Breaker) notify(from, to State) {
	if b.opts.OnStateChange != nil {
		b.opts.OnStateChange(b.opts.Name, from, to)
	}
}

func (b *Breaker) halfOpenSuccesses() bool {
	success := 0
	for i := 0; i < b.windowLen; i++ {
		if b.window[i] {
			success++
		}
	}
	return success >= b.opts.HalfOpenMax
}

func (b *Breaker) pushWindow(ok bool) {
	b.window[b.windowIdx] = ok
	b.windowIdx = (b.windowIdx + 1) % len(b.window)
	if b.windowLen < len(b.window) {
		b.windowLen++
	}
}

func (b *Breaker) resetWindow() {
	for i := range b.window {
		b.window[i] = false
	}
	b.windowIdx = 0
	b.windowLen = 0
}

func (b *Breaker) shouldOpen() bool {
	if b.counts.ConsecutiveF >= int64(b.opts.MaxFailures) {
		return true
	}
	if b.windowLen < b.opts.MinRequests {
		return false
	}
	failures := 0
	for i := 0; i < b.windowLen; i++ {
		if !b.window[i] {
			failures++
		}
	}
	rate := float64(failures) / float64(b.windowLen)
	return rate >= b.opts.FailureRate
}

func (b *Breaker) transition(to State) (State, State, bool) {
	from := b.state
	if from == to {
		return from, to, false
	}

	b.state = to
	b.counts.StateChanges++

	switch to {
	case StateOpen:
		b.openedAt = b.now()
	case StateHalfOpen:
		b.halfOpenSend = 0
		b.resetWindow()
	case StateClosed:
		b.counts.ConsecutiveF = 0
		b.resetWindow()
	}

	return from, to, true
}
