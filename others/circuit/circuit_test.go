package circuit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	b := New(Options{Name: "llm", MaxFailures: 3, OpenTimeout: 50 * time.Millisecond})
	ctx := context.Background()

	boom := errors.New("boom")
	for i := 0; i < 3; i++ {
		if err := b.Do(ctx, func(ctx context.Context) error { return boom }); !errors.Is(err, boom) {
			t.Fatalf("第 %d 次应返回业务错误, got %v", i+1, err)
		}
	}

	if b.State() != StateOpen {
		t.Fatalf("连续失败后状态应为 open, got %v", b.State())
	}

	if err := b.Do(ctx, func(ctx context.Context) error { return nil }); !errors.Is(err, ErrOpen) {
		t.Fatalf("熔断打开时应直接拒绝, got %v", err)
	}
	if b.Counts().Rejected == 0 {
		t.Fatalf("应记录被拒绝次数")
	}
}

func TestBreakerHalfOpenProbeRecovers(t *testing.T) {
	states := make([]State, 0, 4)
	b := New(Options{
		Name:        "llm",
		MaxFailures: 1,
		OpenTimeout: 20 * time.Millisecond,
		HalfOpenMax: 1,
		OnStateChange: func(name string, from, to State) {
			states = append(states, to)
		},
	})
	ctx := context.Background()

	_ = b.Do(ctx, func(ctx context.Context) error { return errors.New("boom") })
	if b.State() != StateOpen {
		t.Fatalf("应进入 open")
	}

	time.Sleep(30 * time.Millisecond)

	if err := b.Do(ctx, func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("半开探测应放行: %v", err)
	}
	if b.State() != StateClosed {
		t.Fatalf("探测成功后应恢复 closed, got %v", b.State())
	}

	if len(states) < 2 || states[0] != StateOpen || states[len(states)-1] != StateClosed {
		t.Fatalf("状态迁移回调异常: %v", states)
	}
}

func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	b := New(Options{Name: "db", MaxFailures: 1, OpenTimeout: 10 * time.Millisecond, HalfOpenMax: 1})
	ctx := context.Background()

	_ = b.Do(ctx, func(ctx context.Context) error { return errors.New("boom") })
	time.Sleep(20 * time.Millisecond)

	_ = b.Do(ctx, func(ctx context.Context) error { return errors.New("still down") })
	if b.State() != StateOpen {
		t.Fatalf("半开探测失败后应重新 open, got %v", b.State())
	}
}

func TestBreakerLimitsHalfOpenProbes(t *testing.T) {
	b := New(Options{Name: "db", MaxFailures: 1, OpenTimeout: 10 * time.Millisecond, HalfOpenMax: 1})
	ctx := context.Background()

	_ = b.Do(ctx, func(ctx context.Context) error { return errors.New("boom") })
	time.Sleep(20 * time.Millisecond)

	release := make(chan struct{})
	go func() {
		_ = b.Do(ctx, func(ctx context.Context) error {
			<-release
			return nil
		})
	}()

	time.Sleep(10 * time.Millisecond)

	if err := b.Do(ctx, func(ctx context.Context) error { return nil }); !errors.Is(err, ErrTooManyProbes) {
		t.Fatalf("半开探测名额用尽时应拒绝, got %v", err)
	}
	close(release)
}

func TestBreakerFailureRateWindow(t *testing.T) {
	b := New(Options{
		Name:        "gateway",
		MaxFailures: 100,
		MinRequests: 10,
		FailureRate: 0.5,
		Window:      10,
	})
	ctx := context.Background()

	var fails int32
	for i := 0; i < 10; i++ {
		_ = b.Do(ctx, func(ctx context.Context) error {
			if atomic.AddInt32(&fails, 1)%2 == 0 {
				return errors.New("half fail")
			}
			return nil
		})
	}

	if b.State() != StateOpen {
		t.Fatalf("失败率达到阈值后应 open, got %v", b.State())
	}
}

func TestBreakerDoValue(t *testing.T) {
	b := New(Options{Name: "ai"})
	v, err := b.DoValue(context.Background(), func(ctx context.Context) (string, error) {
		return "ok", nil
	})
	if err != nil || v != "ok" {
		t.Fatalf("DoValue = %q, %v", v, err)
	}
}

func TestBreakerCustomSuccessPredicate(t *testing.T) {
	businessErr := errors.New("business rejected")
	b := New(Options{
		Name:         "ai",
		MaxFailures:  2,
		IsSuccessful: func(err error) bool { return err == nil || errors.Is(err, businessErr) },
	})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_ = b.Do(ctx, func(ctx context.Context) error { return businessErr })
	}
	if b.State() != StateClosed {
		t.Fatalf("被判定为成功的错误不应触发熔断, got %v", b.State())
	}
}
