package audit

import (
	"context"
	"sync"
	"testing"
	"time"
)

type stubWriter struct {
	mu     sync.Mutex
	events []*Event
	err    error
}

func (w *stubWriter) Write(ctx context.Context, events ...*Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.events = append(w.events, events...)
	return nil
}

func (w *stubWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.events)
}

func TestRecorderFlushesBatchedEvents(t *testing.T) {
	w := &stubWriter{}
	r := NewRecorder(w, RecorderOptions{Buffer: 32, Batch: 4, FlushInterval: 20 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)

	for i := 0; i < 10; i++ {
		r.Record(context.Background(), &Event{Action: "update", Resource: "task"})
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.count() >= 10 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if w.count() != 10 {
		t.Fatalf("应落库 10 条, 实际 %d", w.count())
	}
	if r.Written() != 10 {
		t.Fatalf("Written = %d, want 10", r.Written())
	}
}

func TestRecorderDropsWhenBufferFull(t *testing.T) {
	blocked := make(chan struct{})
	w := &blockingWriter{block: blocked}
	r := NewRecorder(w, RecorderOptions{Buffer: 2, Batch: 1, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)

	for i := 0; i < 50; i++ {
		r.Record(context.Background(), &Event{Action: "update"})
	}

	if r.Dropped() == 0 {
		t.Fatalf("缓冲区打满时应丢弃并计数")
	}
	close(blocked)
}

type blockingWriter struct {
	block chan struct{}
}

func (w *blockingWriter) Write(ctx context.Context, events ...*Event) error {
	<-w.block
	return nil
}

func TestRecorderFlushDrainsSynchronously(t *testing.T) {
	w := &stubWriter{}
	r := NewRecorder(w, RecorderOptions{Buffer: 64, Batch: 1000, FlushInterval: time.Hour})

	for i := 0; i < 5; i++ {
		r.Record(context.Background(), &Event{Action: "delete"})
	}

	if err := r.Flush(context.Background()); err != nil {
		t.Fatalf("Flush 出错: %v", err)
	}
	if w.count() != 5 {
		t.Fatalf("Flush 后应落库 5 条, 实际 %d", w.count())
	}
}

func TestDiffProducesJSON(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
		Pri  int    `json:"pri"`
	}
	before, after := Diff(payload{Name: "a", Pri: 1}, payload{Name: "b", Pri: 2})

	if before == "" || after == "" {
		t.Fatalf("Diff 不应返回空串: %q %q", before, after)
	}
	if before == after {
		t.Fatalf("前后快照应不同")
	}

	emptyBefore, emptyAfter := Diff(nil, nil)
	if emptyBefore != "" || emptyAfter != "" {
		t.Fatalf("nil 应序列化为空串")
	}
}
