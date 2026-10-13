package idem

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGuardExecutesOnceAndReplays(t *testing.T) {
	g := NewGuard(NewMemoryStore(), time.Hour)
	ctx := context.Background()
	key := Key("task.create", "1001")

	var calls int64
	first, err := g.Do(ctx, key, func(ctx context.Context) (string, error) {
		atomic.AddInt64(&calls, 1)
		return "created:1001", nil
	})
	if err != nil {
		t.Fatalf("首次执行出错: %v", err)
	}
	if !first.Executed || first.Replayed {
		t.Fatalf("首次应为 Executed: %+v", first)
	}
	if first.Result != "created:1001" {
		t.Fatalf("结果 = %s", first.Result)
	}

	second, err := g.Do(ctx, key, func(ctx context.Context) (string, error) {
		atomic.AddInt64(&calls, 1)
		return "should-not-run", nil
	})
	if err != nil {
		t.Fatalf("重复调用出错: %v", err)
	}
	if second.Executed || !second.Replayed {
		t.Fatalf("重复调用应为 Replayed: %+v", second)
	}
	if second.Result != "created:1001" {
		t.Fatalf("重放结果 = %s, want created:1001", second.Result)
	}
	if atomic.LoadInt64(&calls) != 1 {
		t.Fatalf("业务函数被调用 %d 次, want 1", calls)
	}
}

func TestGuardReleasesOnErrorSoRetryCanRun(t *testing.T) {
	g := NewGuard(NewMemoryStore(), time.Hour)
	ctx := context.Background()
	key := Key("task.create", "2002")

	sentinel := errors.New("db down")
	if _, err := g.Do(ctx, key, func(ctx context.Context) (string, error) {
		return "", sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("应返回业务错误, got %v", err)
	}

	ok, err := g.Do(ctx, key, func(ctx context.Context) (string, error) {
		return "recovered", nil
	})
	if err != nil {
		t.Fatalf("重试应成功: %v", err)
	}
	if !ok.Executed || ok.Result != "recovered" {
		t.Fatalf("重试结果异常: %+v", ok)
	}
}

func TestGuardRejectsConcurrentDuplicate(t *testing.T) {
	g := NewGuard(NewMemoryStore(), time.Hour)
	ctx := context.Background()
	key := Key("task.create", "3003")

	release := make(chan struct{})
	started := make(chan struct{})

	go func() {
		_, _ = g.Do(ctx, key, func(ctx context.Context) (string, error) {
			close(started)
			<-release
			return "done", nil
		})
	}()

	<-started

	if _, err := g.Do(ctx, key, func(ctx context.Context) (string, error) {
		return "", nil
	}); !errors.Is(err, ErrInProgress) {
		t.Fatalf("并发重复应返回 ErrInProgress, got %v", err)
	}

	close(release)
}

func TestGuardRejectsEmptyKeyAndNilExecutor(t *testing.T) {
	g := NewGuard(NewMemoryStore(), time.Hour)
	ctx := context.Background()

	if _, err := g.Do(ctx, "", func(ctx context.Context) (string, error) { return "", nil }); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("空 key 应返回 ErrKeyEmpty, got %v", err)
	}
	if _, err := g.Do(ctx, "k", nil); err == nil {
		t.Fatalf("nil executor 应报错")
	}
}

func TestGuardOnlyOneOfManyConcurrentWinners(t *testing.T) {
	g := NewGuard(NewMemoryStore(), time.Hour)
	ctx := context.Background()
	key := Key("task.create", "4004")

	var executed int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = g.Do(ctx, key, func(ctx context.Context) (string, error) {
				atomic.AddInt64(&executed, 1)
				time.Sleep(5 * time.Millisecond)
				return "once", nil
			})
		}()
	}
	wg.Wait()

	if atomic.LoadInt64(&executed) != 1 {
		t.Fatalf("并发 50 次只应执行 1 次, 实际 %d 次", executed)
	}
}

func TestKeyAndToken(t *testing.T) {
	if got := Key("a", "b"); got != "a:b" {
		t.Fatalf("Key = %s", got)
	}
	if got := Key("", "b"); got != "b" {
		t.Fatalf("Key = %s", got)
	}
	if got := Key("a", ""); got != "a" {
		t.Fatalf("Key = %s", got)
	}
	if NewToken() == NewToken() {
		t.Fatalf("token 不应重复")
	}
	if len(NewToken()) != 32 {
		t.Fatalf("token 长度应为 32")
	}
}

type stubStore struct {
	acquireOK  bool
	acquireErr error
	rec        *Record
	completed  int
	failed     int
	released   int
}

func (s *stubStore) TryAcquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return s.acquireOK, s.acquireErr
}
func (s *stubStore) Get(ctx context.Context, key string) (*Record, error) { return s.rec, nil }
func (s *stubStore) Complete(ctx context.Context, key, result string, ttl time.Duration) error {
	s.completed++
	return nil
}
func (s *stubStore) Fail(ctx context.Context, key, reason string, ttl time.Duration) error {
	s.failed++
	return nil
}
func (s *stubStore) Release(ctx context.Context, key string) error {
	s.released++
	return nil
}

func TestTwoLevelStoreFastPathBlocks(t *testing.T) {
	fast := &stubStore{acquireOK: false}
	auth := &stubStore{acquireOK: true}

	store := NewTwoLevelStore(fast, auth)
	ok, err := store.TryAcquire(context.Background(), "k", time.Hour)
	if err != nil {
		t.Fatalf("出错: %v", err)
	}
	if ok {
		t.Fatalf("fast 层已存在时不应放行")
	}
	if auth.completed != 0 {
		t.Fatalf("fast 命中时不应触碰权威层")
	}
}

func TestTwoLevelStoreAuthorityDuplicateReleasesFast(t *testing.T) {
	fast := &stubStore{acquireOK: true}
	auth := &stubStore{acquireOK: false}

	store := NewTwoLevelStore(fast, auth)
	ok, err := store.TryAcquire(context.Background(), "k", time.Hour)
	if err != nil {
		t.Fatalf("出错: %v", err)
	}
	if ok {
		t.Fatalf("权威层重复时不应放行")
	}
	if fast.failed != 1 {
		t.Fatalf("权威层判重后应把 fast 层标记为重复, failed=%d", fast.failed)
	}
}

func TestTwoLevelStoreDegradesWhenFastFails(t *testing.T) {
	fast := &stubStore{acquireErr: errors.New("redis down")}
	auth := &stubStore{acquireOK: true}

	store := NewTwoLevelStore(fast, auth)
	ok, err := store.TryAcquire(context.Background(), "k", time.Hour)
	if err != nil {
		t.Fatalf("fast 层故障时不应把错误抛给调用方: %v", err)
	}
	if !ok {
		t.Fatalf("fast 层故障时应由权威层放行")
	}
}

func TestTwoLevelStoreNilSidesFallBack(t *testing.T) {
	only := &stubStore{acquireOK: true}
	if NewTwoLevelStore(nil, only) != Store(only) {
		t.Fatalf("fast 为 nil 时应直接返回权威层")
	}
	if NewTwoLevelStore(only, nil) != Store(only) {
		t.Fatalf("authority 为 nil 时应直接返回 fast 层")
	}
}
