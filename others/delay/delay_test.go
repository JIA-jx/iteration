package delay

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseInt64(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want int64
	}{
		{"int64（go-redis RESP3 返回）", int64(1234), 1234},
		{"float64（RESP2 下整数会被解析成浮点）", float64(1800000000000), 1800000000000},
		{"字符串（部分场景下 Lua 返回的是字符串）", "1800000000000", 1800000000000},
		{"带小数的字符串", "1800000000000.0", 1800000000000},
		{"非法字符串", "not-a-number", 0},
		{"nil", nil, 0},
		{"未支持的类型", []byte("123"), 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseInt64(c.in); got != c.want {
				t.Fatalf("parseInt64(%v) = %d, want %d", c.in, got, c.want)
			}
		})
	}
}

func TestConfigWithDefaults(t *testing.T) {
	var c Config
	c.withDefaults()

	if c.PollInterval.String() != "1s" {
		t.Fatalf("PollInterval 默认 = %v, want 1s", c.PollInterval)
	}
	if c.Batch != 50 {
		t.Fatalf("Batch 默认 = %d, want 50", c.Batch)
	}
	if c.VisibilityTimeout.String() != "30s" {
		t.Fatalf("VisibilityTimeout 默认 = %v, want 30s", c.VisibilityTimeout)
	}
	if c.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts 默认 = %d, want 3", c.MaxAttempts)
	}
	if c.RetryDelay.String() != "1m0s" {
		t.Fatalf("RetryDelay 默认 = %v, want 1m", c.RetryDelay)
	}
	if c.RecoverBatch != 100 {
		t.Fatalf("RecoverBatch 默认 = %d, want 100", c.RecoverBatch)
	}
	if c.Workers != 8 {
		t.Fatalf("Workers 默认 = %d, want 8", c.Workers)
	}
	if c.DefaultTypeWorkers != 2 {
		t.Fatalf("DefaultTypeWorkers 默认 = %d, want 2", c.DefaultTypeWorkers)
	}
	if c.DispatchTimeout != 2*time.Second {
		t.Fatalf("DispatchTimeout 默认 = %v, want 2s", c.DispatchTimeout)
	}
}

func TestRedisStoreShardOfIsStable(t *testing.T) {
	s := &RedisStore{Shards: 4}

	ids := []string{"task_remind:1", "task_remind:2", "invite_expire:abc", "task_remind:999999"}
	for _, id := range ids {
		first := s.shardOf(id)
		if first < 0 || first >= 4 {
			t.Fatalf("shardOf(%s) = %d, 越界", id, first)
		}
		for i := 0; i < 20; i++ {
			if got := s.shardOf(id); got != first {
				t.Fatalf("shardOf(%s) 不稳定: %d != %d", id, got, first)
			}
		}
	}

	plain := &RedisStore{Shards: 1}
	if got := plain.shardOf("anything"); got != 0 {
		t.Fatalf("不分片时 shardOf = %d, want 0", got)
	}
}

func TestRedisStoreKeyLayout(t *testing.T) {
	plain := &RedisStore{Shards: 1}
	if plain.pendingKey(0) != "delay:pending" || plain.inflightKey(0) != "delay:inflight" {
		t.Fatalf("不分片 key 布局被改动: %s / %s", plain.pendingKey(0), plain.inflightKey(0))
	}

	sharded := &RedisStore{Shards: 3}
	if sharded.pendingKey(2) != "delay:pending:2" || sharded.inflightKey(1) != "delay:inflight:1" {
		t.Fatalf("分片 key 布局不符合预期: %s / %s", sharded.pendingKey(2), sharded.inflightKey(1))
	}
	if len(sharded.ownedShards()) != 3 {
		t.Fatalf("未指定 OwnShards 时应负责全部分片")
	}

	sharded.OwnShards = []int{0, 5}
	owned := sharded.ownedShards()
	if len(owned) != 1 || owned[0] != 0 {
		t.Fatalf("OwnShards 越界过滤失败: %v", owned)
	}
}

func TestConfigWithDefaultsKeepsExplicitValues(t *testing.T) {
	c := Config{
		PollInterval:      200 * 1000 * 1000,
		Batch:             5,
		VisibilityTimeout: 10 * 1000 * 1000 * 1000,
		MaxAttempts:       7,
		RetryDelay:        30 * 1000 * 1000 * 1000,
		RecoverBatch:      9,
	}
	c.withDefaults()

	if c.Batch != 5 || c.MaxAttempts != 7 || c.RecoverBatch != 9 {
		t.Fatalf("显式配置被覆盖: %+v", c)
	}
}

func TestSchedulerRegisterAndTypes(t *testing.T) {
	s := NewScheduler(&fakeStore{}, Config{})

	if len(s.RegisteredTypes()) != 0 {
		t.Fatalf("初始不应有已注册类型")
	}

	s.Register("task_remind", func(ctx context.Context, task Due) error { return nil })
	s.Register("invite_expire", func(ctx context.Context, task Due) error { return nil })

	s.Register("", func(ctx context.Context, task Due) error { return nil })
	s.Register("bad", nil)

	types := s.RegisteredTypes()
	if len(types) != 2 {
		t.Fatalf("已注册类型数 = %d, want 2 (%v)", len(types), types)
	}

	s.Register("task_remind", func(ctx context.Context, task Due) error { return nil })
	if len(s.RegisteredTypes()) != 2 {
		t.Fatalf("重复注册同一类型不应增加数量")
	}
}

func TestNewSchedulerPanicsOnNilStore(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("store 为 nil 时应当 panic")
		}
	}()
	NewScheduler(nil, Config{})
}

type fakeStore struct {
	completeCalls int64
	completeOwner string
	releaseCalls  int64
	releaseOwner  string
	failCalls     int64
	abandonCalls  int64
}

func (f *fakeStore) Enqueue(ctx context.Context, t *Task) error { return nil }
func (f *fakeStore) Cancel(ctx context.Context, id string) (bool, error) {
	return false, nil
}
func (f *fakeStore) Claim(ctx context.Context, now int64, limit int, visibilityTimeout int64) ([]Due, error) {
	return nil, nil
}
func (f *fakeStore) Complete(ctx context.Context, id string, owner string) error {
	atomic.AddInt64(&f.completeCalls, 1)
	f.completeOwner = owner
	return nil
}
func (f *fakeStore) Fail(ctx context.Context, id string, owner string, reason string, delayMs int64) error {
	atomic.AddInt64(&f.failCalls, 1)
	return nil
}
func (f *fakeStore) Release(ctx context.Context, id string, owner string) error {
	atomic.AddInt64(&f.releaseCalls, 1)
	f.releaseOwner = owner
	return nil
}
func (f *fakeStore) Abandon(ctx context.Context, id string, owner string, reason string) error {
	atomic.AddInt64(&f.abandonCalls, 1)
	return nil
}
func (f *fakeStore) Recover(ctx context.Context, now int64, limit int) (int, error) {
	return 0, nil
}
func (f *fakeStore) Pending(ctx context.Context, id string) (string, error) { return "", nil }
func (f *fakeStore) PendingCount(ctx context.Context) (int64, error)        { return 0, nil }
func (f *fakeStore) InflightCount(ctx context.Context) (int64, error)       { return 0, nil }

type ownerLostStore struct {
	fakeStore
	failCalls int64
}

func (s *ownerLostStore) Complete(ctx context.Context, id string, owner string) error {
	return ErrNotOwner
}
func (s *ownerLostStore) Fail(ctx context.Context, id string, owner string, reason string, delayMs int64) error {
	atomic.AddInt64(&s.failCalls, 1)
	return ErrNotOwner
}
func (s *ownerLostStore) Release(ctx context.Context, id string, owner string) error {
	atomic.AddInt64(&s.releaseCalls, 1)
	return ErrNotOwner
}

func TestOwnerMismatchIsNotRetriedOrRequeued(t *testing.T) {
	store := &ownerLostStore{}
	s := NewScheduler(store, Config{MaxAttempts: 5})

	executed := 0
	s.Register("task_remind", func(ctx context.Context, task Due) error {
		executed++
		return nil
	})

	s.execute(context.Background(), Due{Id: "t-1", Type: "task_remind", Owner: "owner-a", Attempts: 1}, 0)
	if executed != 1 {
		t.Fatalf("handler 执行次数 = %d, want 1", executed)
	}
	if atomic.LoadInt64(&store.failCalls) != 0 {
		t.Fatalf("owner 不匹配不应重新入队, failCalls=%d", store.failCalls)
	}

	s.execute(context.Background(), Due{Id: "t-2", Type: "task_remind", Owner: "owner-a", Attempts: 1}, 0)

	s.Register("task_remind_fail", func(ctx context.Context, task Due) error {
		return errors.New("boom")
	})
	s.execute(context.Background(), Due{Id: "t-3", Type: "task_remind_fail", Owner: "owner-a", Attempts: 1}, 0)
	if atomic.LoadInt64(&store.failCalls) != 1 {
		t.Fatalf("owner 不匹配时 Fail 至多调用一次（且不应计入重排）: %d", store.failCalls)
	}

	shutdown, cancel := context.WithCancel(context.Background())
	cancel()
	s.execute(shutdown, Due{Id: "t-4", Type: "task_remind", Owner: "owner-a", Attempts: 1}, 0)
	if executed != 2 {
		t.Fatalf("ctx 取消后不应再执行 handler, executed=%d", executed)
	}
}

func TestReleaseTaskCarriesOwner(t *testing.T) {
	store := &fakeStore{}
	s := NewScheduler(store, Config{})

	s.releaseTask(Due{Id: "t-5", Type: "task_remind", Owner: "owner-z"}, "shutdown")

	if atomic.LoadInt64(&store.releaseCalls) != 1 {
		t.Fatalf("Release 调用次数 = %d, want 1", store.releaseCalls)
	}
	if store.releaseOwner != "owner-z" {
		t.Fatalf("Release owner = %q, want owner-z", store.releaseOwner)
	}

	lost := &ownerLostStore{}
	lostScheduler := NewScheduler(lost, Config{})
	lostScheduler.releaseTask(Due{Id: "t-6", Type: "task_remind", Owner: "owner-lost"}, "shutdown")
	if atomic.LoadInt64(&lost.releaseCalls) != 1 {
		t.Fatalf("owner 不匹配时 Release 仍应被调用一次: %d", lost.releaseCalls)
	}
}
