package cachekv

import (
	"context"
	"testing"

	"test.com/toProject/internal/repo"
)

type fakeCache struct {
	repo.Cache

	sets    map[string][]string
	members map[string][]string
	deleted []string
}

func newFakeCache() *fakeCache {
	return &fakeCache{
		sets:    make(map[string][]string),
		members: make(map[string][]string),
	}
}

func (f *fakeCache) SMembers(ctx context.Context, key string) ([]string, error) {
	return f.members[key], nil
}

func (f *fakeCache) Del(ctx context.Context, keys ...string) (int64, error) {
	f.deleted = append(f.deleted, keys...)
	delete(f.members, keys[0])
	return int64(len(keys)), nil
}

func TestInvalidateGroupDeletesIndexedKeys(t *testing.T) {
	cache := newFakeCache()
	cache.members[GroupIndex("task")] = []string{"cache:grpc:a", "cache:grpc:b"}

	n := InvalidateGroup(context.Background(), cache, "task")
	if n != 2 {
		t.Fatalf("应删除 2 个业务缓存 key, got %d", n)
	}

	want := map[string]bool{
		"cache:grpc:a":     true,
		"cache:grpc:b":     true,
		GroupIndex("task"): true,
	}
	for _, k := range cache.deleted {
		delete(want, k)
	}
	if len(want) != 0 {
		t.Fatalf("未删除预期的 key: %v", want)
	}
}

func TestInvalidateGroupNoIndexIsNoop(t *testing.T) {
	cache := newFakeCache()

	n := InvalidateGroup(context.Background(), cache, "task")
	if n != 0 {
		t.Fatalf("索引不存在时没有业务 key 可删, got %d", n)
	}
	if len(cache.deleted) != 1 || cache.deleted[0] != GroupIndex("task") {
		t.Fatalf("索引不存在时只应清掉空索引, got %v", cache.deleted)
	}
}

func TestInvalidateGroupGuards(t *testing.T) {
	if n := InvalidateGroup(context.Background(), nil, "task"); n != 0 {
		t.Fatalf("cache 为 nil 应返回 0")
	}
	cache := newFakeCache()
	if n := InvalidateGroup(context.Background(), cache, ""); n != 0 {
		t.Fatalf("group 为空应返回 0")
	}
	if len(cache.deleted) != 0 {
		t.Fatalf("group 为空时不应触碰 redis")
	}
}

func TestGroupIndexLayout(t *testing.T) {
	if GroupIndex("task") != "cache:idx:task" {
		t.Fatalf("索引 key 布局 = %s", GroupIndex("task"))
	}
	if GRPCPrefix != "cache:grpc:" {
		t.Fatalf("gRPC 缓存前缀被改动: %s", GRPCPrefix)
	}
}
