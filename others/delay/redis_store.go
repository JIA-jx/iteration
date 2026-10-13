package delay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var (
	delayBodyMissing = promauto.NewCounter(prometheus.CounterOpts{
		Name: "delay_task_body_missing_total",
		Help: "claimed delay tasks whose body key had already expired",
	})

	delayOwnerLost = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "delay_task_owner_lost_total",
		Help: "延时任务 inflight 认领已不属于当前 owner 的次数（可见性超时回收后旧 owner 的写回被拒绝）",
	}, []string{"op"})
)

var ErrNotOwner = errors.New("delay: inflight ownership lost")

var ownerSeq uint64

const (
	keyPending     = "delay:pending"
	keyInflight    = "delay:inflight"
	keyTaskPrefix  = "delay:task:"
	defaultTaskTTL = 7 * 24 * time.Hour
)

const claimScript = `
local pendingKey  = KEYS[1]
local inflightKey = KEYS[2]
local now         = tonumber(ARGV[1])
local limit       = tonumber(ARGV[2])
local visTimeout  = tonumber(ARGV[3])
local owner       = ARGV[4]

local due = redis.call('ZRANGEBYSCORE', pendingKey, '-inf', now, 'LIMIT', 0, limit)
if #due == 0 then
  return {}
end

local result = {}
for i = 1, #due do
  local id    = due[i]
  local score = redis.call('ZSCORE', pendingKey, id)
  redis.call('ZREM', pendingKey, id)
  redis.call('HSET', inflightKey, id, owner .. ':' .. tostring(now + visTimeout))
  result[#result + 1] = id
  result[#result + 1] = score
  result[#result + 1] = owner
end
return result
`

const recoverScript = `
local pendingKey  = KEYS[1]
local inflightKey = KEYS[2]
local now         = tonumber(ARGV[1])
local limit       = tonumber(ARGV[2])

local stale = redis.call('HGETALL', inflightKey)
if #stale == 0 then
  return {}
end

local moved = {}
local idx = 1
while idx < #stale and #moved < limit do
  local id  = stale[idx]
  local raw = stale[idx + 1]
  idx = idx + 2

  local visible = tonumber(raw)
  local sep = string.find(raw, ':', 1, true)
  if sep ~= nil then
    visible = tonumber(string.sub(raw, sep + 1))
  end

  if visible ~= nil and visible <= now then
    redis.call('HDEL', inflightKey, id)
    redis.call('ZADD', pendingKey, now, id)
    moved[#moved + 1] = id
  end
end
return moved
`

const delIfOwnerScript = `
local key = KEYS[1]
local id  = ARGV[1]
local owner = ARGV[2]
local newValue = ARGV[3]

local raw = redis.call('HGET', key, id)
if raw == false then
  return 0
end

if string.sub(raw, 1, #owner + 1) ~= owner .. ':' then
  return -1
end

if newValue == '' then
  redis.call('HDEL', key, id)
else
  redis.call('HSET', key, id, newValue)
end
return 1
`

type RedisStore struct {
	rdb *redis.Client
	TTL time.Duration

	Shards int

	OwnShards []int
}

func NewRedisStore(rdb *redis.Client) *RedisStore {
	if rdb == nil {
		panic("delay.NewRedisStore: rdb is nil")
	}
	return &RedisStore{rdb: rdb, TTL: defaultTaskTTL, Shards: 1}
}

func (s *RedisStore) taskKey(id string) string { return keyTaskPrefix + id }

func (s *RedisStore) ttl() time.Duration {
	if s.TTL <= 0 {
		return defaultTaskTTL
	}
	return s.TTL
}

func (s *RedisStore) shardCount() int {
	if s.Shards <= 1 {
		return 1
	}
	return s.Shards
}

func (s *RedisStore) shardOf(id string) int {
	if s.shardCount() == 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(s.shardCount()))
}

func (s *RedisStore) pendingKey(shard int) string {
	if s.shardCount() == 1 {
		return keyPending
	}
	return fmt.Sprintf("%s:%d", keyPending, shard)
}

func (s *RedisStore) inflightKey(shard int) string {
	if s.shardCount() == 1 {
		return keyInflight
	}
	return fmt.Sprintf("%s:%d", keyInflight, shard)
}

func (s *RedisStore) ownedShards() []int {
	if s.shardCount() == 1 {
		return []int{0}
	}
	if len(s.OwnShards) == 0 {
		all := make([]int, 0, s.shardCount())
		for i := 0; i < s.shardCount(); i++ {
			all = append(all, i)
		}
		return all
	}
	valid := make([]int, 0, len(s.OwnShards))
	for _, idx := range s.OwnShards {
		if idx >= 0 && idx < s.shardCount() {
			valid = append(valid, idx)
		}
	}
	if len(valid) == 0 {
		return []int{0}
	}
	return valid
}

func (s *RedisStore) Enqueue(ctx context.Context, t *Task) error {
	if t == nil || t.Id == "" {
		return errors.New("delay: task or task.Id is empty")
	}
	if t.Type == "" {
		return errors.New("delay: task.Type is empty")
	}
	if t.RunAt <= 0 {
		return errors.New("delay: task.RunAt must be a millisecond timestamp")
	}

	now := time.Now().UnixMilli()
	t.CreatedAt = now
	t.UpdatedAt = now
	t.Status = StatusPending

	body, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("delay: marshal task failed: %w", err)
	}

	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, s.taskKey(t.Id), body, s.ttlFor(t.RunAt, now))
	pipe.ZAdd(ctx, s.pendingKey(s.shardOf(t.Id)), &redis.Z{Score: float64(t.RunAt), Member: t.Id})
	_, err = pipe.Exec(ctx)
	return err
}

func (s *RedisStore) ttlFor(runAt, nowMs int64) time.Duration {
	ttl := s.ttl()
	if ahead := time.Duration(runAt-nowMs) * time.Millisecond; ahead > 0 {
		ttl += ahead
	}
	return ttl
}

func (s *RedisStore) Cancel(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, errors.New("delay: id is empty")
	}

	removed, err := s.rdb.ZRem(ctx, s.pendingKey(s.shardOf(id)), id).Result()
	if err != nil {
		return false, err
	}
	if removed == 0 {

		return false, nil
	}

	if err := s.updateStatus(ctx, id, StatusCanceled, ""); err != nil {

		return true, err
	}
	return true, nil
}

func (s *RedisStore) Claim(ctx context.Context, now int64, limit int, visibilityTimeout int64) ([]Due, error) {
	if limit <= 0 {
		limit = 20
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30000
	}

	dues := make([]Due, 0, limit)
	for _, shard := range s.ownedShards() {
		if len(dues) >= limit {
			break
		}
		shardDues, err := s.claimShard(ctx, shard, now, limit-len(dues), visibilityTimeout)
		if err != nil {

			if len(dues) > 0 {
				return dues, nil
			}
			return nil, err
		}
		dues = append(dues, shardDues...)
	}
	return dues, nil
}

func (s *RedisStore) claimShard(ctx context.Context, shard int, now int64, limit int, visibilityTimeout int64) ([]Due, error) {
	owner := nextOwner()
	raw, err := s.rdb.Eval(ctx, claimScript,
		[]string{s.pendingKey(shard), s.inflightKey(shard)},
		now, limit, visibilityTimeout, owner).Result()
	if err != nil {

		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	items, ok := raw.([]interface{})
	if !ok || len(items) == 0 {
		return nil, nil
	}

	dues := make([]Due, 0, len(items)/3)
	for i := 0; i+2 < len(items); i += 3 {
		id, _ := items[i].(string)
		if id == "" {
			continue
		}
		runAt := parseInt64(items[i+1])
		taskOwner, _ := items[i+2].(string)
		if taskOwner == "" {
			taskOwner = owner
		}

		t, err := s.loadAndBumpAttempts(ctx, id)
		if err != nil || t == nil {
			if errors.Is(err, redis.Nil) {
				delayBodyMissing.Inc()
				_ = s.dropInflight(ctx, shard, id, taskOwner)
			} else {
				zap.L().Warn("delay task body load failed, inflight dropped for retry",
					zap.String("id", id), zap.Int("shard", shard), zap.Error(err))
				_ = s.dropInflight(ctx, shard, id, taskOwner)
			}
			continue
		}
		dues = append(dues, Due{
			Id:       t.Id,
			Type:     t.Type,
			Payload:  t.Payload,
			RunAt:    runAt,
			Attempts: t.Attempts,
			Owner:    taskOwner,
		})
	}
	return dues, nil
}

func nextOwner() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10) + "-" +
		strconv.FormatUint(atomic.AddUint64(&ownerSeq, 1), 10)
}

func (s *RedisStore) dropInflight(ctx context.Context, shard int, id, owner string) error {
	return s.releaseOwner(ctx, shard, id, owner, "")
}

func (s *RedisStore) releaseOwner(ctx context.Context, shard int, id, owner, newValue string) error {
	if owner == "" {
		return ErrNotOwner
	}
	res, err := s.rdb.Eval(ctx, delIfOwnerScript,
		[]string{s.inflightKey(shard)},
		id, owner, newValue).Result()
	if err != nil {
		return err
	}
	if parseInt64(res) < 0 {
		return ErrNotOwner
	}
	return nil
}

func (s *RedisStore) loadAndBumpAttempts(ctx context.Context, id string) (*Task, error) {
	raw, err := s.rdb.Get(ctx, s.taskKey(id)).Result()
	if err != nil {
		return nil, err
	}

	var t Task
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return nil, err
	}

	t.Attempts++
	t.Status = StatusInflight
	t.UpdatedAt = time.Now().UnixMilli()
	if body, mErr := json.Marshal(&t); mErr == nil {

		_ = s.rdb.Set(ctx, s.taskKey(id), body, redis.KeepTTL).Err()
	}
	return &t, nil
}

func (s *RedisStore) Complete(ctx context.Context, id string, owner string) error {
	if err := s.releaseOwner(ctx, s.shardOf(id), id, owner, ""); err != nil {
		return err
	}
	return s.updateStatus(ctx, id, StatusDone, "")
}

func (s *RedisStore) Fail(ctx context.Context, id string, owner string, reason string, delayMs int64) error {
	shard := s.shardOf(id)
	if delayMs <= 0 {
		delayMs = 30000
	}
	nextRunAt := time.Now().UnixMilli() + delayMs

	if err := s.releaseOwner(ctx, shard, id, owner, strconv.FormatInt(nextRunAt, 10)); err != nil {
		return err
	}

	if err := s.updateStatus(ctx, id, StatusPending, reason); err != nil {
		return err
	}
	return s.rdb.ZAdd(ctx, s.pendingKey(shard), &redis.Z{
		Score:  float64(nextRunAt),
		Member: id,
	}).Err()
}

func (s *RedisStore) Release(ctx context.Context, id string, owner string) error {
	shard := s.shardOf(id)

	if err := s.releaseOwner(ctx, shard, id, owner, ""); err != nil {
		return err
	}

	if t, err := s.loadAndDecrementAttempts(ctx, id); err == nil && t != nil {
		_ = t
	}

	return s.rdb.ZAdd(ctx, s.pendingKey(shard), &redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: id,
	}).Err()
}

func (s *RedisStore) loadAndDecrementAttempts(ctx context.Context, id string) (*Task, error) {
	raw, err := s.rdb.Get(ctx, s.taskKey(id)).Result()
	if err != nil {
		return nil, err
	}

	var t Task
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return nil, err
	}

	if t.Attempts > 0 {
		t.Attempts--
	}
	t.Status = StatusPending
	t.UpdatedAt = time.Now().UnixMilli()
	if body, mErr := json.Marshal(&t); mErr == nil {
		_ = s.rdb.Set(ctx, s.taskKey(id), body, redis.KeepTTL).Err()
	}
	return &t, nil
}

func (s *RedisStore) Abandon(ctx context.Context, id string, owner string, reason string) error {
	if err := s.releaseOwner(ctx, s.shardOf(id), id, owner, ""); err != nil {
		return err
	}
	return s.updateStatus(ctx, id, StatusFailed, reason)
}

func (s *RedisStore) Recover(ctx context.Context, now int64, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	recovered := 0
	for _, shard := range s.ownedShards() {
		if recovered >= limit {
			break
		}
		n, err := s.recoverShard(ctx, shard, now, limit-recovered)
		if err != nil {
			if recovered > 0 {
				return recovered, nil
			}
			return 0, err
		}
		recovered += n
	}
	return recovered, nil
}

func (s *RedisStore) recoverShard(ctx context.Context, shard int, now int64, limit int) (int, error) {
	raw, err := s.rdb.Eval(ctx, recoverScript,
		[]string{s.pendingKey(shard), s.inflightKey(shard)},
		now, limit).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}

	items, ok := raw.([]interface{})
	if !ok {
		return 0, nil
	}

	recovered := 0
	for _, it := range items {
		id, _ := it.(string)
		if id == "" {
			continue
		}
		recovered++

		_ = s.updateStatus(ctx, id, StatusPending, "visibility timeout, requeued")
	}
	return recovered, nil
}

func (s *RedisStore) Pending(ctx context.Context, id string) (string, error) {
	if id == "" {
		return "", nil
	}

	raw, err := s.rdb.Get(ctx, s.taskKey(id)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	var t Task
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return "", err
	}
	return t.Status, nil
}

func (s *RedisStore) InQueue(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, nil
	}

	_, err := s.rdb.ZScore(ctx, s.pendingKey(s.shardOf(id)), id).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *RedisStore) Inflight(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, nil
	}

	_, err := s.rdb.HGet(ctx, s.inflightKey(s.shardOf(id)), id).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

type Stats struct {
	Pending  int64 `json:"pending"`
	Inflight int64 `json:"inflight"`
}

func (s *RedisStore) Stats(ctx context.Context) (*Stats, error) {
	pending, err := s.PendingCount(ctx)
	if err != nil {
		return nil, err
	}
	inflight, err := s.InflightCount(ctx)
	if err != nil {
		return nil, err
	}
	return &Stats{Pending: pending, Inflight: inflight}, nil
}

func (s *RedisStore) PendingCount(ctx context.Context) (int64, error) {
	var total int64
	for _, shard := range s.ownedShards() {
		n, err := s.rdb.ZCard(ctx, s.pendingKey(shard)).Result()
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (s *RedisStore) InflightCount(ctx context.Context) (int64, error) {
	var total int64
	for _, shard := range s.ownedShards() {
		n, err := s.rdb.HLen(ctx, s.inflightKey(shard)).Result()
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (s *RedisStore) ShardStats(ctx context.Context) (map[int]*Stats, error) {
	result := make(map[int]*Stats)
	for i := 0; i < s.shardCount(); i++ {
		pending, err := s.rdb.ZCard(ctx, s.pendingKey(i)).Result()
		if err != nil {
			return nil, err
		}
		inflight, err := s.rdb.HLen(ctx, s.inflightKey(i)).Result()
		if err != nil {
			return nil, err
		}
		result[i] = &Stats{Pending: pending, Inflight: inflight}
	}
	return result, nil
}

func (s *RedisStore) updateStatus(ctx context.Context, id, status, lastErr string) error {
	raw, err := s.rdb.Get(ctx, s.taskKey(id)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return err
	}
	var t Task
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return err
	}
	t.Status = status
	t.UpdatedAt = time.Now().UnixMilli()
	if lastErr != "" {
		t.LastError = lastErr
	}
	body, err := json.Marshal(&t)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, s.taskKey(id), body, redis.KeepTTL).Err()
}

func parseInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		var f float64
		if _, err := fmt.Sscanf(n, "%f", &f); err == nil {
			return int64(f)
		}
	}
	return 0
}
