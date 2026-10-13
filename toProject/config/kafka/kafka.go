package kafka

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"test.com/others/kk"
	"test.com/toProject/config"
	"test.com/toProject/internal/cachekv"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/repo"
)

var kw *kk.KafkaWriter

const InvalidateTopic = "demo1_cache"

const (
	InvalidateTypeGroup = "group"
	InvalidateTypeKeys  = "keys"
)

func InitKafkaWriter() *kk.KafkaWriter {
	kw = kk.GetWriter(config.C.KafkaConfig.Brokers[0])
	return kw
}

type CacheMessage struct {
	Type string   `json:"type"`
	Name string   `json:"name"`
	Keys []string `json:"keys"`
	From string   `json:"from"`
}

func PublishGroupInvalidate(ctx context.Context, group string) {
	PublishCacheInvalidate(ctx, CacheMessage{
		Type: InvalidateTypeGroup,
		Name: group,
		From: "toProject",
	})
}

func PublishCacheInvalidate(ctx context.Context, msg CacheMessage) {
	if kw == nil {
		return
	}
	body, err := json.Marshal(msg)
	if err != nil {
		zap.L().Warn("marshal cache invalidate message failed", zap.Error(err))
		return
	}
	if err := kw.SendSync(ctx, kk.KKData{Ctx: ctx, Topic: InvalidateTopic, Data: body}); err != nil {
		zap.L().Warn("publish cache invalidate failed", zap.Error(err))
	}
}

func SendCache(ctx context.Context, data []byte) {
	PublishGroupInvalidate(ctx, strings.Trim(strings.TrimSpace(string(data)), `"`))
}

type KafkaCache struct {
	R     *kk.KafkaReader
	cache repo.Cache
}

func NewCacheReader() *KafkaCache {
	reader := kk.GetReader(config.C.KafkaConfig.Brokers, "cache_group", InvalidateTopic)
	return &KafkaCache{
		R:     reader,
		cache: dao.Rc,
	}
}

func (c *KafkaCache) DeleteCache(ctx context.Context, dlq *kk.KafkaWriter) {
	c.R.RunWithDLQ(ctx, dlq, func(ctx context.Context, msg kafka.Message) error {
		var cacheMsg CacheMessage
		if err := json.Unmarshal(msg.Value, &cacheMsg); err != nil {

			legacy := strings.Trim(strings.TrimSpace(string(msg.Value)), `"`)
			if legacy != "" {
				c.invalidateGroup(ctx, legacy)
				return nil
			}
			return err
		}

		switch cacheMsg.Type {
		case InvalidateTypeKeys:
			if len(cacheMsg.Keys) > 0 {
				c.cache.Delete(ctx, cacheMsg.Keys)
			}
		case InvalidateTypeGroup, "":
			c.invalidateGroup(ctx, cacheMsg.Name)
		default:

			zap.L().Warn("unknown cache invalidate type",
				zap.String("type", cacheMsg.Type))
		}
		return nil
	})
}

func (c *KafkaCache) invalidateGroup(ctx context.Context, group string) {
	cachekv.InvalidateGroup(ctx, c.cache, group)
}
