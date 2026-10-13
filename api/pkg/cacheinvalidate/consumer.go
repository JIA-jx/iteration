package cacheinvalidate

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const Topic = "demo1_cache"

type message struct {
	Type string   `json:"type"`
	Name string   `json:"name"`
	Keys []string `json:"keys"`
	From string   `json:"from"`
}

type Consumer struct {
	Brokers []string
	Group   string
	OnPurge func(group string)
}

func (c *Consumer) Run(ctx context.Context) {
	if len(c.Brokers) == 0 || c.OnPurge == nil {
		zap.L().Warn("cache invalidate consumer disabled", zap.Int("brokers", len(c.Brokers)))
		return
	}
	group := c.Group
	if group == "" {
		group = "cache_invalidate_api"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.Brokers,
		GroupID:        group,
		Topic:          Topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer reader.Close()

	zap.L().Info("cache invalidate consumer started",
		zap.String("topic", Topic), zap.String("group", group))

	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			zap.L().Warn("cache invalidate read failed", zap.Error(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}

		group := "unknown"
		var m message
		if jErr := json.Unmarshal(msg.Value, &m); jErr == nil {
			switch {
			case m.Type == "keys":
				group = "keys"
			case m.Name != "":
				group = m.Name
			default:
				group = m.Type
			}
		} else {
			group = strings.Trim(strings.TrimSpace(string(msg.Value)), `"`)
		}
		if group == "" {
			group = "unknown"
		}

		c.OnPurge(group)

		commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if cErr := reader.CommitMessages(commitCtx, msg); cErr != nil {
			zap.L().Warn("cache invalidate commit failed", zap.Error(cErr))
		}
		cancel()
	}
}
