package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"test.com/toUser/internal/dao"

	"time"

	"github.com/segmentio/kafka-go"
	"test.com/others/kk"
	"test.com/toUser/internal/data"
	"test.com/toUser/internal/repo"
)

type ConsumerRegistrar struct {
	brokers   []string
	dlqWriter *kk.KafkaWriter
	notifyDao *dao.NotificationDao
	cache     repo.Cache
}

func NewConsumerRegistrar(brokers []string, dlqWriter *kk.KafkaWriter) *ConsumerRegistrar {
	return &ConsumerRegistrar{
		brokers:   brokers,
		dlqWriter: dlqWriter,
		notifyDao: dao.NewNotificationDao(),
		cache:     dao.Rc,
	}
}

func (c *ConsumerRegistrar) isProcessed(ctx context.Context, msg kafka.Message) bool {
	key := fmt.Sprintf("mq:done:%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
	ok, err := c.cache.SetNX(ctx, key, "1", 24*time.Hour)
	if err != nil {
		log.Printf("idempotent SetNX err topic=%s offset=%d: %s", msg.Topic, msg.Offset, err.Error())
		return true
	}
	return ok
}

func (c *ConsumerRegistrar) Start(ctx context.Context, wg *sync.WaitGroup) {
	consumers := []struct {
		topic   string
		group   string
		handler kk.Handler
	}{
		{"user_register", "user_register_group", c.handleUserRegister},
		{"user_register_dlq", "dlq_group", c.handleDLQ},
	}

	for _, cc := range consumers {
		reader := kk.GetReader(c.brokers, cc.group, cc.topic)
		wg.Add(1)
		go func(r *kk.KafkaReader, h kk.Handler, topic string) {
			defer wg.Done()
			log.Printf("[consumer] start topic=%s", topic)
			r.RunWithDLQ(ctx, c.dlqWriter, h)
			log.Printf("[consumer] exit topic=%s", topic)
		}(reader, cc.handler, cc.topic)
	}
}

func (c *ConsumerRegistrar) handleUserRegister(ctx context.Context, msg kafka.Message) error {
	if !c.isProcessed(ctx, msg) {
		return nil
	}
	n := &data.Notification{
		Type:      "user_registered",
		Topic:     msg.Topic,
		MsgKey:    string(msg.Key),
		Payload:   string(msg.Value),
		CreatedAt: time.Now(),
	}
	return c.notifyDao.SaveNotification(ctx, n)
}

func (c *ConsumerRegistrar) handleDLQ(ctx context.Context, msg kafka.Message) error {
	reason, payload := parseDLQValue(msg.Value)
	dl := &data.DeadLetter{
		Topic:     strings.TrimSuffix(msg.Topic, "_dlq"),
		Reason:    reason,
		Payload:   payload,
		CreatedAt: time.Now(),
	}
	return c.notifyDao.SaveDeadLetter(ctx, dl)
}

func parseDLQValue(v []byte) (reason, payload string) {
	s := string(v)
	if !strings.HasPrefix(s, "[") {
		return "unknown", s
	}
	idx := strings.Index(s, "] ")
	if idx < 0 {
		return "unknown", s
	}
	return s[1:idx], s[idx+2:]
}
