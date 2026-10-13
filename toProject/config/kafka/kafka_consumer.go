package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"test.com/others/idem"
	"test.com/others/kk"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
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
	ttl := 24 * time.Hour

	guard := dao.IdemGuard()
	if guard == nil {
		ok, err := c.cache.SetNX(ctx, key, "1", ttl)
		if err != nil {
			log.Printf("idempotent SetNX err topic=%s offset=%d: %s", msg.Topic, msg.Offset, err.Error())
			return true
		}
		return ok
	}

	outcome, err := guard.Do(ctx, key, func(ctx context.Context) (string, error) {
		return "1", nil
	})
	if err != nil {
		if errors.Is(err, idem.ErrInProgress) {
			return false
		}
		log.Printf("idempotent guard err topic=%s offset=%d: %s", msg.Topic, msg.Offset, err.Error())
		return true
	}
	return outcome.Executed
}

func (c *ConsumerRegistrar) Start(ctx context.Context, wg *sync.WaitGroup) {
	consumers := []struct {
		topic   string
		group   string
		handler kk.Handler
	}{
		{"task_exchange", "task_notify_group", c.handleTaskNotify},
		{"workflow_auth_apply", "workflow_audit_group", c.handleWorkflowEvent},
		{"analysis_delivery", "analysis_group", c.handleAnalysisHistory("analysis_delivery")},
		{"analysis_burndown", "analysis_group", c.handleAnalysisHistory("analysis_burndown")},
		{"analysis_teamload", "analysis_group", c.handleAnalysisHistory("analysis_teamload")},
		{"analysis_ai_recommend", "analysis_group", c.handleAnalysisHistory("analysis_ai_recommend")},
		{"task_exchange_dlq", "dlq_group", c.handleDLQ},
		{"workflow_auth_apply_dlq", "dlq_group", c.handleDLQ},
		{"analysis_delivery_dlq", "dlq_group", c.handleDLQ},
		{"analysis_burndown_dlq", "dlq_group", c.handleDLQ},
		{"analysis_teamload_dlq", "dlq_group", c.handleDLQ},
		{"analysis_ai_recommend_dlq", "dlq_group", c.handleDLQ},
		{"demo1_cache_dlq", "dlq_group", c.handleDLQ},
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

func (c *ConsumerRegistrar) handleTaskNotify(ctx context.Context, msg kafka.Message) error {
	if !c.isProcessed(ctx, msg) {
		return nil
	}
	n := &data.Notification{
		Type:      "task_notify",
		Topic:     msg.Topic,
		MsgKey:    string(msg.Key),
		Payload:   string(msg.Value),
		CreatedAt: time.Now(),
	}
	return c.notifyDao.SaveNotification(ctx, n)
}

func (c *ConsumerRegistrar) handleWorkflowEvent(ctx context.Context, msg kafka.Message) error {
	if !c.isProcessed(ctx, msg) {
		return nil
	}
	n := &data.Notification{
		Type:      "workflow_event",
		Topic:     msg.Topic,
		MsgKey:    string(msg.Key),
		Payload:   string(msg.Value),
		CreatedAt: time.Now(),
	}
	return c.notifyDao.SaveNotification(ctx, n)
}

func (c *ConsumerRegistrar) handleAnalysisHistory(topic string) kk.Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		if !c.isProcessed(ctx, msg) {
			return nil
		}
		n := &data.Notification{
			Type:      "analysis_history",
			Topic:     topic,
			MsgKey:    string(msg.Key),
			Payload:   string(msg.Value),
			CreatedAt: time.Now(),
		}
		return c.notifyDao.SaveNotification(ctx, n)
	}
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
