package kk

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"test.com/others/prom"
)

type KafkaReader struct {
	R       *kafka.Reader
	topic   string
	groupID string
}

func GetReader(brokers []string, groupId, topic string) *KafkaReader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupId,
		Topic:          topic,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	k := &KafkaReader{R: r, topic: topic, groupID: groupId}
	return k
}

type Handler func(ctx context.Context, msg kafka.Message) error

func (r *KafkaReader) RunWithDLQ(ctx context.Context, dlq *KafkaWriter, h Handler) {
	lagCtx, lagCancel := context.WithCancel(context.Background())
	defer lagCancel()
	go r.reportLag(lagCtx)

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		m, err := r.R.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("kafka readMsg err %s \n", err.Error())
			time.Sleep(200 * time.Millisecond)
			continue
		}

		r.handleWithRecover(ctx, dlq, m, h)
		prom.KafkaConsumerConsumed.WithLabelValues(r.topic, r.groupID).Inc()

		commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := r.R.CommitMessages(commitCtx, m); err != nil {
			log.Printf("kafka commit offset err topic=%s partition=%d offset=%d: %s\n",
				m.Topic, m.Partition, m.Offset, err.Error())
		}
		cancel()
	}
}

func (r *KafkaReader) reportLag(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.updateLag()
		}
	}
}

func (r *KafkaReader) updateLag() {
	stats := r.R.Stats()
	prom.KafkaConsumerLag.WithLabelValues(r.topic, r.groupID).Set(float64(stats.Lag))
}

func (r *KafkaReader) handleWithRecover(ctx context.Context, dlq *KafkaWriter, m kafka.Message, h Handler) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("kafka handler panic topic=%s partition=%d offset=%d: %v\n",
				m.Topic, m.Partition, m.Offset, rec)
			r.toDLQ(ctx, dlq, m, fmt.Sprintf("panic: %v", rec))
			prom.KafkaConsumerFailed.WithLabelValues(r.topic, r.groupID).Inc()
		}
	}()
	if err := h(ctx, m); err != nil {
		log.Printf("kafka handle err topic=%s partition=%d offset=%d: %s\n",
			m.Topic, m.Partition, m.Offset, err.Error())
		r.toDLQ(ctx, dlq, m, err.Error())
		prom.KafkaConsumerFailed.WithLabelValues(r.topic, r.groupID).Inc()
	}
}

func (r *KafkaReader) toDLQ(ctx context.Context, dlq *KafkaWriter, m kafka.Message, reason string) {
	if dlq == nil {
		return
	}

	payload := append([]byte("["+reason+"] "), m.Value...)
	if err := dlq.SendSync(ctx, KKData{
		Topic: m.Topic + "_dlq",
		Data:  payload,
	}); err != nil {
		log.Printf("kafka send dlq err topic=%s partition=%d offset=%d: %s\n",
			m.Topic, m.Partition, m.Offset, err.Error())
	}
}

func (r *KafkaReader) Close() {
	r.R.Close()
}
