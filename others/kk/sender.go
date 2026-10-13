package kk

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KKData struct {
	Ctx   context.Context
	Topic string
	Data  []byte
}
type KafkaWriter struct {
	W    *kafka.Writer
	data chan KKData
}

func GetWriter(addr string) *KafkaWriter {
	return &KafkaWriter{
		W: &kafka.Writer{
			Addr:         kafka.TCP(addr),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			MaxAttempts:  3,
		},
	}
}

func (w *KafkaWriter) SendSync(ctx context.Context, data KKData) error {
	msg := kafka.Message{
		Topic: data.Topic,
		Key:   []byte("logMsg"),
		Value: data.Data,
	}
	const retries = 3
	var err error
	for i := 0; i < retries; i++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		ctx_, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = w.W.WriteMessages(ctx_, msg)
		cancel()
		if err == nil {
			return nil
		}
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond * 200):
			}
			continue
		}
		break
	}
	if err != nil {
		log.Printf("kafka SendSync err topic=%s: %s\n", data.Topic, err.Error())
	}
	return err
}
func (w *KafkaWriter) Close() {
	if w.W != nil {
		w.W.Close()
	}
}
