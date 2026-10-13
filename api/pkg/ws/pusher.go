package ws

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const delayNotifyTopic = "delay_notify"

type Pusher struct {
	brokers []string
	group   string
	hub     *Hub
	topic   string
}

func NewPusher(brokers []string, group string) *Pusher {
	return &Pusher{
		brokers: brokers,
		group:   group,
		hub:     Default(),
		topic:   delayNotifyTopic,
	}
}

func (p *Pusher) Run(ctx context.Context) {
	if len(p.brokers) == 0 {
		zap.L().Warn("ws pusher disabled: kafka brokers is empty")
		return
	}
	if p.group == "" {
		p.group = "delay_notify_ws_group"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        p.brokers,
		GroupID:        p.group,
		Topic:          p.topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer reader.Close()

	zap.L().Info("ws pusher started", zap.String("topic", p.topic), zap.String("group", p.group))

	for {
		if err := ctx.Err(); err != nil {
			zap.L().Info("ws pusher stopped")
			return
		}

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("ws pusher read err %s", err.Error())
			time.Sleep(200 * time.Millisecond)
			continue
		}

		p.dispatch(msg)

		commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := reader.CommitMessages(commitCtx, msg); err != nil {
			zap.L().Warn("ws pusher commit offset failed", zap.Error(err))
		}
		cancel()
	}
}

func (p *Pusher) dispatch(msg kafka.Message) {
	sent, err := p.hub.PushNotify(msg.Value)
	if err != nil {
		zap.L().Warn("ws push notify failed", zap.Error(err), zap.Int("payload_size", len(msg.Value)))
		return
	}

	zap.L().Info("ws notify dispatched",
		zap.Int("delivered", sent),
		zap.Int("online_total", p.hub.TotalOnline()))
}
