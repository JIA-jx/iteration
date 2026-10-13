package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var ApiCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "api_request_total",
		Help: "接口请求总数",
	},
	[]string{"api", "status"},
)

var KafkaConsumerLag = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Kafka 消费者落后未消费的消息数",
	},
	[]string{"topic", "group_id"},
)

var KafkaConsumerConsumed = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "kafka_consumer_consumed_total",
		Help: "Kafka 消费者已消费消息总数",
	},
	[]string{"topic", "group_id"},
)

var KafkaConsumerFailed = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "kafka_consumer_failed_total",
		Help: "Kafka 消费者处理失败转 DLQ 的消息数",
	},
	[]string{"topic", "group_id"},
)
