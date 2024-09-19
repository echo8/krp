package producer

import (
	"context"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/metric"
	"koko/kafka-rest-producer/internal/model"
	"log/slog"
)

type MessageBatch struct {
	Messages []TopicAndMessage
	Src      *config.Endpoint
}

type TopicAndMessage struct {
	Topic   string
	Message *model.ProduceMessage
}

type Producer interface {
	Async() bool
	SendAsync(ctx context.Context, batch *MessageBatch) error
	SendSync(ctx context.Context, batch *MessageBatch) ([]model.ProduceResult, error)
	Close() error
}

func NewKafkaProducers(cfgs config.ProducerConfigs, ms metric.Service) (map[config.ProducerId]Producer, error) {
	producers := make(map[config.ProducerId]Producer, len(cfgs))
	for pid, cfg := range cfgs {
		switch cfg := cfg.(type) {
		case config.RdKafkaProducerConfig:
			rdp, err := NewRdKafkaProducer(cfg)
			if err != nil {
				return nil, err
			}
			p, err := NewKafkaProducer(cfg, rdp, ms)
			if err != nil {
				return nil, err
			}
			producers[pid] = p
		case config.SaramaProducerConfig:
			sp, err := NewSaramaAsyncProducer(cfg)
			if err != nil {
				return nil, err
			}
			producers[pid] = NewSaramaBasedProducer(cfg, sp, ms)
		case config.SegmentProducerConfig:
			writer, err := NewSegmentWriter(cfg)
			if err != nil {
				return nil, err
			}
			p, err := NewSegmentBasedProducer(cfg, writer, ms)
			if err != nil {
				return nil, err
			}
			producers[pid] = p
		default:
			return nil, fmt.Errorf("failed to load producer, pid: %v", pid)
		}
		slog.Info("Loaded new producer.", "pid", pid)
	}
	return producers, nil
}
