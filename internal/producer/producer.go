package producer

import (
	"context"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/metric"
	"koko/kafka-rest-producer/internal/model"
	"koko/kafka-rest-producer/internal/producer/rdk"
	"koko/kafka-rest-producer/internal/producer/sarama"
	"koko/kafka-rest-producer/internal/producer/segment"
	"log/slog"
)

type Producer interface {
	Async() bool
	SendAsync(ctx context.Context, batch *model.MessageBatch) error
	SendSync(ctx context.Context, batch *model.MessageBatch) ([]model.ProduceResult, error)
	Close() error
}

func NewKafkaProducers(cfgs config.ProducerConfigs, ms metric.Service) (map[config.ProducerId]Producer, error) {
	producers := make(map[config.ProducerId]Producer, len(cfgs))
	for pid, cfg := range cfgs {
		switch cfg := cfg.(type) {
		case config.RdKafkaProducerConfig:
			p, err := rdk.NewProducer(cfg, ms)
			if err != nil {
				return nil, err
			}
			producers[pid] = p
		case config.SaramaProducerConfig:
			p, err := sarama.NewProducer(cfg, ms)
			if err != nil {
				return nil, err
			}
			producers[pid] = p
		case config.SegmentProducerConfig:
			p, err := segment.NewProducer(cfg, ms)
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
