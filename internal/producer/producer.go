package producer

import (
	"context"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/model"
	"log/slog"
)

type TopicAndMessage struct {
	Topic   string
	Message *model.ProduceMessage
}

type Producer interface {
	Send(ctx context.Context, messages []TopicAndMessage) []model.ProduceResult
	Close() error
}

func NewKafkaProducers(cfgs config.ProducerConfigs) (map[config.ProducerId]Producer, error) {
	producers := make(map[config.ProducerId]Producer, len(cfgs))
	for pid, cfg := range cfgs {
		switch cfg := cfg.(type) {
		case config.RdKafkaProducerConfig:
			rdp, err := NewRdKafkaProducer(cfg)
			if err != nil {
				return nil, err
			}
			p, err := NewKafkaProducer(cfg, rdp)
			if err != nil {
				return nil, err
			}
			producers[pid] = p
		case config.SaramaProducerConfig:
			sp, err := NewSaramaAsyncProducer(cfg)
			if err != nil {
				return nil, err
			}
			producers[pid] = NewSaramaBasedProducer(cfg, sp)
		case config.SegmentProducerConfig:
			writer, err := NewSegmentWriter(cfg)
			if err != nil {
				return nil, err
			}
			p, err := NewSegmentBasedProducer(cfg, writer)
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
