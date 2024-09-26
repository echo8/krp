package main

import (
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	rdkcfg "koko/kafka-rest-producer/internal/config/rdk"
	saramacfg "koko/kafka-rest-producer/internal/config/sarama"
	segmentcfg "koko/kafka-rest-producer/internal/config/segment"
	"koko/kafka-rest-producer/internal/metric"
	"koko/kafka-rest-producer/internal/producer"
	"koko/kafka-rest-producer/internal/producer/rdk"
	"koko/kafka-rest-producer/internal/producer/sarama"
	"koko/kafka-rest-producer/internal/producer/segment"
	"koko/kafka-rest-producer/internal/server"
	"log/slog"
	"os"
)

func main() {
	cfg, err := config.Load(os.Args[1])
	if err != nil {
		panic(err)
	}
	ms, err := metric.NewService(&cfg.Metrics)
	if err != nil {
		panic(err)
	}
	producers, err := newKafkaProducers(cfg.Producers, ms)
	if err != nil {
		panic(err)
	}
	ps, err := producer.NewService(producers)
	if err != nil {
		panic(err)
	}
	s := server.NewServer(cfg, ps, ms)
	err = s.Run()
	if err != nil {
		slog.Error("An error was returned after running the server.", "error", err.Error())
	}
	ps.CloseProducers()
}

func newKafkaProducers(cfgs config.ProducerConfigs, ms metric.Service) (map[config.ProducerId]producer.Producer, error) {
	producers := make(map[config.ProducerId]producer.Producer, len(cfgs))
	for pid, cfg := range cfgs {
		switch cfg := cfg.(type) {
		case *rdkcfg.ProducerConfig:
			p, err := rdk.NewProducer(cfg, ms)
			if err != nil {
				return nil, err
			}
			producers[pid] = p
		case *saramacfg.ProducerConfig:
			p, err := sarama.NewProducer(cfg, ms)
			if err != nil {
				return nil, err
			}
			producers[pid] = p
		case *segmentcfg.ProducerConfig:
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
