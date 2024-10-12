package main

import (
	"echo8/kafka-rest-producer/internal/config"
	rdkcfg "echo8/kafka-rest-producer/internal/config/rdk"
	saramacfg "echo8/kafka-rest-producer/internal/config/sarama"
	segmentcfg "echo8/kafka-rest-producer/internal/config/segment"
	"echo8/kafka-rest-producer/internal/metric"
	"echo8/kafka-rest-producer/internal/producer"
	"echo8/kafka-rest-producer/internal/producer/rdk"
	"echo8/kafka-rest-producer/internal/producer/sarama"
	"echo8/kafka-rest-producer/internal/producer/segment"
	"echo8/kafka-rest-producer/internal/serializer"
	"echo8/kafka-rest-producer/internal/server"
	"fmt"
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
	s, err := server.NewServer(cfg, ps, ms)
	if err != nil {
		panic(err)
	}
	err = s.Run()
	if err != nil {
		slog.Error("An error was returned after running the server.", "error", err.Error())
	}
	ps.CloseProducers()
}

func newKafkaProducers(cfgs config.ProducerConfigs, ms metric.Service) (map[config.ProducerId]producer.Producer, error) {
	producers := make(map[config.ProducerId]producer.Producer, len(cfgs))
	for pid, cfg := range cfgs {
		srCfg := cfg.SchemaRegistryCfg()
		srClient, err := srCfg.ToClient()
		if err != nil {
			return nil, err
		}
		keySerializer, err := serializer.NewSerializer(srCfg, srClient, true)
		if err != nil {
			return nil, err
		}
		valueSerializer, err := serializer.NewSerializer(srCfg, srClient, false)
		if err != nil {
			return nil, err
		}
		switch cfg := cfg.(type) {
		case *rdkcfg.ProducerConfig:
			p, err := rdk.NewProducer(cfg, ms, keySerializer, valueSerializer)
			if err != nil {
				return nil, err
			}
			producers[pid] = p
		case *saramacfg.ProducerConfig:
			p, err := sarama.NewProducer(cfg, ms, keySerializer, valueSerializer)
			if err != nil {
				return nil, err
			}
			producers[pid] = p
		case *segmentcfg.ProducerConfig:
			p, err := segment.NewProducer(cfg, ms, keySerializer, valueSerializer)
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
