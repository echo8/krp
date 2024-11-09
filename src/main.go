package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/echo8/krp/internal/config"
	confluentcfg "github.com/echo8/krp/internal/config/confluent"
	saramacfg "github.com/echo8/krp/internal/config/sarama"
	segmentcfg "github.com/echo8/krp/internal/config/segment"
	"github.com/echo8/krp/internal/metric"
	"github.com/echo8/krp/internal/producer"
	"github.com/echo8/krp/internal/producer/confluent"
	"github.com/echo8/krp/internal/producer/sarama"
	"github.com/echo8/krp/internal/producer/segment"
	"github.com/echo8/krp/internal/serializer"
	"github.com/echo8/krp/internal/server"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

func main() {
	cfgPath := flag.String("config", "", "path to config file")
	flag.Parse()
	if len(*cfgPath) == 0 {
		fmt.Fprintln(os.Stderr, "required flag not defined: -config")
		flag.Usage()
		os.Exit(1)
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fatal("failed to load configuration.", err)
	}
	ms, err := metric.NewService(&cfg.Metrics)
	if err != nil {
		fatal("failed to initialize metrics.", err)
	}
	producers, err := newKafkaProducers(cfg.Producers, ms)
	if err != nil {
		fatal("failed to create producers.", err)
	}
	ps, err := producer.NewService(producers)
	if err != nil {
		fatal("failed to initialize producers.", err)
	}
	s, err := server.NewServer(cfg, ps, ms)
	if err != nil {
		fatal("failed to start server.", err)
	}
	err = s.Run()
	if err != nil {
		slog.Error("an error was returned after running the server.", "error", err.Error())
	}
	ps.CloseProducers()
}

func newKafkaProducers(cfgs config.ProducerConfigs, ms metric.Service) (map[config.ProducerId]producer.Producer, error) {
	producers := make(map[config.ProducerId]producer.Producer, len(cfgs))
	for pid, cfg := range cfgs {
		srCfg := cfg.SchemaRegistryCfg()
		var srClient schemaregistry.Client
		var err error
		if srCfg != nil {
			srClient, err = srCfg.ToClient()
			if err != nil {
				return nil, err
			}
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
		case *confluentcfg.ProducerConfig:
			p, err := confluent.NewProducer(cfg, ms, keySerializer, valueSerializer)
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
		slog.Info("loaded new producer.", "pid", pid)
	}
	return producers, nil
}

func fatal(msg string, err error) {
	slog.Error(msg, "error", err.Error())
	os.Exit(1)
}
