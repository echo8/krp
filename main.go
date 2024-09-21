package main

import (
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/metric"
	"koko/kafka-rest-producer/internal/producer"
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
	producers, err := producer.NewKafkaProducers(cfg.Producers, ms)
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
