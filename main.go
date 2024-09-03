package main

import (
	"koko/kafka-rest-producer/internal/config"
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
	producers, err := producer.NewKafkaProducers(cfg.Producers)
	if err != nil {
		panic(err)
	}
	ps, err := producer.NewService(producers)
	if err != nil {
		panic(err)
	}
	s := server.NewServer(cfg, ps)
	err = s.Run()
	if err != nil {
		slog.Error("An error was returned after running the server.", "error", err.Error())
	}
	ps.CloseProducers()
}
