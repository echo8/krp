package main

import (
	"context"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/producer"
	"koko/kafka-rest-producer/internal/server"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	cfg, err := config.Load(os.Args[1])
	if err != nil {
		panic(err)
	}
	if cfg.Metrics.Enabled {
		setupMetrics()
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

func setupMetrics() {
	slog.Info("Setting up metrics.")
	ctx := context.Background()
	conn, err := grpc.NewClient("otel-collector:4317", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		panic(err)
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(5*time.Second))),
	)
	otel.SetMeterProvider(meterProvider)
}
