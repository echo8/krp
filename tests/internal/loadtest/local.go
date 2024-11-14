package loadtest

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/echo8/krp/tests/internal/testutil"
	"github.com/testcontainers/testcontainers-go/network"
)

type loadTest struct {
	name     string
	endpoint string
	cfg      string
}

func RunLocal() error {
	ctx := context.Background()
	network, _ := network.New(ctx)
	otel, err := testutil.NewOtelCollectorContainer(ctx, network.Name)
	if err != nil {
		return err
	}
	defer otel.Terminate(ctx)
	prom, err := testutil.NewPrometheusContainer(ctx, network.Name)
	if err != nil {
		return err
	}
	defer prom.Terminate(ctx)

	loadtests := []loadTest{
		{
			name:     "confluent, sync, linger=5ms, acks=all",
			endpoint: "/first",
			cfg: `
endpoints:
	first:
		routes:
			- topic: topic1
				producer: confluent
producers:
	confluent:
		type: confluent
		clientConfig:
			bootstrap.servers: broker:9092
			statistics.interval.ms: 5000
			linger.ms: 5
			acks: all`,
		},
		{
			name:     "sarama, sync, linger=5ms, acks=all",
			endpoint: "/first",
			cfg: `
endpoints:
	first:
		routes:
			- topic: topic1
				producer: ibm
producers:
	ibm:
		type: sarama
		metricsFlushDuration: 5s
		clientConfig:
			bootstrap.servers: broker:9092
			producer.flush.frequency: 5ms
			producer.required.acks: all`,
		},
		{
			name:     "segment, sync, linger=5ms, acks=all",
			endpoint: "/first",
			cfg: `
endpoints:
	first:
		routes:
			- topic: topic1
				producer: segment
producers:
	segment:
		type: segment
		metricsFlushDuration: 5s
		clientConfig:
			bootstrap.servers: broker:9092
			batch.timeout: 5ms
			required.acks: all`,
		},
		{
			name:     "franz, sync, linger=5ms, acks=all",
			endpoint: "/first",
			cfg: `
endpoints:
	first:
		routes:
			- topic: topic1
				producer: franz
producers:
	franz:
		type: franz
		clientConfig:
			bootstrap.servers: broker:9092
			producer.linger: 5ms
			required.acks: all`,
		},
	}

	for _, lt := range loadtests {
		if err := runLoadTest(ctx, lt, network.Name); err != nil {
			return err
		}
	}

	return nil
}

func runLoadTest(ctx context.Context, lt loadTest, network string) error {
	fmt.Println("Running load test:", lt.name)

	topics := []kafka.TopicSpecification{
		{
			Topic:         "topic1",
			NumPartitions: 3,
		},
	}
	broker, err := testutil.NewKafkaContainer(ctx, "broker", "9094", network, topics...)
	if err != nil {
		return err
	}
	krp, err := testutil.NewKrpContainer(ctx, network, testutil.FormatCfg(lt.cfg)+`
metrics:
  enable:
    all: true
  otel:
    endpoint: otel-collector:4317
    tls:
      insecure: true
`)
	if err != nil {
		return err
	}
	defer krp.Terminate(ctx)
	defer broker.Terminate(ctx)

	mp, _ := krp.MappedPort(ctx, "8080/tcp")

	fmt.Println()
	fmt.Printf("%10v %10v %10v %10v %10v %10v\n", "Rate", "Total", "Success", "p50", "p90", "p95")

	for range 5 {
		res := GenerateLoad(&LoadConfig{
			Rps:      10000,
			Duration: 10 * time.Second,
			URL:      fmt.Sprintf("http://localhost:%s%s", mp.Port(), lt.endpoint),
		})
		fmt.Printf(
			"%10.2f %10v %10.2f %10.3f %10.3f %10.3f\n",
			res.Rate,
			res.Requests,
			res.Success,
			res.Latencies.P50.Seconds()*1000,
			res.Latencies.P90.Seconds()*1000,
			res.Latencies.P95.Seconds()*1000,
		)
	}
	fmt.Println()
	return nil
}
