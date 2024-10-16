package integrationtest

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type StdoutLogConsumer struct{}

func (lc *StdoutLogConsumer) Accept(l testcontainers.Log) {
	fmt.Print(string(l.Content))
}

func NewKrpContainer(ctx context.Context, network string) (testcontainers.Container, error) {
	g := StdoutLogConsumer{}
	req := testcontainers.ContainerRequest{
		Name: "krp-it",
		FromDockerfile: testcontainers.FromDockerfile{
			Context:       "/home/greg/workspace/krp",
			Dockerfile:    "local/Dockerfile",
			PrintBuildLog: true,
			Repo:          "krp",
			Tag:           "test",
		},
		Networks:     []string{network},
		ExposedPorts: []string{"8080/tcp"},
		Cmd:          []string{"/bin/bash", "-c", "/opt/app/krp /opt/app/config.yaml"},
		WaitingFor: wait.ForHTTP("http://localhost:8080/healthcheck").WithStatusCodeMatcher(func(status int) bool {
			return status == 204
		}),
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(10 * time.Second)},
			Consumers: []testcontainers.LogConsumer{&g},
		},
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func NewKafkaContainer(ctx context.Context, network string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Name:  "kafka-broker-it",
		Image: "apache/kafka:3.8.0",
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://broker:9092,CONTROLLER://localhost:9093,PLAINTEXT_HOST://0.0.0.0:9094",
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://broker:9092,PLAINTEXT_HOST://localhost:9094",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_NUM_PARTITIONS":                           "3",
		},
		ExposedPorts: []string{"9094:9094/tcp"},
		Networks:     []string{network},
		NetworkAliases: map[string][]string{
			network: {"broker"},
		},
		WaitingFor: wait.ForLog("Kafka Server started"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}
