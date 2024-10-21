package integrationtest

import (
	"context"
	"testing"
	"time"

	"github.com/echo8/krp/model"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/network"
)

func TestLinger(t *testing.T) {
	ctx := context.Background()

	network, err := network.New(ctx)
	require.NoError(t, err)
	broker, err := NewKafkaContainer(ctx, "broker", "9094", network.Name)
	require.NoError(t, err)
	defer broker.Terminate(ctx)
	krp, err := NewKrpContainer(ctx, network.Name, `addr: ":8080"
endpoints:
  first:
    async: true
    routes:
      - topic: topic1
        producer: confluent
  second:
    async: true
    routes:
      - topic: topic1
        producer: ibm
  third:
    async: true
    routes:
      - topic: topic1
        producer: segment
  first_fast:
    async: true
    routes:
      - topic: topic1
        producer: confluent_fast
  second_fast:
    async: true
    routes:
      - topic: topic1
        producer: ibm_fast
  third_fast:
    async: true
    routes:
      - topic: topic1
        producer: segment_fast
producers:
  confluent:
    type: kafka
    clientConfig:
      bootstrap.servers: broker:9092
      linger.ms: 5000
  ibm:
    type: sarama
    clientConfig:
      bootstrap.servers: broker:9092
      producer.flush.frequency: 5000ms
  segment:
    type: segment
    clientConfig:
      bootstrap.servers: broker:9092
      batch.timeout: 5000ms
  confluent_fast:
    type: kafka
    clientConfig:
      bootstrap.servers: broker:9092
      linger.ms: 10
  ibm_fast:
    type: sarama
    clientConfig:
      bootstrap.servers: broker:9092
      producer.flush.frequency: 10ms
  segment_fast:
    type: segment
    clientConfig:
      bootstrap.servers: broker:9092
      batch.timeout: 10ms`)
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name      string
		inputPath string
		fast      bool
	}{
		{
			name:      "rdk linger",
			inputPath: "/first",
		},
		{
			name:      "sarama linger",
			inputPath: "/second",
		},
		{
			name:      "segment linger",
			inputPath: "/third",
		},
		{
			name:      "rdk fast linger",
			inputPath: "/first_fast",
			fast:      true,
		},
		{
			name:      "sarama fast linger",
			inputPath: "/second_fast",
			fast:      true,
		},
		{
			name:      "segment fast linger",
			inputPath: "/third_fast",
			fast:      true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := NewConsumer(ctx, t, "topic1", "9094")
			defer consumer.Close()
			req := model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: Ptr("foo")}},
				},
			}
			ProduceAsync(ctx, t, krp, tc.inputPath, req)
			received := GetReceived(t, consumer, req.Messages,
				WithVerifyReceived(tc.fast), WithReadTimeout(3*time.Second))
			if tc.fast {
				require.Equal(t, 1, len(received))
			} else {
				require.Equal(t, 0, len(received))
				CheckReceived(t, consumer, req.Messages)
			}
		})
	}
}

func TestPartitioner(t *testing.T) {
	ctx := context.Background()

	network, err := network.New(ctx)
	require.NoError(t, err)
	broker, err := NewKafkaContainer(ctx, "broker", "9094", network.Name)
	require.NoError(t, err)
	defer broker.Terminate(ctx)
	krp, err := NewKrpContainer(ctx, network.Name, `addr: ":8080"
endpoints:
  first:
    routes:
      - topic: topic1
        producer: confluent
  second:
    routes:
      - topic: topic1
        producer: ibm
  third:
    routes:
      - topic: topic1
        producer: segment
producers:
  confluent:
    type: kafka
    clientConfig:
      bootstrap.servers: broker:9092
      partitioner: random
  ibm:
    type: sarama
    clientConfig:
      bootstrap.servers: broker:9092
      producer.partitioner: round_robin
  segment:
    type: segment
    clientConfig:
      bootstrap.servers: broker:9092
      balancer: hash
      batch.timeout: 10ms`)
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name         string
		inputPath    string
		wantBalanced bool
	}{
		{
			name:         "rdk partitioner",
			inputPath:    "/first",
			wantBalanced: true,
		},
		{
			name:         "sarama partitioner",
			inputPath:    "/second",
			wantBalanced: true,
		},
		{
			name:      "segment partitioner",
			inputPath: "/third",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := NewConsumer(ctx, t, "topic1", "9094")
			defer consumer.Close()
			req := model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar1")},
					},
					{
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar2")},
					},
					{
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar3")},
					},
					{
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar4")},
					},
					{
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar5")},
					},
					{
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar6")},
					},
				},
			}
			ProduceSync(ctx, t, krp, tc.inputPath, req)
			received := GetReceived(t, consumer, req.Messages)
			partitionCounts := make(map[int32]int)
			for _, kafkaMsg := range received {
				p := kafkaMsg.TopicPartition.Partition
				count, ok := partitionCounts[p]
				if ok {
					partitionCounts[p] = count + 1
				} else {
					partitionCounts[p] = 1
				}
			}
			if tc.wantBalanced {
				require.Greater(t, len(partitionCounts), 1)
			} else {
				require.Equal(t, 1, len(partitionCounts))
			}
		})
	}
}

func TestMaxMessageSize(t *testing.T) {
	ctx := context.Background()

	network, err := network.New(ctx)
	require.NoError(t, err)
	broker, err := NewKafkaContainer(ctx, "broker", "9094", network.Name)
	require.NoError(t, err)
	defer broker.Terminate(ctx)
	krp, err := NewKrpContainer(ctx, network.Name, `addr: ":8080"
endpoints:
  first:
    routes:
      - topic: topic1
        producer: confluent
  second:
    routes:
      - topic: topic1
        producer: ibm
  first_large:
    routes:
      - topic: topic1
        producer: confluent_large
  second_large:
    routes:
      - topic: topic1
        producer: ibm_large
producers:
  confluent:
    type: kafka
    clientConfig:
      bootstrap.servers: broker:9092
      message.max.bytes: 1000
  ibm:
    type: sarama
    clientConfig:
      bootstrap.servers: broker:9092
      producer.max.message.bytes: 1000
  confluent_large:
    type: kafka
    clientConfig:
      bootstrap.servers: broker:9092
      message.max.bytes: 5000
  ibm_large:
    type: sarama
    clientConfig:
      bootstrap.servers: broker:9092
      producer.max.message.bytes: 5000`)
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name        string
		inputPath   string
		wantSuccess bool
	}{
		{
			name:      "rdk max message size",
			inputPath: "/first",
		},
		{
			name:      "sarama max message size",
			inputPath: "/second",
		},
		{
			name:        "rdk large max message size",
			inputPath:   "/first_large",
			wantSuccess: true,
		},
		{
			name:        "sarama large max message size",
			inputPath:   "/second_large",
			wantSuccess: true,
		},
	}

	val := ""
	for range 1000 {
		val += "foo"
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := NewConsumer(ctx, t, "topic1", "9094")
			defer consumer.Close()
			req := model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: &val}},
				},
			}
			ProduceSync(ctx, t, krp, tc.inputPath, req, WithSuccess(tc.wantSuccess))
		})
	}
}
