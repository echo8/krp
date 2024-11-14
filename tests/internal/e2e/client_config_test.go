package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/echo8/krp/model"
	"github.com/echo8/krp/tests/internal/testutil"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

func TestLinger(t *testing.T) {
	ctx := context.Background()

	network, err := network.New(ctx)
	require.NoError(t, err)
	defer network.Remove(ctx)
	broker, err := testutil.NewKafkaContainer(ctx, "broker", "9094", network.Name)
	require.NoError(t, err)
	defer broker.Terminate(ctx)
	krp, err := testutil.NewKrpContainer(ctx, network.Name, `
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
  fourth:
    async: true
    routes:
      - topic: topic1
        producer: franz
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
  fourth_fast:
    async: true
    routes:
      - topic: topic1
        producer: franz_fast
producers:
  confluent:
    type: confluent
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
  franz:
    type: franz
    clientConfig:
      bootstrap.servers: broker:9092
      producer.linger: 5000ms
  confluent_fast:
    type: confluent
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
      batch.timeout: 10ms
  franz_fast:
    type: franz
    clientConfig:
      bootstrap.servers: broker:9092
      producer.linger: 10ms`)
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name      string
		inputPath string
		fast      bool
	}{
		{
			name:      "confluent linger",
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
			name:      "franz linger",
			inputPath: "/fourth",
		},
		{
			name:      "confluent fast linger",
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
		{
			name:      "franz fast linger",
			inputPath: "/fourth_fast",
			fast:      true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := testutil.NewConsumer(ctx, t, "topic1", "9094")
			defer consumer.Close()
			req := model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			}
			testutil.ProduceAsync(ctx, t, krp, tc.inputPath, req)
			received := testutil.GetReceived(t, consumer, req.Messages,
				testutil.WithVerifyReceived(tc.fast), testutil.WithReadTimeout(3*time.Second))
			if tc.fast {
				require.Equal(t, 1, len(received))
			} else {
				require.Equal(t, 0, len(received))
				testutil.CheckReceived(t, consumer, req.Messages)
			}
		})
	}
}

func TestPartitioner(t *testing.T) {
	ctx := context.Background()

	network, err := network.New(ctx)
	require.NoError(t, err)
	defer network.Remove(ctx)
	broker, err := testutil.NewKafkaContainer(ctx, "broker", "9094", network.Name)
	require.NoError(t, err)
	defer broker.Terminate(ctx)
	krp, err := testutil.NewKrpContainer(ctx, network.Name, `
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
  fourth:
    routes:
      - topic: topic1
        producer: franz
producers:
  confluent:
    type: confluent
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
      batch.timeout: 10ms
  franz:
    type: franz
    clientConfig:
      bootstrap.servers: broker:9092
      record.partitioner: RoundRobinPartitioner`)
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name         string
		inputPath    string
		wantBalanced bool
	}{
		{
			name:         "confluent partitioner",
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
		{
			name:         "franz partitioner",
			inputPath:    "/fourth",
			wantBalanced: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := testutil.NewConsumer(ctx, t, "topic1", "9094")
			defer consumer.Close()
			req := model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar1")},
					},
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar2")},
					},
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar3")},
					},
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar4")},
					},
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar5")},
					},
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar6")},
					},
				},
			}
			testutil.ProduceSync(ctx, t, krp, tc.inputPath, req)
			received := testutil.GetReceived(t, consumer, req.Messages)
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
	defer network.Remove(ctx)
	broker, err := testutil.NewKafkaContainer(ctx, "broker", "9094", network.Name)
	require.NoError(t, err)
	defer broker.Terminate(ctx)
	krp, err := testutil.NewKrpContainer(ctx, network.Name, `
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
        producer: franz
  first_large:
    routes:
      - topic: topic1
        producer: confluent_large
  second_large:
    routes:
      - topic: topic1
        producer: ibm_large
  third_large:
    routes:
      - topic: topic1
        producer: franz_large
producers:
  confluent:
    type: confluent
    clientConfig:
      bootstrap.servers: broker:9092
      message.max.bytes: 1000
  ibm:
    type: sarama
    clientConfig:
      bootstrap.servers: broker:9092
      producer.max.message.bytes: 1000
  franz:
    type: franz
    clientConfig:
      bootstrap.servers: broker:9092
      producer.batch.bytes.max: 1000
  confluent_large:
    type: confluent
    clientConfig:
      bootstrap.servers: broker:9092
      message.max.bytes: 5000
  ibm_large:
    type: sarama
    clientConfig:
      bootstrap.servers: broker:9092
      producer.max.message.bytes: 5000
  franz_large:
    type: franz
    clientConfig:
      bootstrap.servers: broker:9092
      producer.batch.bytes.max: 5000`)
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name        string
		inputPath   string
		wantSuccess bool
	}{
		{
			name:        "confluent max message size",
			inputPath:   "/first",
			wantSuccess: false,
		},
		{
			name:        "sarama max message size",
			inputPath:   "/second",
			wantSuccess: false,
		},
		{
			name:        "franz max message size",
			inputPath:   "/third",
			wantSuccess: false,
		},
		{
			name:        "confluent large max message size",
			inputPath:   "/first_large",
			wantSuccess: true,
		},
		{
			name:        "sarama large max message size",
			inputPath:   "/second_large",
			wantSuccess: true,
		},
		{
			name:        "franz large max message size",
			inputPath:   "/third_large",
			wantSuccess: true,
		},
	}

	val := ""
	for range 1000 {
		val += "foo"
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := testutil.NewConsumer(ctx, t, "topic1", "9094")
			defer consumer.Close()
			req := model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: &val}},
				},
			}
			testutil.ProduceSync(ctx, t, krp, tc.inputPath, req, testutil.WithSuccess(tc.wantSuccess))
		})
	}
}

func TestSSL(t *testing.T) {
	ctx := context.Background()

	network, err := network.New(ctx)
	require.NoError(t, err)
	defer network.Remove(ctx)
	broker, err := testutil.NewKafkaSSLContainer(ctx, "broker", "9094", network.Name)
	require.NoError(t, err)
	defer broker.Terminate(ctx)

	caCert := testutil.CopyFromContainer(ctx, t, broker, "/etc/kafka/secrets/ca-cert", "ca-cert")
	defer os.Remove(caCert.Name())
	clientCert := testutil.CopyFromContainer(ctx, t, broker, "/etc/kafka/secrets/client.pem", "client.pem")
	defer os.Remove(clientCert.Name())
	clientKey := testutil.CopyFromContainer(ctx, t, broker, "/etc/kafka/secrets/client.key", "client.key")
	defer os.Remove(clientKey.Name())

	krp, err := testutil.NewKrpContainer(ctx, network.Name, `
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
  fourth:
    routes:
      - topic: topic1
        producer: franz
producers:
  confluent:
    type: confluent
    clientConfig:
      bootstrap.servers: broker:9092
      security.protocol: ssl
      ssl.ca.location: /tmp/ca-cert
      ssl.certificate.location: /tmp/client.pem
      ssl.key.location: /tmp/client.key
      ssl.key.password: test1234
  ibm:
    type: sarama
    clientConfig:
      bootstrap.servers: broker:9092
      net.tls.enable: true
      net.tls.cert.file: /tmp/client.pem
      net.tls.key.file: /tmp/client.key
      net.tls.key.password: test1234
      net.tls.ca.file: /tmp/ca-cert
  segment:
    type: segment
    clientConfig:
      bootstrap.servers: broker:9092
      transport.tls.enable: true
      transport.tls.cert.file: /tmp/client.pem
      transport.tls.key.file: /tmp/client.key
      transport.tls.key.password: test1234
      transport.tls.ca.file: /tmp/ca-cert
  franz:
    type: franz
    clientConfig:
      bootstrap.servers: broker:9092
      tls.enable: true
      tls.cert.file: /tmp/client.pem
      tls.key.file: /tmp/client.key
      tls.key.password: test1234
      tls.ca.file: /tmp/ca-cert
`, testcontainers.ContainerFile{
		HostFilePath:      caCert.Name(),
		ContainerFilePath: "/tmp/ca-cert",
	}, testcontainers.ContainerFile{
		HostFilePath:      clientCert.Name(),
		ContainerFilePath: "/tmp/client.pem",
	}, testcontainers.ContainerFile{
		HostFilePath:      clientKey.Name(),
		ContainerFilePath: "/tmp/client.key",
	})
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name      string
		inputPath string
		inputReq  model.ProduceRequest
		want      map[string][]model.ProduceMessage
	}{
		{
			name:      "confluent ssl client",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
		},
		{
			name:      "sarama ssl client",
			inputPath: "/second",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
		},
		{
			name:      "segment ssl client",
			inputPath: "/third",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
		},
		{
			name:      "franz ssl client",
			inputPath: "/fourth",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			consumers := make(map[string]*kafka.Consumer)
			for topic := range tc.want {
				consumer := testutil.NewConsumer(ctx, t, topic, "9094")
				defer consumer.Close()
				consumers[topic] = consumer
			}
			testutil.ProduceSync(ctx, t, krp, tc.inputPath, tc.inputReq)
			for topic, msgs := range tc.want {
				consumer := consumers[topic]
				testutil.CheckReceived(t, consumer, msgs)
			}
		})
	}
}

func TestSASLPlain(t *testing.T) {
	ctx := context.Background()

	network, err := network.New(ctx)
	require.NoError(t, err)
	defer network.Remove(ctx)
	broker, err := testutil.NewKafkaSASLPlainContainer(ctx, "broker", "9094", network.Name)
	require.NoError(t, err)
	defer broker.Terminate(ctx)

	krp, err := testutil.NewKrpContainer(ctx, network.Name, `
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
  fourth:
    routes:
      - topic: topic1
        producer: franz
producers:
  confluent:
    type: confluent
    clientConfig:
      bootstrap.servers: broker:9092
      security.protocol: sasl_plaintext
      sasl.mechanism: PLAIN
      sasl.username: test
      sasl.password: test1234
  ibm:
    type: sarama
    clientConfig:
      bootstrap.servers: broker:9092
      net.sasl.enable: true
      net.sasl.mechanism: PLAIN
      net.sasl.user: test
      net.sasl.password: test1234
  segment:
    type: segment
    clientConfig:
      bootstrap.servers: broker:9092
      transport.sasl.mechanism: plain
      transport.sasl.username: test
      transport.sasl.password: test1234
  franz:
    type: franz
    clientConfig:
      bootstrap.servers: broker:9092
      sasl.enable: true
      sasl.mechanism: plain
      sasl.plain.username: test
      sasl.plain.password: test1234`)
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name      string
		inputPath string
		inputReq  model.ProduceRequest
		want      map[string][]model.ProduceMessage
	}{
		{
			name:      "confluent sasl plain client",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
		},
		{
			name:      "sarama sasl plain client",
			inputPath: "/second",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
		},
		{
			name:      "segment sasl plain client",
			inputPath: "/third",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
		},
		{
			name:      "franz sasl plain client",
			inputPath: "/fourth",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: testutil.Ptr("foo")}},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			consumers := make(map[string]*kafka.Consumer)
			for topic := range tc.want {
				consumer := testutil.NewConsumer(ctx, t, topic, "9094")
				defer consumer.Close()
				consumers[topic] = consumer
			}
			testutil.ProduceSync(ctx, t, krp, tc.inputPath, tc.inputReq)
			for topic, msgs := range tc.want {
				consumer := consumers[topic]
				testutil.CheckReceived(t, consumer, msgs)
			}
		})
	}
}
