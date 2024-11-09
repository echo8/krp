package e2e

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/echo8/krp/model"
	"github.com/echo8/krp/tests/internal/testutil"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/network"
)

func TestSync(t *testing.T) {
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
  "another/first":
    routes:
      - topic: topic1
        producer: segment
producers:
  confluent:
    type: confluent
    clientConfig:
      bootstrap.servers: broker:9092
  ibm:
    type: sarama
    clientConfig:
      bootstrap.servers: broker:9092
  segment:
    type: segment
    clientConfig:
      bootstrap.servers: broker:9092
      batch.timeout: 10ms`)
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name      string
		inputPath string
		inputReq  model.ProduceRequest
		want      map[string][]model.ProduceMessage
	}{
		{
			name:      "confluent string value",
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
			name:      "sarama string value",
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
			name:      "segment string value",
			inputPath: "/another/first",
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
			name:      "confluent string key and value",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
		},
		{
			name:      "sarama string key and value",
			inputPath: "/second",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
		},
		{
			name:      "segment string key and value",
			inputPath: "/another/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
		},
		{
			name:      "confluent bytes key and value",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{Bytes: testutil.Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: testutil.Ptr("YmFy")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{Bytes: testutil.Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: testutil.Ptr("YmFy")},
					},
				},
			},
		},
		{
			name:      "sarama bytes key and value",
			inputPath: "/second",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{Bytes: testutil.Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: testutil.Ptr("YmFy")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{Bytes: testutil.Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: testutil.Ptr("YmFy")},
					},
				},
			},
		},
		{
			name:      "segment bytes key and value",
			inputPath: "/another/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{Bytes: testutil.Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: testutil.Ptr("YmFy")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{Bytes: testutil.Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: testutil.Ptr("YmFy")},
					},
				},
			},
		},
		{
			name:      "confluent headers",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value: &model.ProduceData{String: testutil.Ptr("foo")},
						Headers: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value: &model.ProduceData{String: testutil.Ptr("foo")},
						Headers: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
		},
		{
			name:      "sarama headers",
			inputPath: "/second",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value: &model.ProduceData{String: testutil.Ptr("foo")},
						Headers: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value: &model.ProduceData{String: testutil.Ptr("foo")},
						Headers: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
		},
		{
			name:      "segment headers",
			inputPath: "/another/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value: &model.ProduceData{String: testutil.Ptr("foo")},
						Headers: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value: &model.ProduceData{String: testutil.Ptr("foo")},
						Headers: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
		},
		{
			name:      "confluent timestamp",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value:     &model.ProduceData{String: testutil.Ptr("foo")},
						Timestamp: testutil.Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value:     &model.ProduceData{String: testutil.Ptr("foo")},
						Timestamp: testutil.Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
				},
			},
		},
		{
			name:      "sarama timestamp",
			inputPath: "/second",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value:     &model.ProduceData{String: testutil.Ptr("foo")},
						Timestamp: testutil.Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value:     &model.ProduceData{String: testutil.Ptr("foo")},
						Timestamp: testutil.Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
				},
			},
		},
		{
			name:      "segment timestamp",
			inputPath: "/another/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value:     &model.ProduceData{String: testutil.Ptr("foo")},
						Timestamp: testutil.Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value:     &model.ProduceData{String: testutil.Ptr("foo")},
						Timestamp: testutil.Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
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

func TestAsync(t *testing.T) {
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
producers:
  confluent:
    type: confluent
    clientConfig:
      bootstrap.servers: broker:9092
`)
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name      string
		inputPath string
		inputReq  model.ProduceRequest
		want      map[string][]model.ProduceMessage
	}{
		{
			name:      "async string value",
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
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			consumers := make(map[string]*kafka.Consumer)
			for topic := range tc.want {
				consumer := testutil.NewConsumer(ctx, t, topic, "9094")
				defer consumer.Close()
				consumers[topic] = consumer
			}
			testutil.ProduceAsync(ctx, t, krp, tc.inputPath, tc.inputReq)
			for topic, msgs := range tc.want {
				consumer := consumers[topic]
				testutil.CheckReceived(t, consumer, msgs)
			}
		})
	}
}

func TestProduceError(t *testing.T) {
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
producers:
  confluent:
    type: confluent
    clientConfig:
      bootstrap.servers: broker:9092
`)
	require.NoError(t, err)
	defer krp.Terminate(ctx)

	testcases := []struct {
		name      string
		inputPath string
		inputReq  any
		want      model.ProduceErrorResponse
	}{
		{
			name:      "empty messages",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{},
			},
			want: model.ProduceErrorResponse{Error: "'messages' field cannot be empty"},
		},
		{
			name:      "empty headers",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value:   &model.ProduceData{String: testutil.Ptr("foo")},
						Headers: map[string]string{},
					},
				},
			},
			want: model.ProduceErrorResponse{Error: "'headers' field cannot be empty"},
		},
		{
			name:      "empty timestamp",
			inputPath: "/first",
			inputReq: map[string]any{
				"messages": []map[string]any{
					{
						"value":     map[string]any{"string": "foo"},
						"timestamp": "",
					},
				},
			},
			want: model.ProduceErrorResponse{Error: "failed to parse 'timestamp' field, parsing time \"\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"\" as \"2006\""},
		},
		{
			name:      "no messages",
			inputPath: "/first",
			inputReq:  model.ProduceRequest{},
			want:      model.ProduceErrorResponse{Error: "'messages' field is required"},
		},
		{
			name:      "message with nothing",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{},
				},
			},
			want: model.ProduceErrorResponse{Error: "'value' field is required"},
		},
		{
			name:      "header with null value",
			inputPath: "/first",
			inputReq: map[string]any{
				"messages": []map[string]any{
					{
						"value":   map[string]any{"string": "foo"},
						"headers": map[string]any{"foo": nil},
					},
				},
			},
			want: model.ProduceErrorResponse{Error: "'headers[foo]' must not be null or blank"},
		},
		{
			name:      "header with blank value",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value:   &model.ProduceData{String: testutil.Ptr("foo")},
						Headers: map[string]string{"foo": ""},
					},
				},
			},
			want: model.ProduceErrorResponse{Error: "'headers[foo]' must not be null or blank"},
		},
		{
			name:      "value is null",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value: nil,
					},
				},
			},
			want: model.ProduceErrorResponse{Error: "'value' field is required"},
		},
		{
			name:      "key only",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key: &model.ProduceData{String: testutil.Ptr("foo")},
					},
				},
			},
			want: model.ProduceErrorResponse{Error: "'value' field is required"},
		},
		{
			name:      "null string key",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: nil},
						Value: &model.ProduceData{String: testutil.Ptr("foo")},
					},
				},
			},
			want: model.ProduceErrorResponse{Error: "'string' OR 'bytes' field must be specified"},
		},
		{
			name:      "headers only",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Headers: map[string]string{"foo": "bar"},
					},
				},
			},
			want: model.ProduceErrorResponse{Error: "'value' field is required"},
		},
		{
			name:      "timestamp only",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Timestamp: testutil.Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
				},
			},
			want: model.ProduceErrorResponse{Error: "'value' field is required"},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			res, statusCode := testutil.ProduceError(ctx, t, krp, tc.inputPath, tc.inputReq)
			require.Equal(t, http.StatusBadRequest, statusCode)
			require.Equal(t, tc.want, res)
		})
	}
}
