package integrationtest

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/echo8/krp/model"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/network"
)

func TestEndToEnd(t *testing.T) {
	ctx := context.Background()

	network, err := network.New(ctx)
	require.NoError(t, err)
	_, err = NewKafkaContainer(ctx, network.Name)
	require.NoError(t, err)
	krp, err := NewKrpContainer(ctx, network.Name)
	require.NoError(t, err)

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
					{Value: &model.ProduceData{String: Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: Ptr("foo")}},
				},
			},
		},
		{
			name:      "sarama string value",
			inputPath: "/second",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: Ptr("foo")}},
				},
			},
		},
		{
			name:      "segment string value",
			inputPath: "/another/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: Ptr("foo")}},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{Value: &model.ProduceData{String: Ptr("foo")}},
				},
			},
		},
		{
			name:      "confluent string key and value",
			inputPath: "/first",
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar")},
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
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar")},
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
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{String: Ptr("foo")},
						Value: &model.ProduceData{String: Ptr("bar")},
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
						Key:   &model.ProduceData{Bytes: Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: Ptr("YmFy")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{Bytes: Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: Ptr("YmFy")},
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
						Key:   &model.ProduceData{Bytes: Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: Ptr("YmFy")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{Bytes: Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: Ptr("YmFy")},
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
						Key:   &model.ProduceData{Bytes: Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: Ptr("YmFy")},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Key:   &model.ProduceData{Bytes: Ptr("Zm9v")},
						Value: &model.ProduceData{Bytes: Ptr("YmFy")},
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
						Value: &model.ProduceData{String: Ptr("foo")},
						Headers: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value: &model.ProduceData{String: Ptr("foo")},
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
						Value: &model.ProduceData{String: Ptr("foo")},
						Headers: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value: &model.ProduceData{String: Ptr("foo")},
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
						Value: &model.ProduceData{String: Ptr("foo")},
						Headers: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value: &model.ProduceData{String: Ptr("foo")},
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
						Value:     &model.ProduceData{String: Ptr("foo")},
						Timestamp: Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value:     &model.ProduceData{String: Ptr("foo")},
						Timestamp: Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
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
						Value:     &model.ProduceData{String: Ptr("foo")},
						Timestamp: Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value:     &model.ProduceData{String: Ptr("foo")},
						Timestamp: Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
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
						Value:     &model.ProduceData{String: Ptr("foo")},
						Timestamp: Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
				},
			},
			want: map[string][]model.ProduceMessage{
				"topic1": {
					{
						Value:     &model.ProduceData{String: Ptr("foo")},
						Timestamp: Ptr(time.Date(2000, 1, 1, 1, 1, 1, 0, time.Local)),
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			consumers := make(map[string]*kafka.Consumer)
			for topic := range tc.want {
				consumer := NewConsumer(t, topic, "localhost:9094")
				defer consumer.Close()
				consumers[topic] = consumer
			}
			ProduceSync(ctx, t, krp, tc.inputPath, tc.inputReq)
			for topic, msgs := range tc.want {
				consumer := consumers[topic]
				CheckReceived(t, consumer, msgs)
			}
		})
	}
}
