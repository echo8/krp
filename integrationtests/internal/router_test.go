package integrationtest

import (
	"context"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/echo8/krp/model"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/network"
)

func TestRouterEndToEnd(t *testing.T) {
	ctx := context.Background()
	testcases := []struct {
		name         string
		inputCfg     string
		inputReq     model.ProduceRequest
		inputHeaders map[string]string
		want         map[string]map[string][]model.ProduceMessage
	}{
		{
			name: "single topic, single producer",
			inputCfg: `
			routes:
				- topic: foo
					producer: prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
				},
			},
		},
		{
			name: "multiple topic, single producer",
			inputCfg: `
			routes:
				- topic:
						- foo
						- bar
					producer: prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
					"bar": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
				},
			},
		},
		{
			name: "multiple topic, multiple producer",
			inputCfg: `
			routes:
				- topic:
						- foo
						- bar
					producer:
						- prodOne
						- prodTwo
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
					"bar": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
				},
				"prodTwo": {
					"foo": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
					"bar": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
				},
			},
		},
		{
			name: "multiple routes, multiple topic, single producer",
			inputCfg: `
			routes:
				- topic:
						- foo
					producer:
						- prodOne
				- topic:
						- bar
					producer:
						- prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
					"bar": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
				},
			},
		},
		{
			name: "multiple routes, single topic, multiple producer",
			inputCfg: `
			routes:
				- topic:
						- foo
					producer:
						- prodOne
				- topic:
						- foo
					producer:
						- prodTwo
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
				},
				"prodTwo": {
					"foo": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
				},
			},
		},
		{
			name: "two disjoint routes",
			inputCfg: `
			routes:
				- topic:
						- foo
					producer:
						- prodOne
				- topic:
						- bar
					producer:
						- prodTwo
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{Value: &model.ProduceData{String: Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
				},
				"prodTwo": {
					"bar": {
						{Value: &model.ProduceData{String: Ptr("bar")}},
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			network, err := network.New(ctx)
			require.NoError(t, err)

			brokerPorts := make(map[string]string, 2)
			broker1, err := NewKafkaContainer(ctx, "prodOne", "9094", network.Name)
			require.NoError(t, err)
			brokerPorts["prodOne"] = "9094"
			broker2, err := NewKafkaContainer(ctx, "prodTwo", "9095", network.Name)
			require.NoError(t, err)
			brokerPorts["prodTwo"] = "9095"
			defer broker1.Terminate(ctx)
			defer broker2.Terminate(ctx)
			krp, err := NewKrpContainer(ctx, network.Name, `addr: ":8080"
endpoints:
  first:
`+formatRouteCfg(tc.inputCfg)+`
producers:
  prodOne:
    type: kafka
    clientConfig:
      bootstrap.servers: prodOne:9092
  prodTwo:
    type: kafka
    clientConfig:
      bootstrap.servers: prodTwo:9092
`)
			require.NoError(t, err)
			defer krp.Terminate(ctx)

			consumers := make(map[string]map[string]*kafka.Consumer)
			for brokerName, topicMap := range tc.want {
				consumers[brokerName] = make(map[string]*kafka.Consumer, len(topicMap))
				for topic := range topicMap {
					consumer := NewConsumer(ctx, t, topic, brokerPorts[brokerName])
					defer consumer.Close()
					consumers[brokerName][topic] = consumer
				}
			}
			ProduceSync(ctx, t, krp, "/first", tc.inputReq, WithHeaders(tc.inputHeaders))
			for brokerName, topicMap := range tc.want {
				for topic, msgs := range topicMap {
					consumer := consumers[brokerName][topic]
					CheckReceived(t, consumer, msgs)
				}
			}
		})
	}
}

func formatRouteCfg(cfg string) string {
	newCfg := strings.TrimLeft(cfg, "\n")
	newCfg = strings.ReplaceAll(newCfg, "\t", "  ")
	return newCfg
}
