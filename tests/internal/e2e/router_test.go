package e2e

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/echo8/krp/model"
	"github.com/echo8/krp/tests/internal/testutil"
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
					{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
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
					{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
					},
					"bar": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
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
					{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
					},
					"bar": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
					},
				},
				"prodTwo": {
					"foo": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
					},
					"bar": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
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
					{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
					},
					"bar": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
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
					{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
					},
				},
				"prodTwo": {
					"foo": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
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
					{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
					},
				},
				"prodTwo": {
					"bar": {
						{Value: &model.ProduceData{String: testutil.Ptr("bar")}},
					},
				},
			},
		},
		{
			name: "templated topics",
			inputCfg: `
			routes:
				- topic:
						- foo-${msg:key}
						- bar-${msg:header.my-key}
						- foo-${msg:header.my-other-key}
					producer:
						- prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:     &model.ProduceData{String: testutil.Ptr("foo")},
						Value:   &model.ProduceData{String: testutil.Ptr("bar")},
						Headers: map[string]string{"my-key": "baz"},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo-foo": {
						{
							Key:     &model.ProduceData{String: testutil.Ptr("foo")},
							Value:   &model.ProduceData{String: testutil.Ptr("bar")},
							Headers: map[string]string{"my-key": "baz"},
						},
					},
					"bar-baz": {
						{
							Key:     &model.ProduceData{String: testutil.Ptr("foo")},
							Value:   &model.ProduceData{String: testutil.Ptr("bar")},
							Headers: map[string]string{"my-key": "baz"},
						},
					},
					"foo-": {
						{
							Key:     &model.ProduceData{String: testutil.Ptr("foo")},
							Value:   &model.ProduceData{String: testutil.Ptr("bar")},
							Headers: map[string]string{"my-key": "baz"},
						},
					},
				},
			},
		},
		{
			name: "templated producer",
			inputCfg: `
			routes:
				- topic:
						- foo
					producer:
						- ${msg:header.pid}
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value:   &model.ProduceData{String: testutil.Ptr("foo")},
						Headers: map[string]string{"pid": "prodOne"},
					},
					{
						Value:   &model.ProduceData{String: testutil.Ptr("bar")},
						Headers: map[string]string{"pid": "prodTwo"},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Value:   &model.ProduceData{String: testutil.Ptr("foo")},
							Headers: map[string]string{"pid": "prodOne"},
						},
					},
				},
				"prodTwo": {
					"foo": {
						{
							Value:   &model.ProduceData{String: testutil.Ptr("bar")},
							Headers: map[string]string{"pid": "prodTwo"},
						},
					},
				},
			},
		},
		{
			name: "matched single topic, single producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic: foo
					producer: prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "matched multiple topic, single producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic:
						- foo
						- bar
					producer: prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
					"bar": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "matched multiple topic, multiple producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic:
						- foo
						- bar
					producer:
						- prodOne
						- prodTwo
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
					"bar": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
				"prodTwo": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
					"bar": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "matched multiple routes, multiple topic, single producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic:
						- foo
					producer:
						- prodOne
				- match: "message.key.string == 'foo'"
					topic:
						- bar
					producer:
						- prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
					"bar": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "matched multiple routes, single topic, multiple producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic:
						- foo
					producer:
						- prodOne
				- match: "message.key.string == 'foo'"
					topic:
						- foo
					producer:
						- prodTwo
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
				"prodTwo": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "matched two disjoint routes",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic:
						- foo
					producer:
						- prodOne
				- match: "message.key.string == 'foo'"
					topic:
						- bar
					producer:
						- prodTwo
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
				"prodTwo": {
					"bar": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "matched templated topics",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic:
						- foo-${msg:key}
						- bar-${msg:header.my-key}
						- foo-${msg:header.my-other-key}
					producer:
						- prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:     &model.ProduceData{String: testutil.Ptr("foo")},
						Value:   &model.ProduceData{String: testutil.Ptr("bar")},
						Headers: map[string]string{"my-key": "baz"},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo-foo": {
						{
							Key:     &model.ProduceData{String: testutil.Ptr("foo")},
							Value:   &model.ProduceData{String: testutil.Ptr("bar")},
							Headers: map[string]string{"my-key": "baz"},
						},
					},
					"bar-baz": {
						{
							Key:     &model.ProduceData{String: testutil.Ptr("foo")},
							Value:   &model.ProduceData{String: testutil.Ptr("bar")},
							Headers: map[string]string{"my-key": "baz"},
						},
					},
					"foo-": {
						{
							Key:     &model.ProduceData{String: testutil.Ptr("foo")},
							Value:   &model.ProduceData{String: testutil.Ptr("bar")},
							Headers: map[string]string{"my-key": "baz"},
						},
					},
				},
			},
		},
		{
			name: "matched templated producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic:
						- foo
					producer:
						- ${msg:header.pid}
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:     &model.ProduceData{String: testutil.Ptr("foo")},
						Value:   &model.ProduceData{String: testutil.Ptr("bar")},
						Headers: map[string]string{"pid": "prodOne"},
					},
					{
						Key:     &model.ProduceData{String: testutil.Ptr("foo")},
						Value:   &model.ProduceData{String: testutil.Ptr("bar")},
						Headers: map[string]string{"pid": "prodTwo"},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:     &model.ProduceData{String: testutil.Ptr("foo")},
							Value:   &model.ProduceData{String: testutil.Ptr("bar")},
							Headers: map[string]string{"pid": "prodOne"},
						},
					},
				},
				"prodTwo": {
					"foo": {
						{
							Key:     &model.ProduceData{String: testutil.Ptr("foo")},
							Value:   &model.ProduceData{String: testutil.Ptr("bar")},
							Headers: map[string]string{"pid": "prodTwo"},
						},
					},
				},
			},
		},
		{
			name: "different matches, multiple routes, multiple topic, single producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo1'"
					topic:
						- foo
					producer:
						- prodOne
				- match: "message.key.string == 'foo2'"
					topic:
						- bar
					producer:
						- prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo1")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo2")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo1")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
					"bar": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo2")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "different matches, multiple routes, multiple topic, multiple producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo1'"
					topic:
						- foo
					producer:
						- prodOne
				- match: "message.key.string == 'foo2'"
					topic:
						- bar
					producer:
						- prodTwo
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo1")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo2")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo1")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
				"prodTwo": {
					"bar": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo2")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "matches and match all, multiple routes, multiple topic, multiple producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo1'"
					topic:
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
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo1")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo2")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo1")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
				"prodTwo": {
					"bar": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo1")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo2")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "unmatched, multiple routes, multiple topic, single producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo1'"
					topic:
						- foo
					producer:
						- prodOne
				- match: "message.key.string == 'foo2'"
					topic:
						- bar
					producer:
						- prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo1")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo2")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo3")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo1")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
					"bar": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo2")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "matched http header, single topic, single producer",
			inputCfg: `
			routes:
				- match: "httpHeader('Foo1') == 'bar1'"
					topic: foo
					producer: prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:   &model.ProduceData{String: testutil.Ptr("foo")},
						Value: &model.ProduceData{String: testutil.Ptr("bar")},
					},
				},
			},
			inputHeaders: map[string]string{"Foo1": "bar1"},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {
						{
							Key:   &model.ProduceData{String: testutil.Ptr("foo")},
							Value: &model.ProduceData{String: testutil.Ptr("bar")},
						},
					},
				},
			},
		},
		{
			name: "unmatched http header, single topic, single producer",
			inputCfg: `
			routes:
				- match: "httpHeader('Foo1') == 'bar1'"
					topic: foo
					producer: prodOne
			`,
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Key:     &model.ProduceData{String: testutil.Ptr("foo")},
						Value:   &model.ProduceData{String: testutil.Ptr("bar")},
						Headers: map[string]string{"baz1": "baz2"},
					},
				},
			},
			inputHeaders: map[string]string{"Foo1": "bar2"},
			want: map[string]map[string][]model.ProduceMessage{
				"prodOne": {
					"foo": {},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			network, err := network.New(ctx)
			require.NoError(t, err)

			brokerPorts := make(map[string]string, 2)
			broker1, err := testutil.NewKafkaContainer(ctx, "prodOne", "9094", network.Name)
			require.NoError(t, err)
			brokerPorts["prodOne"] = "9094"
			defer broker1.Terminate(ctx)

			broker2, err := testutil.NewKafkaContainer(ctx, "prodTwo", "9095", network.Name)
			require.NoError(t, err)
			brokerPorts["prodTwo"] = "9095"
			defer broker2.Terminate(ctx)

			krp, err := testutil.NewKrpContainer(ctx, network.Name, `addr: ":8080"
endpoints:
  first:
`+testutil.FormatCfg(tc.inputCfg)+`
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
					consumer := testutil.NewConsumer(ctx, t, topic, brokerPorts[brokerName])
					defer consumer.Close()
					consumers[brokerName][topic] = consumer
				}
			}
			testutil.ProduceSync(ctx, t, krp, "/first", tc.inputReq, testutil.WithHeaders(tc.inputHeaders))
			for brokerName, topicMap := range tc.want {
				for topic, msgs := range topicMap {
					consumer := consumers[brokerName][topic]
					testutil.CheckReceived(t, consumer, msgs)
				}
			}
		})
	}
}
