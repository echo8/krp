package router

import (
	"context"
	"echo8/kafka-rest-producer/internal/config"
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/producer"
	"echo8/kafka-rest-producer/internal/util"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestRouter(t *testing.T) {
	tests := []struct {
		name      string
		inputCfg  string
		inputMsgs []model.ProduceMessage
		want      map[string]model.MessageBatch
	}{
		{
			name: "single topic, single producer",
			inputCfg: `
			routes:
				- topic: foo
					producer: prodOne
			`,
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
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
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
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
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
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
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
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
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
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
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "bar", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
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
			inputMsgs: []model.ProduceMessage{
				{
					Key:     util.Ptr("foo"),
					Value:   util.Ptr("bar"),
					Headers: map[string]string{"my-key": "baz"},
				},
			},
			want: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo-foo",
							Message: &model.ProduceMessage{
								Key:     util.Ptr("foo"),
								Value:   util.Ptr("bar"),
								Headers: map[string]string{"my-key": "baz"},
							},
						},
						{
							Topic: "bar-baz",
							Message: &model.ProduceMessage{
								Key:     util.Ptr("foo"),
								Value:   util.Ptr("bar"),
								Headers: map[string]string{"my-key": "baz"},
							},
						},
						{
							Topic: "foo-",
							Message: &model.ProduceMessage{
								Key:     util.Ptr("foo"),
								Value:   util.Ptr("bar"),
								Headers: map[string]string{"my-key": "baz"},
							},
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
			inputMsgs: []model.ProduceMessage{
				{
					Value:   util.Ptr("foo"),
					Headers: map[string]string{"pid": "prodOne"},
				},
				{
					Value:   util.Ptr("bar"),
					Headers: map[string]string{"pid": "prodTwo"},
				},
			},
			want: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo",
							Message: &model.ProduceMessage{
								Value:   util.Ptr("foo"),
								Headers: map[string]string{"pid": "prodOne"},
							},
						},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo",
							Message: &model.ProduceMessage{
								Value:   util.Ptr("bar"),
								Headers: map[string]string{"pid": "prodTwo"},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadEndpointConfig(t, tc.inputCfg)
			ps := createProducers()
			router, err := New(cfg, ps)
			require.NoError(t, err)
			if cfg.Async {
				router.SendAsync(context.Background(), tc.inputMsgs)
			} else {
				router.SendSync(context.Background(), tc.inputMsgs)
			}
			for pid, wantBatch := range tc.want {
				actual := ps.GetProducer(config.ProducerId(pid)).(*producer.TestProducer).Batch
				require.ElementsMatch(t, wantBatch.Messages, actual.Messages)
			}
		})
	}
}

func loadEndpointConfig(t *testing.T, str string) *config.EndpointConfig {
	noTabs := strings.ReplaceAll(str, "\t", "  ")
	cfg := &config.EndpointConfig{}
	err := yaml.Unmarshal([]byte(noTabs), cfg)
	require.NoError(t, err)
	return cfg
}

func createProducers() producer.Service {
	pMap := make(map[config.ProducerId]producer.Producer)
	pMap[config.ProducerId("prodOne")] = &producer.TestProducer{}
	pMap[config.ProducerId("prodTwo")] = &producer.TestProducer{}
	ps, _ := producer.NewService(pMap)
	return ps
}
