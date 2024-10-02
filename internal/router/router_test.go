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
					producer: syncProd
			`,
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"syncProd": {
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
					producer: syncProd
			`,
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"syncProd": {
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
						- syncProd
						- asyncProd
			`,
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"syncProd": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
					},
				},
				"asyncProd": {
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
						- syncProd
				- topic:
						- bar
					producer:
						- syncProd
			`,
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"syncProd": {
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
						- syncProd
				- topic:
						- foo
					producer:
						- asyncProd
			`,
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"syncProd": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
					},
				},
				"asyncProd": {
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
						- syncProd
				- topic:
						- bar
					producer:
						- asyncProd
			`,
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			want: map[string]model.MessageBatch{
				"syncProd": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
					},
				},
				"asyncProd": {
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
						- syncProd
			`,
			inputMsgs: []model.ProduceMessage{
				{
					Key:     util.Ptr("foo"),
					Value:   util.Ptr("bar"),
					Headers: map[string]string{"my-key": "baz"},
				},
			},
			want: map[string]model.MessageBatch{
				"syncProd": {
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
					Headers: map[string]string{"pid": "syncProd"},
				},
				{
					Value:   util.Ptr("bar"),
					Headers: map[string]string{"pid": "asyncProd"},
				},
			},
			want: map[string]model.MessageBatch{
				"syncProd": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo",
							Message: &model.ProduceMessage{
								Value:   util.Ptr("foo"),
								Headers: map[string]string{"pid": "syncProd"},
							},
						},
					},
				},
				"asyncProd": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo",
							Message: &model.ProduceMessage{
								Value:   util.Ptr("bar"),
								Headers: map[string]string{"pid": "asyncProd"},
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
			router.Send(context.Background(), tc.inputMsgs)
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
	pMap[config.ProducerId("syncProd")] = &producer.TestProducer{}
	pMap[config.ProducerId("asyncProd")] = &producer.TestProducer{}
	ps, _ := producer.NewService(pMap)
	return ps
}
