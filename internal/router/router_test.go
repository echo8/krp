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
		name        string
		inputCfg    string
		inputMsgs   []model.ProduceMessage
		wantBatches map[string]model.MessageBatch
		wantResults []model.ProduceResult
	}{
		{
			name: "single topic, single producer",
			inputCfg: `
			routes:
				- topic: foo
					producer: prodOne
			`,
			inputMsgs: []model.ProduceMessage{{Value: util.Ptr("bar")}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
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
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
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
			wantBatches: map[string]model.MessageBatch{
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
			wantResults: []model.ProduceResult{{Success: true}},
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
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: util.Ptr("bar")}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
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
			wantBatches: map[string]model.MessageBatch{
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
			wantResults: []model.ProduceResult{{Success: true}},
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
			wantBatches: map[string]model.MessageBatch{
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
			wantResults: []model.ProduceResult{{Success: true}},
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
			wantBatches: map[string]model.MessageBatch{
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
			wantResults: []model.ProduceResult{{Success: true}},
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
			wantBatches: map[string]model.MessageBatch{
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
							Pos: 1,
						},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true, Pos: 0}, {Success: true, Pos: 1}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadEndpointConfig(t, tc.inputCfg)
			ps := createProducers()
			router, err := New(cfg, ps)
			require.NoError(t, err)
			if cfg.Async {
				err := router.SendAsync(context.Background(), tc.inputMsgs)
				require.NoError(t, err)
			} else {
				results, err := router.SendSync(context.Background(), tc.inputMsgs)
				require.NoError(t, err)
				require.Equal(t, tc.wantResults, results)
			}
			for pid, wantBatch := range tc.wantBatches {
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
