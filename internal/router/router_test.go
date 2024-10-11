package router

import (
	"context"
	"echo8/kafka-rest-producer/internal/config"
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/producer"
	"echo8/kafka-rest-producer/internal/util"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestRouter(t *testing.T) {
	tests := []struct {
		name         string
		inputCfg     string
		inputMsgs    []model.ProduceMessage
		inputHttpReq http.Request
		wantBatches  map[string]model.MessageBatch
		wantResults  []model.ProduceResult
	}{
		{
			name: "single topic, single producer",
			inputCfg: `
			routes:
				- topic: foo
					producer: prodOne
			`,
			inputMsgs: []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
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
			inputMsgs: []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
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
			inputMsgs: []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
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
			inputMsgs: []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
						{Topic: "bar", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
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
			inputMsgs: []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
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
			inputMsgs: []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "bar", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
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
					Key:     &model.ProduceData{String: util.Ptr("foo")},
					Value:   &model.ProduceData{String: util.Ptr("bar")},
					Headers: map[string]string{"my-key": "baz"},
				},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo-foo",
							Message: &model.ProduceMessage{
								Key:     &model.ProduceData{String: util.Ptr("foo")},
								Value:   &model.ProduceData{String: util.Ptr("bar")},
								Headers: map[string]string{"my-key": "baz"},
							},
						},
						{
							Topic: "bar-baz",
							Message: &model.ProduceMessage{
								Key:     &model.ProduceData{String: util.Ptr("foo")},
								Value:   &model.ProduceData{String: util.Ptr("bar")},
								Headers: map[string]string{"my-key": "baz"},
							},
						},
						{
							Topic: "foo-",
							Message: &model.ProduceMessage{
								Key:     &model.ProduceData{String: util.Ptr("foo")},
								Value:   &model.ProduceData{String: util.Ptr("bar")},
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
					Value:   &model.ProduceData{String: util.Ptr("foo")},
					Headers: map[string]string{"pid": "prodOne"},
				},
				{
					Value:   &model.ProduceData{String: util.Ptr("bar")},
					Headers: map[string]string{"pid": "prodTwo"},
				},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo",
							Message: &model.ProduceMessage{
								Value:   &model.ProduceData{String: util.Ptr("foo")},
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
								Value:   &model.ProduceData{String: util.Ptr("bar")},
								Headers: map[string]string{"pid": "prodTwo"},
							},
							Pos: 1,
						},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true, Pos: 0}, {Success: true, Pos: 1}},
		},
		{
			name: "single topic, single producer failure",
			inputCfg: `
			routes:
				- topic: foo
					producer: prodFails
			`,
			inputMsgs: []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodFails": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: false}},
		},
		{
			name: "single topic, multiple producer failure",
			inputCfg: `
			routes:
				- topic: foo
					producer:
						- prodOne
						- prodFails
			`,
			inputMsgs: []model.ProduceMessage{
				{Value: &model.ProduceData{String: util.Ptr("bar1")}},
				{Value: &model.ProduceData{String: util.Ptr("bar2")}},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar1")}}, Pos: 0},
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar2")}}, Pos: 1},
					},
				},
				"prodFails": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar1")}}, Pos: 0},
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar2")}}, Pos: 1},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: false, Pos: 0}, {Success: false, Pos: 1}},
		},
		{
			name: "two disjoint routes, one failure",
			inputCfg: `
			routes:
				- topic:
						- foo
					producer:
						- prodOne
				- topic:
						- bar
					producer:
						- prodFails
			`,
			inputMsgs: []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
				"prodFails": {
					Messages: []model.TopicAndMessage{
						{Topic: "bar", Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: false}},
		},
		{
			name: "templated producer failure",
			inputCfg: `
			routes:
				- topic:
						- foo
					producer:
						- ${msg:header.pid}
			`,
			inputMsgs: []model.ProduceMessage{
				{
					Value:   &model.ProduceData{String: util.Ptr("foo")},
					Headers: map[string]string{"pid": "prodOne"},
				},
				{
					Value:   &model.ProduceData{String: util.Ptr("bar")},
					Headers: map[string]string{"pid": "prodFails"},
				},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo",
							Message: &model.ProduceMessage{
								Value:   &model.ProduceData{String: util.Ptr("foo")},
								Headers: map[string]string{"pid": "prodOne"},
							},
						},
					},
				},
				"prodFails": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo",
							Message: &model.ProduceMessage{
								Value:   &model.ProduceData{String: util.Ptr("bar")},
								Headers: map[string]string{"pid": "prodFails"},
							},
							Pos: 1,
						},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true, Pos: 0}, {Success: false, Pos: 1}},
		},
		// --------- matcher tests ---------
		{
			name: "matched single topic, single producer",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic: foo
					producer: prodOne
			`,
			inputMsgs: []model.ProduceMessage{{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
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
			inputMsgs: []model.ProduceMessage{{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
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
			inputMsgs: []model.ProduceMessage{{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
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
			inputMsgs: []model.ProduceMessage{{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
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
			inputMsgs: []model.ProduceMessage{{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
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
			inputMsgs: []model.ProduceMessage{{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
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
			inputMsgs: []model.ProduceMessage{
				{
					Key:     &model.ProduceData{String: util.Ptr("foo")},
					Value:   &model.ProduceData{String: util.Ptr("bar")},
					Headers: map[string]string{"my-key": "baz"},
				},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo-foo",
							Message: &model.ProduceMessage{
								Key:     &model.ProduceData{String: util.Ptr("foo")},
								Value:   &model.ProduceData{String: util.Ptr("bar")},
								Headers: map[string]string{"my-key": "baz"},
							},
						},
						{
							Topic: "bar-baz",
							Message: &model.ProduceMessage{
								Key:     &model.ProduceData{String: util.Ptr("foo")},
								Value:   &model.ProduceData{String: util.Ptr("bar")},
								Headers: map[string]string{"my-key": "baz"},
							},
						},
						{
							Topic: "foo-",
							Message: &model.ProduceMessage{
								Key:     &model.ProduceData{String: util.Ptr("foo")},
								Value:   &model.ProduceData{String: util.Ptr("bar")},
								Headers: map[string]string{"my-key": "baz"},
							},
						},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
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
			inputMsgs: []model.ProduceMessage{
				{
					Key:     &model.ProduceData{String: util.Ptr("foo")},
					Value:   &model.ProduceData{String: util.Ptr("foo")},
					Headers: map[string]string{"pid": "prodOne"},
				},
				{
					Key:     &model.ProduceData{String: util.Ptr("foo")},
					Value:   &model.ProduceData{String: util.Ptr("bar")},
					Headers: map[string]string{"pid": "prodTwo"},
				},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo",
							Message: &model.ProduceMessage{
								Key:     &model.ProduceData{String: util.Ptr("foo")},
								Value:   &model.ProduceData{String: util.Ptr("foo")},
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
								Key:     &model.ProduceData{String: util.Ptr("foo")},
								Value:   &model.ProduceData{String: util.Ptr("bar")},
								Headers: map[string]string{"pid": "prodTwo"},
							},
							Pos: 1,
						},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true, Pos: 0}, {Success: true, Pos: 1}},
		},
		{
			name: "matched single topic, single producer failure",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic: foo
					producer: prodFails
			`,
			inputMsgs: []model.ProduceMessage{{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodFails": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: false}},
		},
		{
			name: "matched single topic, multiple producer failure",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic: foo
					producer:
						- prodOne
						- prodFails
			`,
			inputMsgs: []model.ProduceMessage{
				{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar1")}},
				{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar2")}},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar1")}}, Pos: 0},
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar2")}}, Pos: 1},
					},
				},
				"prodFails": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar1")}}, Pos: 0},
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar2")}}, Pos: 1},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: false, Pos: 0}, {Success: false, Pos: 1}},
		},
		{
			name: "matched two disjoint routes, one failure",
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
						- prodFails
			`,
			inputMsgs: []model.ProduceMessage{{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
				"prodFails": {
					Messages: []model.TopicAndMessage{
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}, Value: &model.ProduceData{String: util.Ptr("bar")}}},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: false}},
		},
		{
			name: "matched templated producer failure",
			inputCfg: `
			routes:
				- match: "message.key.string == 'foo'"
					topic:
						- foo
					producer:
						- ${msg:header.pid}
			`,
			inputMsgs: []model.ProduceMessage{
				{
					Key:     &model.ProduceData{String: util.Ptr("foo")},
					Value:   &model.ProduceData{String: util.Ptr("foo")},
					Headers: map[string]string{"pid": "prodOne"},
				},
				{
					Key:     &model.ProduceData{String: util.Ptr("foo")},
					Value:   &model.ProduceData{String: util.Ptr("bar")},
					Headers: map[string]string{"pid": "prodFails"},
				},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo",
							Message: &model.ProduceMessage{
								Key:     &model.ProduceData{String: util.Ptr("foo")},
								Value:   &model.ProduceData{String: util.Ptr("foo")},
								Headers: map[string]string{"pid": "prodOne"},
							},
						},
					},
				},
				"prodFails": {
					Messages: []model.TopicAndMessage{
						{
							Topic: "foo",
							Message: &model.ProduceMessage{
								Key:     &model.ProduceData{String: util.Ptr("foo")},
								Value:   &model.ProduceData{String: util.Ptr("bar")},
								Headers: map[string]string{"pid": "prodFails"},
							},
							Pos: 1,
						},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true, Pos: 0}, {Success: false, Pos: 1}},
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
			inputMsgs: []model.ProduceMessage{
				{Key: &model.ProduceData{String: util.Ptr("foo1")}, Value: &model.ProduceData{String: util.Ptr("bar")}},
				{Key: &model.ProduceData{String: util.Ptr("foo2")}, Value: &model.ProduceData{String: util.Ptr("bar")}},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo1")}, Value: &model.ProduceData{String: util.Ptr("bar")}}, Pos: 0},
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo2")}, Value: &model.ProduceData{String: util.Ptr("bar")}}, Pos: 1},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true, Pos: 0}, {Success: true, Pos: 1}},
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
			inputMsgs: []model.ProduceMessage{
				{Key: &model.ProduceData{String: util.Ptr("foo1")}, Value: &model.ProduceData{String: util.Ptr("bar")}},
				{Key: &model.ProduceData{String: util.Ptr("foo2")}, Value: &model.ProduceData{String: util.Ptr("bar")}},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo1")}, Value: &model.ProduceData{String: util.Ptr("bar")}}, Pos: 0},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo2")}, Value: &model.ProduceData{String: util.Ptr("bar")}}, Pos: 1},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true, Pos: 0}, {Success: true, Pos: 1}},
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
			inputMsgs: []model.ProduceMessage{
				{Key: &model.ProduceData{String: util.Ptr("foo1")}, Value: &model.ProduceData{String: util.Ptr("bar")}},
				{Key: &model.ProduceData{String: util.Ptr("foo2")}, Value: &model.ProduceData{String: util.Ptr("bar")}},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo1")}, Value: &model.ProduceData{String: util.Ptr("bar")}}, Pos: 0},
					},
				},
				"prodTwo": {
					Messages: []model.TopicAndMessage{
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo1")}, Value: &model.ProduceData{String: util.Ptr("bar")}}, Pos: 0},
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo2")}, Value: &model.ProduceData{String: util.Ptr("bar")}}, Pos: 1},
					},
				},
			},
			wantResults: []model.ProduceResult{{Success: true, Pos: 0}, {Success: true, Pos: 1}},
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
			inputMsgs: []model.ProduceMessage{
				{Key: &model.ProduceData{String: util.Ptr("foo1")}, Value: &model.ProduceData{String: util.Ptr("bar")}},
				{Key: &model.ProduceData{String: util.Ptr("foo2")}, Value: &model.ProduceData{String: util.Ptr("bar")}},
				{Key: &model.ProduceData{String: util.Ptr("foo3")}, Value: &model.ProduceData{String: util.Ptr("bar")}},
			},
			wantBatches: map[string]model.MessageBatch{
				"prodOne": {
					Messages: []model.TopicAndMessage{
						{Topic: "foo", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo1")}, Value: &model.ProduceData{String: util.Ptr("bar")}}, Pos: 0},
						{Topic: "bar", Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo2")}, Value: &model.ProduceData{String: util.Ptr("bar")}}, Pos: 1},
					},
				},
			},
			wantResults: []model.ProduceResult{
				{Success: true, Pos: 0},
				{Success: true, Pos: 1},
				{Success: true, Pos: 2},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name+" sync", func(t *testing.T) {
			cfg := loadEndpointConfig(t, tc.inputCfg)
			ps := createProducers(tc.wantBatches)
			router, err := New(cfg, ps)
			require.NoError(t, err)
			results, err := router.SendSync(context.Background(), &tc.inputHttpReq, tc.inputMsgs)
			require.NoError(t, err)
			require.Equal(t, tc.wantResults, results)
			for pid, wantBatch := range tc.wantBatches {
				actual := ps.GetProducer(config.ProducerId(pid)).(*producer.TestProducer).Batch
				require.ElementsMatch(t, wantBatch.Messages, actual.Messages)
			}
		})
		t.Run(tc.name+" async", func(t *testing.T) {
			cfg := loadEndpointConfig(t, tc.inputCfg)
			cfg.Async = true
			ps := createProducers(tc.wantBatches)
			router, err := New(cfg, ps)
			require.NoError(t, err)
			err = router.SendAsync(context.Background(), &tc.inputHttpReq, tc.inputMsgs)
			require.NoError(t, err)
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

func createProducers(wantBatches map[string]model.MessageBatch) producer.Service {
	pMap := make(map[config.ProducerId]producer.Producer)
	pMap[config.ProducerId("prodOne")] = &producer.TestProducer{}
	pMap[config.ProducerId("prodTwo")] = &producer.TestProducer{}
	batch, ok := wantBatches["prodFails"]
	if ok {
		res := make([]model.ProduceResult, 0, len(batch.Messages))
		for i := range batch.Messages {
			res = append(res, model.ProduceResult{Success: false, Pos: batch.Messages[i].Pos})
		}
		pMap[config.ProducerId("prodFails")] = &producer.TestProducer{Result: res}
	}
	ps, _ := producer.NewService(pMap)
	return ps
}
