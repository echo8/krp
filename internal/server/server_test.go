package server

import (
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/model"
	"koko/kafka-rest-producer/internal/producer"
	"koko/kafka-rest-producer/internal/util"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestProducer struct {
	Messages []producer.TopicAndMessage
	Result   []model.ProduceResult
}

func (k *TestProducer) Send(messages []producer.TopicAndMessage) []model.ProduceResult {
	k.Messages = messages
	if k.Result != nil {
		return k.Result
	} else {
		return []model.ProduceResult{}
	}
}

func (k *TestProducer) Close() error {
	return nil
}

func TestProduce(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name  string
		input string
		want  []producer.TopicAndMessage
	}{
		{
			name: "all",
			input: `
			{
				"messages": [
					{
						"key": "foo1", 
						"value": "bar1", 
						"headers": [{"key": "foo2", "value": "bar2"}], 
						"timestamp": "2020-12-09T16:09:53+00:00"
					}
				]
			}`,
			want: []producer.TopicAndMessage{
				{
					Topic: testTopic,
					Message: &model.ProduceMessage{
						Key:   util.Ptr("foo1"),
						Value: util.Ptr("bar1"),
						Headers: []model.ProduceHeader{
							{
								Key:   util.Ptr("foo2"),
								Value: util.Ptr("bar2"),
							},
						},
						Timestamp: &ts,
					},
				},
			},
		},
		{
			name:  "value only",
			input: `{"messages": [{"value": "bar1"}]}`,
			want:  []producer.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar1")}}},
		},
		{
			name:  "blank key",
			input: `{"messages": [{"key": "", "value": "bar1"}]}`,
			want: []producer.TopicAndMessage{{Topic: testTopic,
				Message: &model.ProduceMessage{Key: util.Ptr(""), Value: util.Ptr("bar1")}}},
		},
		{
			name:  "blank value",
			input: `{"messages": [{"value": ""}]}`,
			want:  []producer.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("")}}},
		},
		{
			name:  "blank headers",
			input: `{"messages": [{"value": "bar1", "headers": [{"key": "", "value": ""}]}]}`,
			want: []producer.TopicAndMessage{{Topic: testTopic,
				Message: &model.ProduceMessage{Value: util.Ptr("bar1"), Headers: []model.ProduceHeader{{Key: util.Ptr(""), Value: util.Ptr("")}}}}},
		},
		{
			name:  "null key",
			input: `{"messages": [{"key": null, "value": "bar1"}]}`,
			want:  []producer.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar1")}}},
		},
		{
			name:  "null headers",
			input: `{"messages": [{"value": "bar1", "headers": null}]}`,
			want:  []producer.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar1")}}},
		},
		{
			name:  "null timestamp",
			input: `{"messages": [{"value": "bar1", "timestamp": null}]}`,
			want:  []producer.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar1")}}},
		},
		{
			name:  "multiple messages",
			input: `{"messages": [{"value": "bar1"}, {"value": "bar2"}, {"value": "bar3"}]}`,
			want: []producer.TopicAndMessage{
				{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar1")}},
				{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar2")}},
				{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar3")}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, tp := sendMessages(tc.input)
			require.Equal(t, 200, resp.Code)
			require.Equal(t, tc.want, tp.Messages)
		})
	}
}

func TestProduceResults(t *testing.T) {
	tests := []struct {
		name  string
		input []model.ProduceResult
		want  string
	}{
		{
			name:  "partition and offset",
			input: []model.ProduceResult{{Partition: util.Ptr(int32(7)), Offset: util.Ptr(int64(77))}},
			want:  `{"results":[{"partition":7,"offset":77}]}`,
		},
		{
			name:  "partition and offset zeros",
			input: []model.ProduceResult{{Partition: util.Ptr(int32(0)), Offset: util.Ptr(int64(0))}},
			want:  `{"results":[{"partition":0,"offset":0}]}`,
		},
		{
			name: "multiple results",
			input: []model.ProduceResult{{Partition: util.Ptr(int32(7)), Offset: util.Ptr(int64(77))},
				{Partition: util.Ptr(int32(8)), Offset: util.Ptr(int64(88))}},
			want: `{"results":[{"partition":7,"offset":77},{"partition":8,"offset":88}]}`,
		},
		{
			name:  "error",
			input: []model.ProduceResult{{Error: util.Ptr("test error")}},
			want:  `{"results":[{"error":"test error"}]}`,
		},
		{
			name:  "blank error",
			input: []model.ProduceResult{{Error: util.Ptr("")}},
			want:  `{"results":[{"error":""}]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, _ := sendMessagesWithResult(tc.input)
			require.Equal(t, 200, resp.Code)
			require.Equal(t, tc.want, resp.Body.String())
		})
	}
}

func TestProduceWithValidationFailures(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty messages",
			input: `{"messages": []}`,
		},
		{
			name:  "empty headers",
			input: `{"messages": [{"value": "bar1", "headers": []}]}`,
		},
		{
			name:  "empty timestamp",
			input: `{"messages": [{"value": "bar1", "timestamp": ""}]}`,
		},
		{
			name:  "no messages",
			input: `{}`,
		},
		{
			name:  "message with nothing",
			input: `{"messages": [{}]}`,
		},
		{
			name:  "header without key",
			input: `{"messages": [{"value": "bar1", "headers": [{"value": "bar2"}]}]}`,
		},
		{
			name:  "header without value",
			input: `{"messages": [{"value": "bar1", "headers": [{"key": "foo1"}]}]}`,
		},
		{
			name:  "header with null key",
			input: `{"messages": [{"value": "bar1", "headers": [{"key": null, "value": "bar2"}]}]}`,
		},
		{
			name:  "header with null value",
			input: `{"messages": [{"value": "bar1", "headers": [{"key": "foo1", "value": null}]}]}`,
		},
		{
			name:  "value is null",
			input: `{"messages": [{"value": null}]}`,
		},
		{
			name:  "key only",
			input: `{"messages": [{"key": "foo1"}]}`,
		},
		{
			name:  "headers only",
			input: `{"messages": [{"headers": [{"key": "foo2", "value": "bar2"}]}]}`,
		},
		{
			name:  "timestamp only",
			input: `{"messages": [{"timestamp": "2020-12-09T16:09:53+00:00"}]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, _ := sendMessages(tc.input)
			require.Equal(t, 400, resp.Code)
		})
	}
}

func TestProduceWithInvalidProducerId(t *testing.T) {
	resp, _ := sendMessagesWith(`{"messages": [{"value": "bar1"}]}`, "testId1", "testId2", nil)
	require.Equal(t, 404, resp.Code)
}

func sendMessages(json string) (*httptest.ResponseRecorder, *TestProducer) {
	return sendMessagesWith(json, "testId", "testId", nil)
}

func sendMessagesWithResult(result []model.ProduceResult) (*httptest.ResponseRecorder, *TestProducer) {
	return sendMessagesWith(`{"messages": [{"value": "bar1"}]}`, "testId", "testId", result)
}

const testTopic string = "test-topic"

func sendMessagesWith(json, eid, sendEid string, result []model.ProduceResult) (*httptest.ResponseRecorder, *TestProducer) {
	cfg := &config.ServerConfig{Endpoints: config.NamespacedEndpointConfigs{
		config.DefaultNamespace: map[config.EndpointId]config.EndpointConfig{
			config.EndpointId(eid): {Topic: testTopic, Producer: "testPid"},
		},
	}}
	tp := &TestProducer{Result: result}
	ps, _ := producer.NewServiceFrom(config.ProducerId("testPid"), tp)
	s := NewServer(cfg, ps)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/"+sendEid, strings.NewReader(json))
	s.engine.ServeHTTP(w, req)
	return w, tp
}
