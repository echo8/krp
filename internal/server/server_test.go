package server

import (
	"context"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/metric"
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
	Batch   model.MessageBatch
	Result  []model.ProduceResult
	Error   error
	IsAsync bool
}

func (k *TestProducer) Async() bool {
	return k.IsAsync
}

func (k *TestProducer) SendAsync(ctx context.Context, batch *model.MessageBatch) error {
	k.Batch = *batch
	if k.Error != nil {
		return k.Error
	} else {
		return nil
	}
}

func (k *TestProducer) SendSync(ctx context.Context, batch *model.MessageBatch) ([]model.ProduceResult, error) {
	k.Batch = *batch
	if k.Result != nil {
		return k.Result, nil
	} else if k.Error != nil {
		return nil, k.Error
	} else {
		return []model.ProduceResult{}, nil
	}
}

func (k *TestProducer) Close() error {
	return nil
}

func TestProduceSync(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name  string
		input string
		want  model.MessageBatch
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
			want: model.MessageBatch{
				Messages: []model.TopicAndMessage{
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
				Src: &config.Endpoint{Id: "testId"},
			},
		},
		{
			name:  "value only",
			input: `{"messages": [{"value": "bar1"}]}`,
			want: model.MessageBatch{
				Messages: []model.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar1")}}},
				Src:      &config.Endpoint{Id: "testId"},
			},
		},
		{
			name:  "blank key",
			input: `{"messages": [{"key": "", "value": "bar1"}]}`,
			want: model.MessageBatch{
				Messages: []model.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Key: util.Ptr(""), Value: util.Ptr("bar1")}}},
				Src:      &config.Endpoint{Id: "testId"},
			},
		},
		{
			name:  "blank value",
			input: `{"messages": [{"value": ""}]}`,
			want: model.MessageBatch{
				Messages: []model.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("")}}},
				Src:      &config.Endpoint{Id: "testId"},
			},
		},
		{
			name:  "blank headers",
			input: `{"messages": [{"value": "bar1", "headers": [{"key": "", "value": ""}]}]}`,
			want: model.MessageBatch{
				Messages: []model.TopicAndMessage{{Topic: testTopic,
					Message: &model.ProduceMessage{Value: util.Ptr("bar1"), Headers: []model.ProduceHeader{{Key: util.Ptr(""), Value: util.Ptr("")}}}}},
				Src: &config.Endpoint{Id: "testId"},
			},
		},
		{
			name:  "null key",
			input: `{"messages": [{"key": null, "value": "bar1"}]}`,
			want: model.MessageBatch{
				Messages: []model.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar1")}}},
				Src:      &config.Endpoint{Id: "testId"},
			},
		},
		{
			name:  "null headers",
			input: `{"messages": [{"value": "bar1", "headers": null}]}`,
			want: model.MessageBatch{
				Messages: []model.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar1")}}},
				Src:      &config.Endpoint{Id: "testId"},
			},
		},
		{
			name:  "null timestamp",
			input: `{"messages": [{"value": "bar1", "timestamp": null}]}`,
			want: model.MessageBatch{
				Messages: []model.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar1")}}},
				Src:      &config.Endpoint{Id: "testId"},
			},
		},
		{
			name:  "multiple messages",
			input: `{"messages": [{"value": "bar1"}, {"value": "bar2"}, {"value": "bar3"}]}`,
			want: model.MessageBatch{
				Messages: []model.TopicAndMessage{
					{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar1")}},
					{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar2")}},
					{Topic: testTopic, Message: &model.ProduceMessage{Value: util.Ptr("bar3")}},
				},
				Src: &config.Endpoint{Id: "testId"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, tp := sendMessages(tc.input)
			require.Equal(t, 200, resp.Code)
			require.Equal(t, tc.want, tp.Batch)
		})
	}
}

func TestProduceSyncResults(t *testing.T) {
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

func TestProduceWithProducerError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode int
	}{
		{
			name: "producer error",
			err: fmt.Errorf("test-error"),
			wantCode: http.StatusInternalServerError,
		},
		{
			name: "request canceled",
			err: context.Canceled,
			wantCode: 499,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, _ := sendMessagesWith(`{"messages": [{"value": "bar1"}]}`, "testId1", "testId1", nil, tc.err, false)
			require.Equal(t, tc.wantCode, resp.Result().StatusCode)
		})
	}
}

func TestProduceWithInvalidProducerId(t *testing.T) {
	resp, _ := sendMessagesWith(`{"messages": [{"value": "bar1"}]}`, "testId1", "testId2", nil, nil, false)
	require.Equal(t, 404, resp.Code)
}

func TestProduceAsync(t *testing.T) {
	resp, _ := sendMessagesWith(`{"messages": [{"value": "bar1"}]}`, "testId1", "testId1", nil, nil, true)
	require.Equal(t, 204, resp.Code)
}

func sendMessages(json string) (*httptest.ResponseRecorder, *TestProducer) {
	return sendMessagesWith(json, "testId", "testId", nil, nil, false)
}

func sendMessagesWithResult(result []model.ProduceResult) (*httptest.ResponseRecorder, *TestProducer) {
	return sendMessagesWith(`{"messages": [{"value": "bar1"}]}`, "testId", "testId", result, nil, false)
}

const testTopic string = "test-topic"

func sendMessagesWith(json, eid, sendEid string, result []model.ProduceResult, err error, async bool) (*httptest.ResponseRecorder, *TestProducer) {
	cfg := &config.ServerConfig{Endpoints: config.NamespacedEndpointConfigs{
		config.DefaultNamespace: map[config.EndpointId]config.EndpointConfig{
			config.EndpointId(eid): {Endpoint: &config.Endpoint{Id: eid}, Topic: testTopic, Producer: "testPid"},
		},
	}}
	tp := &TestProducer{Result: result, Error: err, IsAsync: async}
	ps, _ := producer.NewServiceFrom(config.ProducerId("testPid"), tp)
	ms, _ := metric.NewService(&config.MetricsConfig{})
	s := NewServer(cfg, ps, ms)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/"+sendEid, strings.NewReader(json))
	s.engine.ServeHTTP(w, req)
	return w, tp
}
