package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/echo8/krp/internal/config"
	"github.com/echo8/krp/internal/metric"
	pmodel "github.com/echo8/krp/internal/model"
	"github.com/echo8/krp/internal/producer"
	"github.com/echo8/krp/internal/util"
	"github.com/echo8/krp/model"

	"github.com/stretchr/testify/require"
)

func TestProduceSync(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name  string
		input string
		want  pmodel.MessageBatch
	}{
		{
			name: "all",
			input: `
			{
				"messages": [
					{
						"key": {"string": "foo1"}, 
						"value": {"string": "bar1"}, 
						"headers": {"foo2": "bar2"}, 
						"timestamp": "2020-12-09T16:09:53+00:00"
					}
				]
			}`,
			want: pmodel.MessageBatch{
				Messages: []pmodel.TopicAndMessage{
					{
						Topic: testTopic,
						Message: &model.ProduceMessage{
							Key:   &model.ProduceData{String: util.Ptr("foo1")},
							Value: &model.ProduceData{String: util.Ptr("bar1")},
							Headers: map[string]string{
								"foo2": "bar2",
							},
							Timestamp: &ts,
						},
					},
				},
				Src: &config.Endpoint{Path: config.EndpointPath("testId")},
			},
		},
		{
			name:  "value only",
			input: `{"messages": [{"value": {"string": "bar1"}}]}`,
			want: pmodel.MessageBatch{
				Messages: []pmodel.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar1")}}}},
				Src:      &config.Endpoint{Path: config.EndpointPath("testId")},
			},
		},
		{
			name:  "blank key",
			input: `{"messages": [{"key": {"string": ""}, "value": {"string": "bar1"}}]}`,
			want: pmodel.MessageBatch{
				Messages: []pmodel.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("")}, Value: &model.ProduceData{String: util.Ptr("bar1")}}}},
				Src:      &config.Endpoint{Path: config.EndpointPath("testId")},
			},
		},
		{
			name:  "blank value",
			input: `{"messages": [{"value": {"string": ""}}]}`,
			want: pmodel.MessageBatch{
				Messages: []pmodel.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("")}}}},
				Src:      &config.Endpoint{Path: config.EndpointPath("testId")},
			},
		},
		{
			name:  "null key",
			input: `{"messages": [{"key": null, "value": {"string": "bar1"}}]}`,
			want: pmodel.MessageBatch{
				Messages: []pmodel.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar1")}}}},
				Src:      &config.Endpoint{Path: config.EndpointPath("testId")},
			},
		},
		{
			name:  "null headers",
			input: `{"messages": [{"value": {"string": "bar1"}, "headers": null}]}`,
			want: pmodel.MessageBatch{
				Messages: []pmodel.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar1")}}}},
				Src:      &config.Endpoint{Path: config.EndpointPath("testId")},
			},
		},
		{
			name:  "null timestamp",
			input: `{"messages": [{"value": {"string": "bar1"}, "timestamp": null}]}`,
			want: pmodel.MessageBatch{
				Messages: []pmodel.TopicAndMessage{{Topic: testTopic, Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar1")}}}},
				Src:      &config.Endpoint{Path: config.EndpointPath("testId")},
			},
		},
		{
			name:  "multiple messages",
			input: `{"messages": [{"value": {"string": "bar1"}}, {"value": {"string": "bar2"}}, {"value": {"string": "bar3"}}]}`,
			want: pmodel.MessageBatch{
				Messages: []pmodel.TopicAndMessage{
					{Topic: testTopic, Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
					{Topic: testTopic, Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar2")}}},
					{Topic: testTopic, Message: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("bar3")}}},
				},
				Src: &config.Endpoint{Path: config.EndpointPath("testId")},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, tp := sendMessages(tc.input)
			require.Equal(t, http.StatusOK, resp.Code)
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
			name:  "success",
			input: []model.ProduceResult{{Success: true}},
			want:  `{"results":[{"success":true}]}`,
		},
		{
			name:  "failure",
			input: []model.ProduceResult{{Success: false}},
			want:  `{"results":[{"success":false}]}`,
		},
		{
			name:  "multiple results",
			input: []model.ProduceResult{{Success: true, Pos: 0}, {Success: true, Pos: 1}},
			want:  `{"results":[{"success":true},{"success":true}]}`,
		},
		{
			name:  "multiple different results",
			input: []model.ProduceResult{{Success: true, Pos: 0}, {Success: false, Pos: 1}},
			want:  `{"results":[{"success":true},{"success":false}]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, _ := sendMessagesWithResult(tc.input)
			require.Equal(t, http.StatusOK, resp.Code)
			require.Equal(t, tc.want, resp.Body.String())
		})
	}
}

func TestProduceWithValidationFailures(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "empty messages",
			input: `{"messages": []}`,
			want:  `{"error":"'messages' field cannot be empty"}`,
		},
		{
			name:  "empty headers",
			input: `{"messages": [{"value": {"string": "bar1"}, "headers": {}}]}`,
			want:  `{"error":"'headers' field cannot be empty"}`,
		},
		{
			name:  "empty timestamp",
			input: `{"messages": [{"value": {"string": "bar1"}, "timestamp": ""}]}`,
			want:  `{"error":"failed to parse 'timestamp' field, parsing time \"\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"\" as \"2006\""}`,
		},
		{
			name:  "no messages",
			input: `{}`,
			want:  `{"error":"'messages' field is required"}`,
		},
		{
			name:  "message with nothing",
			input: `{"messages": [{}]}`,
			want:  `{"error":"'value' field is required"}`,
		},
		{
			name:  "header with null value",
			input: `{"messages": [{"value": {"string": "bar1"}, "headers": {"foo1": null}}]}`,
			want:  `{"error":"'headers[foo1]' must not be null or blank"}`,
		},
		{
			name:  "header with blank value",
			input: `{"messages": [{"value": {"string": "bar1"}, "headers": {"foo1": ""}}]}`,
			want:  `{"error":"'headers[foo1]' must not be null or blank"}`,
		},
		{
			name:  "value is null",
			input: `{"messages": [{"value": null}]}`,
			want:  `{"error":"'value' field is required"}`,
		},
		{
			name:  "key only",
			input: `{"messages": [{"key": {"string": "foo1"}}]}`,
			want:  `{"error":"'value' field is required"}`,
		},
		{
			name:  "null string key",
			input: `{"messages": [{"key": {"string": null}, "value": {"string": "bar1"}}]}`,
			want:  `{"error":"'string' OR 'bytes' field must be specified"}`,
		},
		{
			name:  "headers only",
			input: `{"messages": [{"headers": {"foo2": "bar2"}}]}`,
			want:  `{"error":"'value' field is required"}`,
		},
		{
			name:  "timestamp only",
			input: `{"messages": [{"timestamp": "2020-12-09T16:09:53+00:00"}]}`,
			want:  `{"error":"'value' field is required"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, _ := sendMessages(tc.input)
			require.Equal(t, http.StatusBadRequest, resp.Code)
			require.Equal(t, tc.want, resp.Body.String())
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
			name:     "producer error",
			err:      fmt.Errorf("test-error"),
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "request canceled",
			err:      context.Canceled,
			wantCode: 499,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, _ := sendMessagesWith(`{"messages": [{"value": {"string": "bar1"}}]}`, "testId1", "testId1", nil, tc.err, false)
			require.Equal(t, tc.wantCode, resp.Result().StatusCode)
		})
	}
}

func TestProduceWithInvalidProducerId(t *testing.T) {
	resp, _ := sendMessagesWith(`{"messages": [{"value": {"string": "bar1"}}]}`, "testId1", "testId2", nil, nil, false)
	require.Equal(t, http.StatusNotFound, resp.Code)
}

func TestProduceAsync(t *testing.T) {
	resp, _ := sendMessagesWith(`{"messages": [{"value": {"string": "bar1"}}]}`, "testId1", "testId1", nil, nil, true)
	require.Equal(t, http.StatusNoContent, resp.Code)
}

func sendMessages(json string) (*httptest.ResponseRecorder, *producer.TestProducer) {
	return sendMessagesWith(json, "testId", "testId", nil, nil, false)
}

func sendMessagesWithResult(result []model.ProduceResult) (*httptest.ResponseRecorder, *producer.TestProducer) {
	return sendMessagesWith(`{"messages": [{"value": {"string": "bar1"}}]}`, "testId", "testId", result, nil, false)
}

const testTopic string = "test-topic"

func sendMessagesWith(json, eid, sendEid string, result []model.ProduceResult, err error, async bool) (*httptest.ResponseRecorder, *producer.TestProducer) {
	cfg := &config.ServerConfig{
		Endpoints: config.EndpointConfigs{
			config.EndpointPath(eid): {
				Endpoint: &config.Endpoint{Path: config.EndpointPath(eid)},
				Async:    async,
				Routes: []*config.RouteConfig{
					{Topic: config.Topic(testTopic), Producer: config.ProducerId("testPid")},
				},
			},
		},
	}
	tp := &producer.TestProducer{Result: result, Error: err}
	ps, _ := producer.NewServiceFrom(config.ProducerId("testPid"), tp)
	ms, _ := metric.NewService(&config.MetricsConfig{})
	s, _ := NewServer(cfg, ps, ms)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/"+sendEid, strings.NewReader(json))
	s.ServeHTTP(w, req)
	return w, tp
}
