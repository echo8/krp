package franz

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/echo8/krp/internal/config"
	franzcfg "github.com/echo8/krp/internal/config/franz"
	"github.com/echo8/krp/internal/metric"
	pmodel "github.com/echo8/krp/internal/model"
	"github.com/echo8/krp/internal/producer"
	"github.com/echo8/krp/internal/serializer"
	"github.com/echo8/krp/internal/util"
	"github.com/echo8/krp/model"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type mockFranzClient struct {
	errMap map[string]error
	resMap map[string]*kgo.Record
}

func (m *mockFranzClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	v := string(r.Value)
	err, ok := m.errMap[v]
	if !ok {
		err = nil
		m.resMap[v] = r
	}
	promise(r, err)
}

func (m *mockFranzClient) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	fmt.Println("len of records", len(rs))
	results := make([]kgo.ProduceResult, 0, len(rs))
	for i := range rs {
		v := string(rs[i].Value)
		err, ok := m.errMap[v]
		if !ok {
			err = nil
			m.resMap[v] = rs[i]
		}
		results = append(results, kgo.ProduceResult{Record: rs[i], Err: err})
	}
	return results
}

func (m *mockFranzClient) Close() {
}

func newMockFranzClient(errMap map[string]error) *mockFranzClient {
	if errMap == nil {
		errMap = make(map[string]error)
	}
	return &mockFranzClient{errMap: errMap, resMap: make(map[string]*kgo.Record)}
}

func TestFranzProduce(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name         string
		input        []model.ProduceMessage
		wantMessages []*kgo.Record
		wantResults  []model.ProduceResult
	}{
		{
			name: "all",
			input: []model.ProduceMessage{
				{
					Key:       &model.ProduceData{String: util.Ptr("foo1")},
					Value:     &model.ProduceData{String: util.Ptr("bar1")},
					Headers:   map[string]string{"foo2": "bar2"},
					Timestamp: &ts,
				},
			},
			wantMessages: []*kgo.Record{
				{
					Key:       []byte("foo1"),
					Value:     []byte("bar1"),
					Headers:   []kgo.RecordHeader{{Key: "foo2", Value: []byte("bar2")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
		},
		{
			name: "multiple messages",
			input: []model.ProduceMessage{
				{
					Key:       &model.ProduceData{String: util.Ptr("foo1")},
					Value:     &model.ProduceData{String: util.Ptr("bar1")},
					Headers:   map[string]string{"foo2": "bar2"},
					Timestamp: &ts,
				},
				{
					Key:       &model.ProduceData{String: util.Ptr("foo3")},
					Value:     &model.ProduceData{String: util.Ptr("bar3")},
					Headers:   map[string]string{"foo4": "bar4"},
					Timestamp: &ts,
				},
			},
			wantMessages: []*kgo.Record{
				{
					Key:       []byte("foo1"),
					Value:     []byte("bar1"),
					Headers:   []kgo.RecordHeader{{Key: "foo2", Value: []byte("bar2")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
				{
					Key:       []byte("foo3"),
					Value:     []byte("bar3"),
					Headers:   []kgo.RecordHeader{{Key: "foo4", Value: []byte("bar4")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
			},
			wantResults: []model.ProduceResult{
				{Success: true, Pos: 0},
				{Success: true, Pos: 1},
			},
		},
		{
			name: "multiple headers",
			input: []model.ProduceMessage{
				{
					Value:   &model.ProduceData{String: util.Ptr("bar1")},
					Headers: map[string]string{"foo2": "bar2", "foo3": "bar3"},
				},
			},
			wantMessages: []*kgo.Record{
				{
					Value: []byte("bar1"),
					Headers: []kgo.RecordHeader{
						{Key: "foo2", Value: []byte("bar2")},
						{Key: "foo3", Value: []byte("bar3")},
					},
					Topic: testTopic,
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
		},
		{
			name: "value only",
			input: []model.ProduceMessage{
				{
					Value: &model.ProduceData{String: util.Ptr("bar1")},
				},
			},
			wantMessages: []*kgo.Record{
				{
					Value: []byte("bar1"),
					Topic: testTopic,
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
		},
		{
			name: "blank value",
			input: []model.ProduceMessage{
				{
					Value: &model.ProduceData{String: util.Ptr("")},
				},
			},
			wantMessages: []*kgo.Record{
				{
					Value: []byte(""),
					Topic: testTopic,
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
		},
		{
			name: "blank headers",
			input: []model.ProduceMessage{
				{
					Value:   &model.ProduceData{String: util.Ptr("bar1")},
					Headers: map[string]string{"": ""},
				},
			},
			wantMessages: []*kgo.Record{
				{
					Value:   []byte("bar1"),
					Headers: []kgo.RecordHeader{{Key: "", Value: []byte("")}},
					Topic:   testTopic,
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, producer, res, err := sendMessagesWith(tc.input, nil, false)
			require.NoError(t, err)
			defer producer.Close()
			for _, m := range tc.wantMessages {
				r := client.resMap[string(m.Value)]
				require.ElementsMatch(t, m.Headers, r.Headers)
				m.Headers = []kgo.RecordHeader{}
				r.Headers = []kgo.RecordHeader{}
				m.Context = nil
				r.Context = nil
				require.Equal(t, m, r)
			}
			require.Equal(t, tc.wantResults, res)
		})
	}
}

func TestFranzProduceAsync(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name         string
		input        []model.ProduceMessage
		wantMessages []*kgo.Record
	}{
		{
			name: "all",
			input: []model.ProduceMessage{
				{
					Key:       &model.ProduceData{String: util.Ptr("foo1")},
					Value:     &model.ProduceData{String: util.Ptr("bar1")},
					Headers:   map[string]string{"foo2": "bar2"},
					Timestamp: &ts,
				},
			},
			wantMessages: []*kgo.Record{
				{
					Key:       []byte("foo1"),
					Value:     []byte("bar1"),
					Headers:   []kgo.RecordHeader{{Key: "foo2", Value: []byte("bar2")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
			},
		},
		{
			name: "multiple messages",
			input: []model.ProduceMessage{
				{
					Key:       &model.ProduceData{String: util.Ptr("foo1")},
					Value:     &model.ProduceData{String: util.Ptr("bar1")},
					Headers:   map[string]string{"foo2": "bar2"},
					Timestamp: &ts,
				},
				{
					Key:       &model.ProduceData{String: util.Ptr("foo3")},
					Value:     &model.ProduceData{String: util.Ptr("bar3")},
					Headers:   map[string]string{"foo4": "bar4"},
					Timestamp: &ts,
				},
			},
			wantMessages: []*kgo.Record{
				{
					Key:       []byte("foo1"),
					Value:     []byte("bar1"),
					Headers:   []kgo.RecordHeader{{Key: "foo2", Value: []byte("bar2")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
				{
					Key:       []byte("foo3"),
					Value:     []byte("bar3"),
					Headers:   []kgo.RecordHeader{{Key: "foo4", Value: []byte("bar4")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, producer, _, err := sendMessagesWith(tc.input, nil, true)
			require.NoError(t, err)
			defer producer.Close()
			for _, m := range tc.wantMessages {
				r := client.resMap[string(m.Value)]
				require.ElementsMatch(t, m.Headers, r.Headers)
				m.Headers = []kgo.RecordHeader{}
				r.Headers = []kgo.RecordHeader{}
				m.Context = nil
				r.Context = nil
				require.Equal(t, m, r)
			}
		})
	}
}

func TestFranzProduceWithErrors(t *testing.T) {
	tests := []struct {
		name        string
		async       bool
		input       []model.ProduceMessage
		inputErrors map[string]error
		wantResults []model.ProduceResult
	}{
		{
			name:  "error event",
			input: []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
			inputErrors: map[string]error{
				"bar1": fmt.Errorf("test-error"),
			},
			wantResults: []model.ProduceResult{{Success: false}},
		},
		{
			name: "multiple error events",
			input: []model.ProduceMessage{
				{Value: &model.ProduceData{String: util.Ptr("bar1")}},
				{Value: &model.ProduceData{String: util.Ptr("bar2")}},
			},
			inputErrors: map[string]error{
				"bar1": fmt.Errorf("test-error1"),
				"bar2": fmt.Errorf("test-error2"),
			},
			wantResults: []model.ProduceResult{
				{Success: false, Pos: 0},
				{Success: false, Pos: 1},
			},
		},
		{
			name: "both error and success events",
			input: []model.ProduceMessage{
				{Value: &model.ProduceData{String: util.Ptr("bar1")}},
				{Value: &model.ProduceData{String: util.Ptr("bar2")}},
			},
			inputErrors: map[string]error{
				"bar1": fmt.Errorf("test-error"),
			},
			wantResults: []model.ProduceResult{
				{Success: false, Pos: 0},
				{Success: true, Pos: 1},
			},
		},
		{
			name:  "error event async",
			async: true,
			input: []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
			inputErrors: map[string]error{
				"bar1": fmt.Errorf("test-error"),
			},
			wantResults: nil,
		},
		{
			name:  "multiple error events async",
			async: true,
			input: []model.ProduceMessage{
				{Value: &model.ProduceData{String: util.Ptr("bar1")}},
				{Value: &model.ProduceData{String: util.Ptr("bar2")}},
			},
			inputErrors: map[string]error{
				"bar1": fmt.Errorf("test-error1"),
				"bar2": fmt.Errorf("test-error2"),
			},
			wantResults: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, producer, res, err := sendMessagesWith(tc.input, tc.inputErrors, tc.async)
			require.NoError(t, err)
			defer producer.Close()
			require.Equal(t, tc.wantResults, res)
		})
	}
}

const testTopic string = "test-topic"

func sendMessagesWith(msgs []model.ProduceMessage, errMap map[string]error, async bool) (*mockFranzClient, producer.Producer, []model.ProduceResult, error) {
	client := newMockFranzClient(errMap)
	cfg := &franzcfg.ProducerConfig{}
	metrics, _ := metric.NewService(&config.MetricsConfig{})
	keySerializer, _ := serializer.NewSerializer(cfg.SchemaRegistry, nil, true)
	valueSerializer, _ := serializer.NewSerializer(cfg.SchemaRegistry, nil, false)
	producer := newProducer(cfg, client, metrics, keySerializer, valueSerializer)
	if !async {
		res, err := producer.SendSync(context.Background(), messageBatch(testTopic, msgs))
		return client, producer, res, err
	} else {
		err := producer.SendAsync(context.Background(), messageBatch(testTopic, msgs))
		return client, producer, nil, err
	}
}

func messageBatch(topic string, messages []model.ProduceMessage) *pmodel.MessageBatch {
	res := make([]pmodel.TopicAndMessage, len(messages))
	for i, msg := range messages {
		res[i] = pmodel.TopicAndMessage{Topic: topic, Message: &msg, Pos: i}
	}
	return &pmodel.MessageBatch{Messages: res, Src: &config.Endpoint{}}
}
