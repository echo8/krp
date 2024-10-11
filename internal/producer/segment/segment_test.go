package segment

import (
	"context"
	"echo8/kafka-rest-producer/internal/config"
	segmentcfg "echo8/kafka-rest-producer/internal/config/segment"
	"echo8/kafka-rest-producer/internal/metric"
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/producer"
	"echo8/kafka-rest-producer/internal/serializer"
	"echo8/kafka-rest-producer/internal/util"
	"fmt"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/stretchr/testify/require"
)

type mockSegmentWriter struct {
	messages    []kafka.Message
	returnError error
	callback    func(messages []kafka.Message, err error)
}

func newMockSegmentWriter(returnError error) *mockSegmentWriter {
	return &mockSegmentWriter{returnError: returnError}
}

func (w *mockSegmentWriter) sendCallback(cb func(messages []kafka.Message, err error)) {
	w.callback = cb
}

func (w *mockSegmentWriter) writeMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.messages = make([]kafka.Message, len(msgs))
	cbMsgs := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		sm := m
		sm.WriterData = nil
		w.messages[i] = sm
		if w.returnError == nil {
			m.Partition = 7 + i
			m.Offset = 77 + int64(i)
		}
		cbMsgs[i] = m
	}
	w.callback(cbMsgs, w.returnError)
	return nil
}

func (w *mockSegmentWriter) stats() kafka.WriterStats {
	return kafka.WriterStats{}
}

func (w *mockSegmentWriter) close() error {
	return nil
}

func TestSegmentProduce(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name         string
		input        []model.ProduceMessage
		wantMessages []kafka.Message
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
			wantMessages: []kafka.Message{
				{
					Key:     []byte("foo1"),
					Value:   []byte("bar1"),
					Headers: []kafka.Header{{Key: "foo2", Value: []byte("bar2")}},
					Time:    ts,
					Topic:   testTopic,
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
			wantMessages: []kafka.Message{
				{
					Key:     []byte("foo1"),
					Value:   []byte("bar1"),
					Headers: []kafka.Header{{Key: "foo2", Value: []byte("bar2")}},
					Time:    ts,
					Topic:   testTopic,
				},
				{
					Key:     []byte("foo3"),
					Value:   []byte("bar3"),
					Headers: []kafka.Header{{Key: "foo4", Value: []byte("bar4")}},
					Time:    ts,
					Topic:   testTopic,
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
			wantMessages: []kafka.Message{
				{
					Value: []byte("bar1"),
					Headers: []kafka.Header{
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
			wantMessages: []kafka.Message{
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
			wantMessages: []kafka.Message{
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
			wantMessages: []kafka.Message{
				{
					Value:   []byte("bar1"),
					Headers: []kafka.Header{{Key: "", Value: []byte("")}},
					Topic:   testTopic,
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			writer, kp, res := sendSegmentMessages(tc.input)
			defer kp.Close()
			for i := range tc.wantMessages {
				m := tc.wantMessages[i]
				r := writer.messages[i]
				require.ElementsMatch(t, m.Headers, r.Headers)
				m.Headers = []protocol.Header{}
				r.Headers = []protocol.Header{}
				require.Equal(t, m, r)
			}
			require.Equal(t, tc.wantResults, res)
		})
	}
}

func TestSegmentProduceAsync(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name         string
		input        []model.ProduceMessage
		wantMessages []kafka.Message
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
			wantMessages: []kafka.Message{
				{
					Key:     []byte("foo1"),
					Value:   []byte("bar1"),
					Headers: []kafka.Header{{Key: "foo2", Value: []byte("bar2")}},
					Time:    ts,
					Topic:   testTopic,
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
			wantMessages: []kafka.Message{
				{
					Key:     []byte("foo1"),
					Value:   []byte("bar1"),
					Headers: []kafka.Header{{Key: "foo2", Value: []byte("bar2")}},
					Time:    ts,
					Topic:   testTopic,
				},
				{
					Key:     []byte("foo3"),
					Value:   []byte("bar3"),
					Headers: []kafka.Header{{Key: "foo4", Value: []byte("bar4")}},
					Time:    ts,
					Topic:   testTopic,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			writer, kp, res := sendSegmentMessagesWith(tc.input, nil, true)
			defer kp.Close()
			for i := range tc.wantMessages {
				m := tc.wantMessages[i]
				r := writer.messages[i]
				require.ElementsMatch(t, m.Headers, r.Headers)
				m.Headers = []protocol.Header{}
				r.Headers = []protocol.Header{}
				require.Equal(t, m, r)
			}
			require.Nil(t, res)
		})
	}
}

func TestSegmentProduceWithErrors(t *testing.T) {
	tests := []struct {
		name        string
		async       bool
		input       []model.ProduceMessage
		inputError  error
		wantResults []model.ProduceResult
	}{
		{
			name:        "error event",
			input:       []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
			inputError:  fmt.Errorf("test-error"),
			wantResults: []model.ProduceResult{{Success: false}},
		},
		{
			name: "multiple error events",
			input: []model.ProduceMessage{
				{Value: &model.ProduceData{String: util.Ptr("bar1")}},
				{Value: &model.ProduceData{String: util.Ptr("bar2")}},
			},
			inputError: fmt.Errorf("test-error"),
			wantResults: []model.ProduceResult{
				{Success: false, Pos: 0},
				{Success: false, Pos: 1},
			},
		},
		{
			name:        "error event async",
			async:       true,
			input:       []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
			inputError:  fmt.Errorf("test-error"),
			wantResults: nil,
		},
		{
			name:  "multiple error events async",
			async: true,
			input: []model.ProduceMessage{
				{Value: &model.ProduceData{String: util.Ptr("bar1")}},
				{Value: &model.ProduceData{String: util.Ptr("bar2")}},
			},
			inputError:  fmt.Errorf("test-error"),
			wantResults: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, kp, res := sendSegmentMessagesWith(tc.input, tc.inputError, tc.async)
			defer kp.Close()
			require.Equal(t, tc.wantResults, res)
		})
	}
}

func sendSegmentMessages(msgs []model.ProduceMessage) (*mockSegmentWriter, producer.Producer, []model.ProduceResult) {
	return sendSegmentMessagesWith(msgs, nil, false)
}

func sendSegmentMessagesWith(
	msgs []model.ProduceMessage, returnError error, async bool,
) (*mockSegmentWriter, producer.Producer, []model.ProduceResult) {
	writerAsync := newMockSegmentWriter(returnError)
	writerSync := newMockSegmentWriter(returnError)
	cfg := &segmentcfg.ProducerConfig{}
	ms, _ := metric.NewService(&config.MetricsConfig{})
	keySerializer, _ := serializer.NewSerializer(cfg.SchemaRegistry, true)
	valueSerializer, _ := serializer.NewSerializer(cfg.SchemaRegistry, false)
	kp, _ := newProducer(cfg, writerAsync, writerSync, ms, keySerializer, valueSerializer)
	if !async {
		res, _ := kp.SendSync(context.Background(), messageBatch(testTopic, msgs))
		return writerSync, kp, res
	} else {
		kp.SendAsync(context.Background(), messageBatch(testTopic, msgs))
		return writerAsync, kp, nil
	}
}

const testTopic string = "test-topic"

func messageBatch(topic string, messages []model.ProduceMessage) *model.MessageBatch {
	res := make([]model.TopicAndMessage, len(messages))
	for i, msg := range messages {
		res[i] = model.TopicAndMessage{Topic: topic, Message: &msg, Pos: i}
	}
	return &model.MessageBatch{Messages: res, Src: &config.Endpoint{}}
}
