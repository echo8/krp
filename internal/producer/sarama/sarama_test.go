package sarama

import (
	"context"
	"echo8/kafka-rest-producer/internal/config"
	saramacfg "echo8/kafka-rest-producer/internal/config/sarama"
	"echo8/kafka-rest-producer/internal/metric"
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/producer"
	"echo8/kafka-rest-producer/internal/util"
	"fmt"
	"sync"
	"testing"
	"time"

	kafka "github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSaramaProducer struct {
	messages  sync.Map
	resMap    map[string]saramaPartAndOffset
	errMap    map[string]error
	inputCh   chan *kafka.ProducerMessage
	errorCh   chan *kafka.ProducerError
	successCh chan *kafka.ProducerMessage
}

type saramaPartAndOffset struct {
	partition int32
	offset    int64
}

func (p *testSaramaProducer) Input() chan<- *kafka.ProducerMessage {
	return p.inputCh
}

func (p *testSaramaProducer) Errors() <-chan *kafka.ProducerError {
	return p.errorCh
}

func (p *testSaramaProducer) Successes() <-chan *kafka.ProducerMessage {
	return p.successCh
}

func (p *testSaramaProducer) Close() error {
	if p.inputCh != nil {
		close(p.inputCh)
	}
	return nil
}

func newTestSaramaAsyncProducer(async bool, errMap map[string]error, input []model.ProduceMessage) *testSaramaProducer {
	p := &testSaramaProducer{errMap: errMap}
	p.inputCh = make(chan *kafka.ProducerMessage, 10)
	p.errorCh = make(chan *kafka.ProducerError, 10)
	if !async {
		p.successCh = make(chan *kafka.ProducerMessage, 10)
		p.resMap = make(map[string]saramaPartAndOffset, len(input))
		startPart := int32(7)
		startOffs := int64(77)
		for i, m := range input {
			p.resMap[*m.Value] = saramaPartAndOffset{partition: startPart + int32(i), offset: startOffs + int64(i)}
		}
	}
	go func() {
		for m := range p.inputCh {
			v, _ := m.Value.Encode()
			sv := string(v)
			sentError := false
			if p.errMap != nil {
				err, ok := p.errMap[sv]
				if ok {
					p.errorCh <- &kafka.ProducerError{Msg: m, Err: err}
					sentError = true
				}
			}
			p.messages.Store(sv, &kafka.ProducerMessage{
				Topic:     m.Topic,
				Key:       m.Key,
				Value:     m.Value,
				Headers:   m.Headers,
				Timestamp: m.Timestamp,
			})
			if !async && !sentError {
				p.successCh <- &kafka.ProducerMessage{
					Topic:     m.Topic,
					Key:       m.Key,
					Value:     m.Value,
					Headers:   m.Headers,
					Timestamp: m.Timestamp,
					Metadata:  m.Metadata,
					Partition: p.resMap[sv].partition,
					Offset:    p.resMap[sv].offset,
				}
			}
		}
		close(p.errorCh)

	}()
	return p
}

func TestSaramaProduce(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name         string
		input        []model.ProduceMessage
		wantMessages []*kafka.ProducerMessage
		wantResults  []model.ProduceResult
	}{
		{
			name: "all",
			input: []model.ProduceMessage{
				{
					Key:       util.Ptr("foo1"),
					Value:     util.Ptr("bar1"),
					Headers:   map[string]string{"foo2": "bar2"},
					Timestamp: &ts,
				},
			},
			wantMessages: []*kafka.ProducerMessage{
				{
					Key:       kafka.StringEncoder("foo1"),
					Value:     kafka.StringEncoder("bar1"),
					Headers:   []kafka.RecordHeader{{Key: []byte("foo2"), Value: []byte("bar2")}},
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
					Key:       util.Ptr("foo1"),
					Value:     util.Ptr("bar1"),
					Headers:   map[string]string{"foo2": "bar2"},
					Timestamp: &ts,
				},
				{
					Key:       util.Ptr("foo3"),
					Value:     util.Ptr("bar3"),
					Headers:   map[string]string{"foo4": "bar4"},
					Timestamp: &ts,
				},
			},
			wantMessages: []*kafka.ProducerMessage{
				{
					Key:       kafka.StringEncoder("foo1"),
					Value:     kafka.StringEncoder("bar1"),
					Headers:   []kafka.RecordHeader{{Key: []byte("foo2"), Value: []byte("bar2")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
				{
					Key:       kafka.StringEncoder("foo3"),
					Value:     kafka.StringEncoder("bar3"),
					Headers:   []kafka.RecordHeader{{Key: []byte("foo4"), Value: []byte("bar4")}},
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
					Value:   util.Ptr("bar1"),
					Headers: map[string]string{"foo2": "bar2", "foo3": "bar3"},
				},
			},
			wantMessages: []*kafka.ProducerMessage{
				{
					Value: kafka.StringEncoder("bar1"),
					Headers: []kafka.RecordHeader{
						{Key: []byte("foo2"), Value: []byte("bar2")},
						{Key: []byte("foo3"), Value: []byte("bar3")},
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
					Value: util.Ptr("bar1"),
				},
			},
			wantMessages: []*kafka.ProducerMessage{
				{
					Value: kafka.StringEncoder("bar1"),
					Topic: testTopic,
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
		},
		{
			name: "blank value",
			input: []model.ProduceMessage{
				{
					Value: util.Ptr(""),
				},
			},
			wantMessages: []*kafka.ProducerMessage{
				{
					Value: kafka.StringEncoder(""),
					Topic: testTopic,
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
		},
		{
			name: "blank headers",
			input: []model.ProduceMessage{
				{
					Value:   util.Ptr("bar1"),
					Headers: map[string]string{"": ""},
				},
			},
			wantMessages: []*kafka.ProducerMessage{
				{
					Value:   kafka.StringEncoder("bar1"),
					Headers: []kafka.RecordHeader{{Key: []byte(""), Value: []byte("")}},
					Topic:   testTopic,
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sp, kp, res := sendSaramaMessages(tc.input)
			defer kp.Close()
			for _, m := range tc.wantMessages {
				v, _ := m.Value.Encode()
				r, _ := sp.messages.Load(string(v))
				require.ElementsMatch(t, m.Headers, r.(*kafka.ProducerMessage).Headers)
				m.Headers = []kafka.RecordHeader{}
				r.(*kafka.ProducerMessage).Headers = []kafka.RecordHeader{}
				require.Equal(t, m, r)
			}
			require.Equal(t, tc.wantResults, res)
		})
	}
}

func TestSaramaProduceAsync(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name         string
		input        []model.ProduceMessage
		wantMessages []*kafka.ProducerMessage
	}{
		{
			name: "all",
			input: []model.ProduceMessage{
				{
					Key:       util.Ptr("foo1"),
					Value:     util.Ptr("bar1"),
					Headers:   map[string]string{"foo2": "bar2"},
					Timestamp: &ts,
				},
			},
			wantMessages: []*kafka.ProducerMessage{
				{
					Key:       kafka.StringEncoder("foo1"),
					Value:     kafka.StringEncoder("bar1"),
					Headers:   []kafka.RecordHeader{{Key: []byte("foo2"), Value: []byte("bar2")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
			},
		},
		{
			name: "multiple messages",
			input: []model.ProduceMessage{
				{
					Key:       util.Ptr("foo1"),
					Value:     util.Ptr("bar1"),
					Headers:   map[string]string{"foo2": "bar2"},
					Timestamp: &ts,
				},
				{
					Key:       util.Ptr("foo3"),
					Value:     util.Ptr("bar3"),
					Headers:   map[string]string{"foo4": "bar4"},
					Timestamp: &ts,
				},
			},
			wantMessages: []*kafka.ProducerMessage{
				{
					Key:       kafka.StringEncoder("foo1"),
					Value:     kafka.StringEncoder("bar1"),
					Headers:   []kafka.RecordHeader{{Key: []byte("foo2"), Value: []byte("bar2")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
				{
					Key:       kafka.StringEncoder("foo3"),
					Value:     kafka.StringEncoder("bar3"),
					Headers:   []kafka.RecordHeader{{Key: []byte("foo4"), Value: []byte("bar4")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sp, kp, res := sendSaramaMessagesWith(tc.input, nil, true)
			defer kp.Close()
			for _, m := range tc.wantMessages {
				v, _ := m.Value.Encode()
				require.Eventually(t, func() bool {
					r, _ := sp.messages.Load(string(v))
					require.ElementsMatch(t, m.Headers, r.(*kafka.ProducerMessage).Headers)
					m.Headers = []kafka.RecordHeader{}
					r.(*kafka.ProducerMessage).Headers = []kafka.RecordHeader{}
					return assert.Equal(t, m, r)
				}, time.Second, 10*time.Millisecond)
			}
			require.Nil(t, res)
		})
	}
}

func TestSaramaProduceWithErrors(t *testing.T) {
	tests := []struct {
		name        string
		async       bool
		input       []model.ProduceMessage
		inputErrors map[string]error
		wantResults []model.ProduceResult
	}{
		{
			name:  "error event",
			input: []model.ProduceMessage{{Value: util.Ptr("bar1")}},
			inputErrors: map[string]error{
				"bar1": fmt.Errorf("test-error"),
			},
			wantResults: []model.ProduceResult{{Success: false}},
		},
		{
			name: "multiple error events",
			input: []model.ProduceMessage{
				{Value: util.Ptr("bar1")},
				{Value: util.Ptr("bar2")},
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
				{Value: util.Ptr("bar1")},
				{Value: util.Ptr("bar2")},
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
			input: []model.ProduceMessage{{Value: util.Ptr("bar1")}},
			inputErrors: map[string]error{
				"bar1": fmt.Errorf("test-error"),
			},
			wantResults: nil,
		},
		{
			name:  "multiple error events async",
			async: true,
			input: []model.ProduceMessage{
				{Value: util.Ptr("bar1")},
				{Value: util.Ptr("bar2")},
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
			_, kp, res := sendSaramaMessagesWith(tc.input, tc.inputErrors, tc.async)
			defer kp.Close()
			require.Equal(t, tc.wantResults, res)
		})
	}
}

func sendSaramaMessages(msgs []model.ProduceMessage) (*testSaramaProducer, producer.Producer, []model.ProduceResult) {
	return sendSaramaMessagesWith(msgs, nil, false)
}

func sendSaramaMessagesWith(
	msgs []model.ProduceMessage, errMap map[string]error, async bool,
) (*testSaramaProducer, producer.Producer, []model.ProduceResult) {
	sp := newTestSaramaAsyncProducer(async, errMap, msgs)
	cfg := &saramacfg.ProducerConfig{}
	ms, _ := metric.NewService(&config.MetricsConfig{})
	kp := newProducer(cfg, sp, ms)
	if !async {
		res, _ := kp.SendSync(context.Background(), messageBatch(testTopic, msgs))
		return sp, kp, res
	} else {
		kp.SendAsync(context.Background(), messageBatch(testTopic, msgs))
		return sp, kp, nil
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
