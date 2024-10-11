package rdk

import (
	"context"
	"echo8/kafka-rest-producer/internal/config"
	rdkcfg "echo8/kafka-rest-producer/internal/config/rdk"
	"echo8/kafka-rest-producer/internal/metric"
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/producer"
	"echo8/kafka-rest-producer/internal/serializer"
	"echo8/kafka-rest-producer/internal/util"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

type TestRdKafkaProducer struct {
	numCalls     int
	messages     []*kafka.Message
	returnEvents []kafka.Event
	returnErrors []error
}

func (p *TestRdKafkaProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	i := p.numCalls
	p.numCalls++
	if p.returnErrors != nil {
		return p.returnErrors[i]
	}
	msg.Opaque = nil
	p.messages[i] = msg
	deliveryChan <- p.returnEvents[i]
	return nil
}

func (p *TestRdKafkaProducer) Events() chan kafka.Event {
	return nil
}

func (p *TestRdKafkaProducer) Len() int {
	return 0
}

func (p *TestRdKafkaProducer) Close() {}

func newTestRdkProducer(events []kafka.Event, errors []error) *TestRdKafkaProducer {
	rdp := &TestRdKafkaProducer{returnEvents: events, returnErrors: errors}
	if events != nil {
		rdp.messages = make([]*kafka.Message, len(events))
	} else {
		rdp.messages = make([]*kafka.Message, len(errors))
	}
	return rdp
}

func TestProduce(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name         string
		input        []model.ProduceMessage
		wantMessages []*kafka.Message
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
			wantMessages: []*kafka.Message{
				{
					Key:            []byte("foo1"),
					Value:          []byte("bar1"),
					Headers:        []kafka.Header{{Key: "foo2", Value: []byte("bar2")}},
					Timestamp:      ts,
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
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
			wantMessages: []*kafka.Message{
				{
					Key:            []byte("foo1"),
					Value:          []byte("bar1"),
					Headers:        []kafka.Header{{Key: "foo2", Value: []byte("bar2")}},
					Timestamp:      ts,
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
				},
				{
					Key:            []byte("foo3"),
					Value:          []byte("bar3"),
					Headers:        []kafka.Header{{Key: "foo4", Value: []byte("bar4")}},
					Timestamp:      ts,
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
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
			wantMessages: []*kafka.Message{
				{
					Value: []byte("bar1"),
					Headers: []kafka.Header{
						{Key: "foo2", Value: []byte("bar2")},
						{Key: "foo3", Value: []byte("bar3")},
					},
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
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
			wantMessages: []*kafka.Message{
				{
					Value:          []byte("bar1"),
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
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
			wantMessages: []*kafka.Message{
				{
					Value:          []byte(""),
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
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
			wantMessages: []*kafka.Message{
				{
					Value:          []byte("bar1"),
					Headers:        []kafka.Header{{Key: "", Value: []byte("")}},
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
				},
			},
			wantResults: []model.ProduceResult{{Success: true}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rdp, kp, res := sendMessages(tc.input)
			defer kp.Close()
			for i := range tc.wantMessages {
				m := tc.wantMessages[i]
				r := rdp.messages[i]
				require.ElementsMatch(t, m.Headers, r.Headers)
				m.Headers = []kafka.Header{}
				r.Headers = []kafka.Header{}
				require.Equal(t, m, r)
			}
			require.Equal(t, tc.wantResults, res)
		})
	}
}

func TestProduceAsync(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name         string
		input        []model.ProduceMessage
		wantMessages []*kafka.Message
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
			wantMessages: []*kafka.Message{
				{
					Key:            []byte("foo1"),
					Value:          []byte("bar1"),
					Headers:        []kafka.Header{{Key: "foo2", Value: []byte("bar2")}},
					Timestamp:      ts,
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
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
			wantMessages: []*kafka.Message{
				{
					Key:            []byte("foo1"),
					Value:          []byte("bar1"),
					Headers:        []kafka.Header{{Key: "foo2", Value: []byte("bar2")}},
					Timestamp:      ts,
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
				},
				{
					Key:            []byte("foo3"),
					Value:          []byte("bar3"),
					Headers:        []kafka.Header{{Key: "foo4", Value: []byte("bar4")}},
					Timestamp:      ts,
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rdp, kp, res := sendMessagesWith(tc.input, nil, nil, true)
			defer kp.Close()
			for i := range tc.wantMessages {
				m := tc.wantMessages[i]
				r := rdp.messages[i]
				require.ElementsMatch(t, m.Headers, r.Headers)
				m.Headers = []kafka.Header{}
				r.Headers = []kafka.Header{}
				require.Equal(t, m, r)
			}
			require.Nil(t, res)
		})
	}
}

func TestProduceWithErrors(t *testing.T) {
	tests := []struct {
		name        string
		async       bool
		input       []model.ProduceMessage
		inputEvents []kafka.Event
		inputErrors []error
		wantResults []model.ProduceResult
	}{
		{
			name:        "error event",
			input:       []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
			inputEvents: []kafka.Event{&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error")}, Opaque: &meta{&config.Endpoint{}, 0}}},
			wantResults: []model.ProduceResult{{Success: false}},
		},
		{
			name:        "immediate error",
			input:       []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
			inputErrors: []error{fmt.Errorf("test-error")},
			wantResults: []model.ProduceResult{{Success: false}},
		},
		{
			name:        "unrecognized event",
			input:       []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
			inputEvents: []kafka.Event{kafka.OffsetsCommitted{}},
			wantResults: []model.ProduceResult{{Success: false, Pos: -1}},
		},
		{
			name: "multiple error events",
			input: []model.ProduceMessage{
				{Value: &model.ProduceData{String: util.Ptr("bar1")}},
				{Value: &model.ProduceData{String: util.Ptr("bar2")}},
			},
			inputEvents: []kafka.Event{
				&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error1")}, Opaque: &meta{&config.Endpoint{}, 0}},
				&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error2")}, Opaque: &meta{&config.Endpoint{}, 1}},
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
			inputEvents: []kafka.Event{
				&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error")}, Opaque: &meta{&config.Endpoint{}, 0}},
				&kafka.Message{TopicPartition: kafka.TopicPartition{Partition: int32(7), Offset: kafka.Offset(77)}, Opaque: &meta{&config.Endpoint{}, 1}},
			},
			wantResults: []model.ProduceResult{
				{Success: false, Pos: 0},
				{Success: true, Pos: 1},
			},
		},
		{
			name:        "error event async",
			async:       true,
			input:       []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
			inputEvents: []kafka.Event{&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error")}, Opaque: &meta{&config.Endpoint{}, 0}}},
			wantResults: nil,
		},
		{
			name:        "immediate error async",
			async:       true,
			input:       []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
			inputErrors: []error{fmt.Errorf("test-error")},
			wantResults: nil,
		},
		{
			name:        "unrecognized event async",
			async:       true,
			input:       []model.ProduceMessage{{Value: &model.ProduceData{String: util.Ptr("bar1")}}},
			inputEvents: []kafka.Event{kafka.OffsetsCommitted{}},
			wantResults: nil,
		},
		{
			name:  "multiple error events async",
			async: true,
			input: []model.ProduceMessage{
				{Value: &model.ProduceData{String: util.Ptr("bar1")}},
				{Value: &model.ProduceData{String: util.Ptr("bar2")}},
			},
			inputEvents: []kafka.Event{
				&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error1")}, Opaque: &meta{&config.Endpoint{}, 0}},
				&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error2")}, Opaque: &meta{&config.Endpoint{}, 1}},
			},
			wantResults: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, kp, res := sendMessagesWith(tc.input, tc.inputEvents, tc.inputErrors, tc.async)
			defer kp.Close()
			require.Equal(t, tc.wantResults, res)
		})
	}
}

const testTopic string = "test-topic"

func sendMessages(msgs []model.ProduceMessage) (*TestRdKafkaProducer, producer.Producer, []model.ProduceResult) {
	return sendMessagesWith(msgs, nil, nil, false)
}

func sendMessagesWith(
	msgs []model.ProduceMessage, events []kafka.Event, errs []error, async bool,
) (*TestRdKafkaProducer, producer.Producer, []model.ProduceResult) {
	if events == nil {
		events = make([]kafka.Event, len(msgs))
		for i := range msgs {
			part := int32(7 + i)
			offset := kafka.Offset(77 + i)
			events[i] = &kafka.Message{TopicPartition: kafka.TopicPartition{Partition: part, Offset: offset}, Opaque: &meta{&config.Endpoint{}, i}}
		}
	}
	rdp := newTestRdkProducer(events, errs)
	cfg := &rdkcfg.ProducerConfig{}
	ms, _ := metric.NewService(&config.MetricsConfig{})
	keySerializer, _ := serializer.NewSerializer(cfg.SchemaRegistry, true)
	valueSerializer, _ := serializer.NewSerializer(cfg.SchemaRegistry, false)
	kp, _ := newProducer(cfg, rdp, ms, keySerializer, valueSerializer)
	if !async {
		res, _ := kp.SendSync(context.Background(), messageBatch(testTopic, msgs))
		return rdp, kp, res
	} else {
		kp.SendAsync(context.Background(), messageBatch(testTopic, msgs))
		return rdp, kp, nil
	}
}

func messageBatch(topic string, messages []model.ProduceMessage) *model.MessageBatch {
	res := make([]model.TopicAndMessage, len(messages))
	for i, msg := range messages {
		res[i] = model.TopicAndMessage{Topic: topic, Message: &msg, Pos: i}
	}
	return &model.MessageBatch{Messages: res, Src: &config.Endpoint{}}
}
