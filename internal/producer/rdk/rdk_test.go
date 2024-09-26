package rdk

import (
	"context"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/metric"
	"koko/kafka-rest-producer/internal/model"
	"koko/kafka-rest-producer/internal/util"
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
					Key:       util.Ptr("foo1"),
					Value:     util.Ptr("bar1"),
					Headers:   []model.ProduceHeader{{Key: util.Ptr("foo2"), Value: util.Ptr("bar2")}},
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
			wantResults: []model.ProduceResult{{Partition: util.Ptr(int32(7)), Offset: util.Ptr(int64(77))}},
		},
		{
			name: "multiple messages",
			input: []model.ProduceMessage{
				{
					Key:       util.Ptr("foo1"),
					Value:     util.Ptr("bar1"),
					Headers:   []model.ProduceHeader{{Key: util.Ptr("foo2"), Value: util.Ptr("bar2")}},
					Timestamp: &ts,
				},
				{
					Key:       util.Ptr("foo3"),
					Value:     util.Ptr("bar3"),
					Headers:   []model.ProduceHeader{{Key: util.Ptr("foo4"), Value: util.Ptr("bar4")}},
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
				{Partition: util.Ptr(int32(7)), Offset: util.Ptr(int64(77))},
				{Partition: util.Ptr(int32(8)), Offset: util.Ptr(int64(78))},
			},
		},
		{
			name: "multiple headers",
			input: []model.ProduceMessage{
				{
					Value: util.Ptr("bar1"),
					Headers: []model.ProduceHeader{
						{Key: util.Ptr("foo2"), Value: util.Ptr("bar2")},
						{Key: util.Ptr("foo3"), Value: util.Ptr("bar3")},
					},
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
			wantResults: []model.ProduceResult{{Partition: util.Ptr(int32(7)), Offset: util.Ptr(int64(77))}},
		},
		{
			name: "value only",
			input: []model.ProduceMessage{
				{
					Value: util.Ptr("bar1"),
				},
			},
			wantMessages: []*kafka.Message{
				{
					Value:          []byte("bar1"),
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
				},
			},
			wantResults: []model.ProduceResult{{Partition: util.Ptr(int32(7)), Offset: util.Ptr(int64(77))}},
		},
		{
			name: "blank value",
			input: []model.ProduceMessage{
				{
					Value: util.Ptr(""),
				},
			},
			wantMessages: []*kafka.Message{
				{
					Value:          []byte(""),
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
				},
			},
			wantResults: []model.ProduceResult{{Partition: util.Ptr(int32(7)), Offset: util.Ptr(int64(77))}},
		},
		{
			name: "blank headers",
			input: []model.ProduceMessage{
				{
					Value:   util.Ptr("bar1"),
					Headers: []model.ProduceHeader{{Key: util.Ptr(""), Value: util.Ptr("")}},
				},
			},
			wantMessages: []*kafka.Message{
				{
					Value:          []byte("bar1"),
					Headers:        []kafka.Header{{Key: "", Value: []byte("")}},
					TopicPartition: kafka.TopicPartition{Topic: util.Ptr(testTopic), Partition: kafka.PartitionAny},
				},
			},
			wantResults: []model.ProduceResult{{Partition: util.Ptr(int32(7)), Offset: util.Ptr(int64(77))}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rdp, kp, res := sendMessages(tc.input)
			defer kp.Close()
			require.Equal(t, tc.wantMessages, rdp.messages)
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
					Key:       util.Ptr("foo1"),
					Value:     util.Ptr("bar1"),
					Headers:   []model.ProduceHeader{{Key: util.Ptr("foo2"), Value: util.Ptr("bar2")}},
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
					Key:       util.Ptr("foo1"),
					Value:     util.Ptr("bar1"),
					Headers:   []model.ProduceHeader{{Key: util.Ptr("foo2"), Value: util.Ptr("bar2")}},
					Timestamp: &ts,
				},
				{
					Key:       util.Ptr("foo3"),
					Value:     util.Ptr("bar3"),
					Headers:   []model.ProduceHeader{{Key: util.Ptr("foo4"), Value: util.Ptr("bar4")}},
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
			require.Equal(t, tc.wantMessages, rdp.messages)
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
			input:       []model.ProduceMessage{{Value: util.Ptr("bar1")}},
			inputEvents: []kafka.Event{&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error")}, Opaque: &config.Endpoint{}}},
			wantResults: []model.ProduceResult{{Error: util.Ptr("Delivery failure: test-error")}},
		},
		{
			name:        "immediate error",
			input:       []model.ProduceMessage{{Value: util.Ptr("bar1")}},
			inputErrors: []error{fmt.Errorf("test-error")},
			wantResults: []model.ProduceResult{{Error: util.Ptr("Delivery failure: test-error")}},
		},
		{
			name:        "unrecognized event",
			input:       []model.ProduceMessage{{Value: util.Ptr("bar1")}},
			inputEvents: []kafka.Event{kafka.OffsetsCommitted{}},
			wantResults: []model.ProduceResult{{Error: util.Ptr("Possible delivery failure. Unrecognizable event: OffsetsCommitted (<nil>, [])")}},
		},
		{
			name: "multiple error events",
			input: []model.ProduceMessage{
				{Value: util.Ptr("bar1")},
				{Value: util.Ptr("bar2")},
			},
			inputEvents: []kafka.Event{
				&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error1")}, Opaque: &config.Endpoint{}},
				&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error2")}, Opaque: &config.Endpoint{}},
			},
			wantResults: []model.ProduceResult{
				{Error: util.Ptr("Delivery failure: test-error1")},
				{Error: util.Ptr("Delivery failure: test-error2")},
			},
		},
		{
			name: "both error and success events",
			input: []model.ProduceMessage{
				{Value: util.Ptr("bar1")},
				{Value: util.Ptr("bar2")},
			},
			inputEvents: []kafka.Event{
				&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error")}, Opaque: &config.Endpoint{}},
				&kafka.Message{TopicPartition: kafka.TopicPartition{Partition: int32(7), Offset: kafka.Offset(77)}, Opaque: &config.Endpoint{}},
			},
			wantResults: []model.ProduceResult{
				{Error: util.Ptr("Delivery failure: test-error")},
				{Partition: util.Ptr(int32(7)), Offset: util.Ptr(int64(77))},
			},
		},
		{
			name:        "error event async",
			async:       true,
			input:       []model.ProduceMessage{{Value: util.Ptr("bar1")}},
			inputEvents: []kafka.Event{&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error")}, Opaque: &config.Endpoint{}}},
			wantResults: nil,
		},
		{
			name:        "immediate error async",
			async:       true,
			input:       []model.ProduceMessage{{Value: util.Ptr("bar1")}},
			inputErrors: []error{fmt.Errorf("test-error")},
			wantResults: nil,
		},
		{
			name:        "unrecognized event async",
			async:       true,
			input:       []model.ProduceMessage{{Value: util.Ptr("bar1")}},
			inputEvents: []kafka.Event{kafka.OffsetsCommitted{}},
			wantResults: nil,
		},
		{
			name:  "multiple error events async",
			async: true,
			input: []model.ProduceMessage{
				{Value: util.Ptr("bar1")},
				{Value: util.Ptr("bar2")},
			},
			inputEvents: []kafka.Event{
				&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error1")}, Opaque: &config.Endpoint{}},
				&kafka.Message{TopicPartition: kafka.TopicPartition{Error: fmt.Errorf("test-error2")}, Opaque: &config.Endpoint{}},
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

func sendMessages(msgs []model.ProduceMessage) (*TestRdKafkaProducer, *kafkaProducer, []model.ProduceResult) {
	return sendMessagesWith(msgs, nil, nil, false)
}

func sendMessagesWith(
	msgs []model.ProduceMessage, events []kafka.Event, errs []error, async bool,
) (*TestRdKafkaProducer, *kafkaProducer, []model.ProduceResult) {
	if events == nil {
		events = make([]kafka.Event, len(msgs))
		for i := range msgs {
			part := int32(7 + i)
			offset := kafka.Offset(77 + i)
			events[i] = &kafka.Message{TopicPartition: kafka.TopicPartition{Partition: part, Offset: offset}, Opaque: &config.Endpoint{}}
		}
	}
	rdp := newTestRdkProducer(events, errs)
	cfg := config.RdKafkaProducerConfig{Async: async}
	ms, _ := metric.NewService(&config.MetricsConfig{})
	kp, _ := newProducer(cfg, rdp, ms)
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
		res[i] = model.TopicAndMessage{Topic: topic, Message: &msg}
	}
	return &model.MessageBatch{Messages: res, Src: &config.Endpoint{}}
}
