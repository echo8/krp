package producer

import (
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/model"
	"koko/kafka-rest-producer/internal/util"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSaramaProducer struct {
	messages sync.Map
	resMap   map[string]saramaPartAndOffset
	errMap   map[string]error
	inputCh  chan *sarama.ProducerMessage
	errorCh  chan *sarama.ProducerError
}

type saramaPartAndOffset struct {
	partition int32
	offset    int64
}

func (p *testSaramaProducer) initAsync() {
	p.inputCh = make(chan *sarama.ProducerMessage)
	p.errorCh = make(chan *sarama.ProducerError)
	go func() {
		for m := range p.inputCh {
			v, _ := m.Value.Encode()
			sv := string(v)
			if p.errMap != nil {
				err, ok := p.errMap[sv]
				if ok {
					p.errorCh <- &sarama.ProducerError{Msg: m, Err: err}
				}
			}
			p.messages.Store(sv, m)
		}
		close(p.errorCh)
	}()
}

func (p *testSaramaProducer) initSync(input []model.ProduceMessage) {
	p.resMap = make(map[string]saramaPartAndOffset, len(input))
	startPart := int32(7)
	startOffs := int64(77)
	for i, m := range input {
		p.resMap[*m.Value] = saramaPartAndOffset{partition: startPart + int32(i), offset: startOffs + int64(i)}
	}
}

func (p *testSaramaProducer) Input() chan<- *sarama.ProducerMessage {
	return p.inputCh
}

func (p *testSaramaProducer) Errors() <-chan *sarama.ProducerError {
	return p.errorCh
}

func (p *testSaramaProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	v, _ := msg.Value.Encode()
	sv := string(v)
	if p.errMap != nil {
		err, ok := p.errMap[sv]
		if ok {
			return -1, -1, err
		}
	}
	p.messages.Store(sv, msg)
	res := p.resMap[sv]
	return res.partition, res.offset, nil
}

func (p *testSaramaProducer) Close() error {
	if p.inputCh != nil {
		close(p.inputCh)
	}
	return nil
}

func newTestSaramaAsyncProducer(errMap map[string]error) *testSaramaProducer {
	p := &testSaramaProducer{errMap: errMap}
	p.initAsync()
	return p
}

func newTestSaramaSyncProducer(input []model.ProduceMessage, errMap map[string]error) *testSaramaProducer {
	p := &testSaramaProducer{errMap: errMap}
	p.initSync(input)
	return p
}

func TestSaramaProduce(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2020-12-09T16:09:53+00:00")
	tests := []struct {
		name         string
		input        []model.ProduceMessage
		wantMessages []*sarama.ProducerMessage
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
			wantMessages: []*sarama.ProducerMessage{
				{
					Key:       sarama.StringEncoder("foo1"),
					Value:     sarama.StringEncoder("bar1"),
					Headers:   []sarama.RecordHeader{{Key: []byte("foo2"), Value: []byte("bar2")}},
					Timestamp: ts,
					Topic:     testTopic,
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
			wantMessages: []*sarama.ProducerMessage{
				{
					Key:       sarama.StringEncoder("foo1"),
					Value:     sarama.StringEncoder("bar1"),
					Headers:   []sarama.RecordHeader{{Key: []byte("foo2"), Value: []byte("bar2")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
				{
					Key:       sarama.StringEncoder("foo3"),
					Value:     sarama.StringEncoder("bar3"),
					Headers:   []sarama.RecordHeader{{Key: []byte("foo4"), Value: []byte("bar4")}},
					Timestamp: ts,
					Topic:     testTopic,
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
			wantMessages: []*sarama.ProducerMessage{
				{
					Value: sarama.StringEncoder("bar1"),
					Headers: []sarama.RecordHeader{
						{Key: []byte("foo2"), Value: []byte("bar2")},
						{Key: []byte("foo3"), Value: []byte("bar3")},
					},
					Topic: testTopic,
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
			wantMessages: []*sarama.ProducerMessage{
				{
					Value: sarama.StringEncoder("bar1"),
					Topic: testTopic,
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
			wantMessages: []*sarama.ProducerMessage{
				{
					Value: sarama.StringEncoder(""),
					Topic: testTopic,
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
			wantMessages: []*sarama.ProducerMessage{
				{
					Value:   sarama.StringEncoder("bar1"),
					Headers: []sarama.RecordHeader{{Key: []byte(""), Value: []byte("")}},
					Topic:   testTopic,
				},
			},
			wantResults: []model.ProduceResult{{Partition: util.Ptr(int32(7)), Offset: util.Ptr(int64(77))}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sp, kp, res := sendSaramaMessages(tc.input)
			defer kp.Close()
			for _, m := range tc.wantMessages {
				v, _ := m.Value.Encode()
				r, _ := sp.messages.Load(string(v))
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
		wantMessages []*sarama.ProducerMessage
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
			wantMessages: []*sarama.ProducerMessage{
				{
					Key:       sarama.StringEncoder("foo1"),
					Value:     sarama.StringEncoder("bar1"),
					Headers:   []sarama.RecordHeader{{Key: []byte("foo2"), Value: []byte("bar2")}},
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
			wantMessages: []*sarama.ProducerMessage{
				{
					Key:       sarama.StringEncoder("foo1"),
					Value:     sarama.StringEncoder("bar1"),
					Headers:   []sarama.RecordHeader{{Key: []byte("foo2"), Value: []byte("bar2")}},
					Timestamp: ts,
					Topic:     testTopic,
				},
				{
					Key:       sarama.StringEncoder("foo3"),
					Value:     sarama.StringEncoder("bar3"),
					Headers:   []sarama.RecordHeader{{Key: []byte("foo4"), Value: []byte("bar4")}},
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
				r, _ := sp.messages.Load(string(v))
				require.Eventually(t, func() bool {
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
			wantResults: []model.ProduceResult{{Error: util.Ptr("Delivery failure: test-error")}},
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
			inputErrors: map[string]error{
				"bar1": fmt.Errorf("test-error"),
			},
			wantResults: []model.ProduceResult{
				{Error: util.Ptr("Delivery failure: test-error")},
				{Partition: util.Ptr(int32(8)), Offset: util.Ptr(int64(78))},
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

func sendSaramaMessages(msgs []model.ProduceMessage) (*testSaramaProducer, *saramaProducer, []model.ProduceResult) {
	return sendSaramaMessagesWith(msgs, nil, false)
}

func sendSaramaMessagesWith(
	msgs []model.ProduceMessage, errMap map[string]error, async bool,
) (*testSaramaProducer, *saramaProducer, []model.ProduceResult) {
	if async {
		sp := newTestSaramaAsyncProducer(errMap)
		cfg := config.SaramaProducerConfig{Async: true}
		kp := NewSaramaBasedAsyncProducer(cfg, sp)
		res := kp.Send(toTopicMessages(testTopic, msgs))
		return sp, kp, res
	} else {
		sp := newTestSaramaSyncProducer(msgs, errMap)
		cfg := config.SaramaProducerConfig{Async: false}
		kp := NewSaramaBasedSyncProducer(cfg, sp)
		res := kp.Send(toTopicMessages(testTopic, msgs))
		return sp, kp, res
	}
}
