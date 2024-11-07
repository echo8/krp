package confluent

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/echo8/krp/internal/config"
	confluentcfg "github.com/echo8/krp/internal/config/confluent"
	"github.com/echo8/krp/internal/metric"
	pmodel "github.com/echo8/krp/internal/model"
	"github.com/echo8/krp/internal/producer"
	"github.com/echo8/krp/internal/serializer"
	"github.com/echo8/krp/model"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type confluentKafkaProducer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Len() int
	Close()
}

func newConfluentKafkaProducer(cfg *confluentcfg.ProducerConfig) (confluentKafkaProducer, error) {
	kp, err := kafka.NewProducer(cfg.ClientConfig.ToConfigMap())
	if err != nil {
		return nil, err
	}
	return kp, nil
}

type kafkaProducer struct {
	config          *confluentcfg.ProducerConfig
	producer        confluentKafkaProducer
	asyncChan       chan kafka.Event
	metrics         metric.Service
	keySerializer   serializer.Serializer
	valueSerializer serializer.Serializer
}

type meta struct {
	src *config.Endpoint
	pos int
}

func NewProducer(cfg *confluentcfg.ProducerConfig, ms metric.Service, keySerializer serializer.Serializer,
	valueSerializer serializer.Serializer) (producer.Producer, error) {
	p, err := newConfluentKafkaProducer(cfg)
	if err != nil {
		return nil, err
	}
	return newProducer(cfg, p, ms, keySerializer, valueSerializer)
}

func newProducer(cfg *confluentcfg.ProducerConfig, rdp confluentKafkaProducer, ms metric.Service,
	keySerializer serializer.Serializer, valueSerializer serializer.Serializer) (producer.Producer, error) {
	slog.Info("Creating producer.", "config", cfg)
	asyncChan := make(chan kafka.Event, cfg.AsyncBufferSize)
	p := &kafkaProducer{cfg, rdp, asyncChan, ms, keySerializer, valueSerializer}
	go func() {
		ctx := context.Background()
		for e := range asyncChan {
			p.processResult(ctx, e)
		}
	}()
	go func() {
		for e := range rdp.Events() {
			p.processEvent(e)
		}
	}()
	return p, nil
}

type producerError struct {
	Error error
	Src   *config.Endpoint
	Pos   int
}

func (pe producerError) String() string {
	return pe.Error.Error()
}

func (k *kafkaProducer) SendAsync(ctx context.Context, batch *pmodel.MessageBatch) error {
	for i := range batch.Messages {
		msg, err := k.kafkaMessage(&batch.Messages[i], batch.Src)
		if err != nil {
			return err
		}
		err = k.producer.Produce(msg, k.asyncChan)
		if err != nil {
			slog.Error("Kafka delivery failure.", "error", err.Error())
			k.metrics.RecordEndpointMessage(ctx, false, batch.Src)
		}
	}
	return nil
}

func (k *kafkaProducer) SendSync(ctx context.Context, batch *pmodel.MessageBatch) ([]model.ProduceResult, error) {
	rcs := make([]chan kafka.Event, len(batch.Messages))
	for i := range batch.Messages {
		tm := &batch.Messages[i]
		msg, err := k.kafkaMessage(tm, batch.Src)
		if err != nil {
			return nil, err
		}
		rc := make(chan kafka.Event, 1)
		err = k.producer.Produce(msg, rc)
		if err != nil {
			select {
			case rc <- producerError{err, batch.Src, tm.Pos}:
			default:
				slog.Error("Failed to capture producer error.", "error", err.Error())
				k.metrics.RecordEndpointMessage(ctx, false, batch.Src)
			}
		}
		rcs[i] = rc
	}
	res := make([]model.ProduceResult, len(batch.Messages))
	for i, rc := range rcs {
		select {
		case e := <-rc:
			res[i] = k.processResult(ctx, e)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return res, nil
}

func (k *kafkaProducer) Close() error {
	k.producer.Close()
	return nil
}

func (k *kafkaProducer) processEvent(event kafka.Event) {
	switch ev := event.(type) {
	case *kafka.Stats:
		if k.metrics.Config().Enable.Producer {
			k.metrics.RecordConfluentMetrics(ev.String(), k.producer.Len(), len(k.asyncChan))
		}
	default:
		slog.Info("Kafka event received.", "event", ev.String())
	}
}

func (k *kafkaProducer) processResult(ctx context.Context, event kafka.Event) model.ProduceResult {
	switch ev := event.(type) {
	case *kafka.Message:
		meta := ev.Opaque.(*meta)
		if ev.TopicPartition.Error != nil {
			err := fmt.Sprintf("Delivery failure: %s", ev.TopicPartition.Error.Error())
			slog.Error("Kafka delivery failure.", "error", err)
			k.metrics.RecordEndpointMessage(ctx, false, meta.src)
			return model.ProduceResult{Success: false, Pos: meta.pos}
		} else {
			k.metrics.RecordEndpointMessage(ctx, true, meta.src)
			return model.ProduceResult{Success: true, Pos: meta.pos}
		}
	case producerError:
		err := fmt.Sprintf("Delivery failure: %s", ev.String())
		slog.Error("Kafka delivery failure.", "error", err)
		k.metrics.RecordEndpointMessage(ctx, false, ev.Src)
		return model.ProduceResult{Success: false}
	default:
		err := fmt.Sprintf("Possible delivery failure. Unrecognizable event: %s", ev.String())
		slog.Error("Possible kafka delivery failure.", "error", err)
		return model.ProduceResult{Success: false, Pos: -1}
	}
}

func (k *kafkaProducer) kafkaMessage(m *pmodel.TopicAndMessage, src *config.Endpoint) (*kafka.Message, error) {
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &m.Topic, Partition: kafka.PartitionAny}}
	if m.Message.Key != nil {
		keyBytes, err := k.keySerializer.Serialize(m.Topic, m.Message)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize key: %w", err)
		}
		msg.Key = keyBytes
	}
	if m.Message.Value != nil {
		valueBytes, err := k.valueSerializer.Serialize(m.Topic, m.Message)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize value: %w", err)
		}
		msg.Value = valueBytes
	}
	if len(m.Message.Headers) > 0 {
		headers := make([]kafka.Header, len(m.Message.Headers))
		j := 0
		for k, v := range m.Message.Headers {
			headers[j] = kafka.Header{Key: k, Value: []byte(v)}
			j += 1
		}
		msg.Headers = headers
	}
	if m.Message.Timestamp != nil {
		msg.Timestamp = *m.Message.Timestamp
	}
	msg.Opaque = &meta{src: src, pos: m.Pos}
	return msg, nil
}
