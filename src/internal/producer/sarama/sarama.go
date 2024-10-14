package sarama

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/echo8/krp/internal/config"
	saramacfg "github.com/echo8/krp/internal/config/sarama"
	"github.com/echo8/krp/internal/metric"
	"github.com/echo8/krp/internal/model"
	"github.com/echo8/krp/internal/producer"
	"github.com/echo8/krp/internal/serializer"

	kafka "github.com/IBM/sarama"
)

type saramaAsyncProducer interface {
	Input() chan<- *kafka.ProducerMessage
	Errors() <-chan *kafka.ProducerError
	Successes() <-chan *kafka.ProducerMessage
	Close() error
}

func newSaramaAsyncProducer(cfg *saramacfg.ProducerConfig) (kafka.AsyncProducer, error) {
	sc, err := cfg.ClientConfig.ToConfig()
	if err != nil {
		return nil, err
	}
	addrs, err := cfg.ClientConfig.GetAddrs()
	if err != nil {
		return nil, err
	}
	return kafka.NewAsyncProducer(addrs, sc)
}

type kafkaProducer struct {
	cfg             *saramacfg.ProducerConfig
	ap              saramaAsyncProducer
	metrics         metric.Service
	keySerializer   serializer.Serializer
	valueSerializer serializer.Serializer
}

type meta struct {
	src   *config.Endpoint
	resCh chan model.ProduceResult
	ctx   context.Context
	pos   int
}

func NewProducer(cfg *saramacfg.ProducerConfig, ms metric.Service,
	keySerializer serializer.Serializer, valueSerializer serializer.Serializer) (producer.Producer, error) {
	p, err := newSaramaAsyncProducer(cfg)
	if err != nil {
		return nil, err
	}
	return newProducer(cfg, p, ms, keySerializer, valueSerializer), nil
}

func newProducer(cfg *saramacfg.ProducerConfig, ap saramaAsyncProducer, ms metric.Service,
	keySerializer serializer.Serializer, valueSerializer serializer.Serializer) producer.Producer {
	slog.Info("Creating producer.", "config", cfg)
	p := &kafkaProducer{
		cfg:             cfg,
		ap:              ap,
		metrics:         ms,
		keySerializer:   keySerializer,
		valueSerializer: valueSerializer,
	}
	go func() {
		ctx := context.Background()
		for e := range ap.Errors() {
			err := fmt.Sprintf("Delivery failure: %s", e.Error())
			slog.Error("Kafka delivery failure.", "error", err)
			meta := e.Msg.Metadata.(*meta)
			if meta.resCh != nil {
				p.metrics.RecordEndpointMessage(meta.ctx, false, meta.src)
				meta.resCh <- model.ProduceResult{Success: false, Pos: meta.pos}
			} else {
				p.metrics.RecordEndpointMessage(ctx, false, meta.src)
			}
		}
	}()
	go func() {
		ctx := context.Background()
		for msg := range ap.Successes() {
			meta := msg.Metadata.(*meta)
			if meta.resCh != nil {
				p.metrics.RecordEndpointMessage(meta.ctx, true, meta.src)
				meta.resCh <- model.ProduceResult{Success: true, Pos: meta.pos}
			} else {
				p.metrics.RecordEndpointMessage(ctx, true, meta.src)
			}
		}
	}()
	if p.metrics.Config().Enable.Producer {
		p.setupMetrics()
	}
	return p
}

func (s *kafkaProducer) SendAsync(ctx context.Context, batch *model.MessageBatch) error {
	for i := range batch.Messages {
		msg, err := s.producerMessage(&batch.Messages[i])
		if err != nil {
			return err
		}
		msg.Metadata = &meta{src: batch.Src}
		select {
		case s.ap.Input() <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *kafkaProducer) SendSync(ctx context.Context, batch *model.MessageBatch) ([]model.ProduceResult, error) {
	resChs := make([]chan model.ProduceResult, len(batch.Messages))
	res := make([]model.ProduceResult, len(batch.Messages))
	for i := range batch.Messages {
		tm := &batch.Messages[i]
		msg, err := s.producerMessage(tm)
		if err != nil {
			return nil, err
		}
		resCh := make(chan model.ProduceResult, 1)
		resChs[i] = resCh
		msg.Metadata = &meta{src: batch.Src, resCh: resCh, ctx: ctx, pos: tm.Pos}
		select {
		case s.ap.Input() <- msg:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	for i, resCh := range resChs {
		if res[i] == (model.ProduceResult{}) {
			select {
			case r := <-resCh:
				res[i] = r
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return res, nil
}

func (s *kafkaProducer) Close() error {
	if s.ap != nil {
		return s.ap.Close()
	}
	return nil
}

func (s *kafkaProducer) producerMessage(m *model.TopicAndMessage) (*kafka.ProducerMessage, error) {
	msg := &kafka.ProducerMessage{Topic: m.Topic}
	if m.Message.Key != nil {
		keyBytes, err := s.keySerializer.Serialize(m.Topic, m.Message)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize key: %w", err)
		}
		msg.Key = kafka.ByteEncoder(keyBytes)
	}
	if m.Message.Value != nil {
		valueBytes, err := s.valueSerializer.Serialize(m.Topic, m.Message)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize value: %w", err)
		}
		msg.Value = kafka.ByteEncoder(valueBytes)
	}
	if len(m.Message.Headers) > 0 {
		headers := make([]kafka.RecordHeader, len(m.Message.Headers))
		j := 0
		for k, v := range m.Message.Headers {
			headers[j] = kafka.RecordHeader{Key: []byte(k), Value: []byte(v)}
			j += 1
		}
		msg.Headers = headers
	}
	if m.Message.Timestamp != nil {
		msg.Timestamp = *m.Message.Timestamp
	}
	return msg, nil
}

func (s *kafkaProducer) setupMetrics() {
	go func() {
		for range time.Tick(s.cfg.MetricsFlushDuration) {
			s.metrics.RecordSaramMetrics(s.cfg.ClientConfig.MetricRegistry)
		}
	}()
}
