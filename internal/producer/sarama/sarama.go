package sarama

import (
	"context"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/metric"
	"koko/kafka-rest-producer/internal/model"
	"log/slog"
	"time"

	kafka "github.com/IBM/sarama"
)

type saramaAsyncProducer interface {
	Input() chan<- *kafka.ProducerMessage
	Errors() <-chan *kafka.ProducerError
	Successes() <-chan *kafka.ProducerMessage
	Close() error
}

func newSaramaAsyncProducer(cfg config.SaramaProducerConfig) (kafka.AsyncProducer, error) {
	sc, err := config.ToSaramaConfig(cfg.ClientConfig)
	if err != nil {
		return nil, err
	}
	addrs, err := config.ToSaramaAddrs(cfg.ClientConfig)
	if err != nil {
		return nil, err
	}
	return kafka.NewAsyncProducer(addrs, sc)
}

type kafkaProducer struct {
	cfg     config.SaramaProducerConfig
	ap      saramaAsyncProducer
	metrics metric.Service
}

type meta struct {
	src   *config.Endpoint
	resCh chan model.ProduceResult
	ctx   context.Context
}

func NewProducer(cfg config.SaramaProducerConfig, ms metric.Service) (*kafkaProducer, error) {
	p, err := newSaramaAsyncProducer(cfg)
	if err != nil {
		return nil, err
	}
	return newProducer(cfg, p, ms), nil
}

func newProducer(cfg config.SaramaProducerConfig, ap saramaAsyncProducer, ms metric.Service) *kafkaProducer {
	slog.Info("Creating producer.", "config", cfg)
	p := &kafkaProducer{cfg: cfg, ap: ap, metrics: ms}
	go func() {
		ctx := context.Background()
		for e := range ap.Errors() {
			err := fmt.Sprintf("Delivery failure: %s", e.Error())
			slog.Error("Kafka delivery failure.", "error", err)
			meta := e.Msg.Metadata.(*meta)
			if meta.resCh != nil {
				p.metrics.RecordEndpointMessage(meta.ctx, false, meta.src)
				meta.resCh <- model.ProduceResult{Error: &err}
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
				meta.resCh <- model.ProduceResult{Partition: &msg.Partition, Offset: &msg.Offset}
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

func (s *kafkaProducer) Async() bool {
	return s.cfg.Async
}

func (s *kafkaProducer) SendAsync(ctx context.Context, batch *model.MessageBatch) error {
	for i := range batch.Messages {
		msg := producerMessage(&batch.Messages[i])
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
		msg := producerMessage(&batch.Messages[i])
		resCh := make(chan model.ProduceResult, 1)
		resChs[i] = resCh
		msg.Metadata = &meta{src: batch.Src, resCh: resCh, ctx: ctx}
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

func producerMessage(m *model.TopicAndMessage) *kafka.ProducerMessage {
	msg := &kafka.ProducerMessage{Topic: m.Topic}
	if m.Message.Key != nil {
		msg.Key = kafka.StringEncoder(*m.Message.Key)
	}
	if m.Message.Value != nil {
		msg.Value = kafka.StringEncoder(*m.Message.Value)
	}
	if len(m.Message.Headers) > 0 {
		headers := make([]kafka.RecordHeader, len(m.Message.Headers))
		for j, h := range m.Message.Headers {
			headers[j] = kafka.RecordHeader{Key: []byte(*h.Key), Value: []byte(*h.Value)}
		}
		msg.Headers = headers
	}
	if m.Message.Timestamp != nil {
		msg.Timestamp = *m.Message.Timestamp
	}
	return msg
}

func (s *kafkaProducer) setupMetrics() {
	go func() {
		for range time.Tick(s.cfg.MetricsFlushDuration) {
			s.metrics.RecordSaramMetrics(s.cfg.ClientConfig.MetricRegistry)
		}
	}()
}
