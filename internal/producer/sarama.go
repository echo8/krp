package producer

import (
	"context"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/metric"
	"koko/kafka-rest-producer/internal/model"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
)

type saramaAsyncProducer interface {
	Input() chan<- *sarama.ProducerMessage
	Errors() <-chan *sarama.ProducerError
	Successes() <-chan *sarama.ProducerMessage
	Close() error
}

func NewSaramaAsyncProducer(cfg config.SaramaProducerConfig) (sarama.AsyncProducer, error) {
	sc, err := config.ToSaramaConfig(cfg.ClientConfig)
	if err != nil {
		return nil, err
	}
	addrs, err := config.ToSaramaAddrs(cfg.ClientConfig)
	if err != nil {
		return nil, err
	}
	return sarama.NewAsyncProducer(addrs, sc)
}

type saramaProducer struct {
	cfg     config.SaramaProducerConfig
	ap      saramaAsyncProducer
	metrics metric.Service
}

type saramaMeta struct {
	src   *config.Endpoint
	resCh chan model.ProduceResult
	ctx   context.Context
}

func NewSaramaBasedProducer(cfg config.SaramaProducerConfig, ap saramaAsyncProducer, ms metric.Service) *saramaProducer {
	slog.Info("Creating producer.", "config", cfg)
	p := &saramaProducer{cfg: cfg, ap: ap, metrics: ms}
	go func() {
		ctx := context.Background()
		for e := range ap.Errors() {
			err := fmt.Sprintf("Delivery failure: %s", e.Error())
			slog.Error("Kafka delivery failure.", "error", err)
			meta := e.Msg.Metadata.(*saramaMeta)
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
			meta := msg.Metadata.(*saramaMeta)
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

func (s *saramaProducer) Async() bool {
	return s.cfg.Async
}

func (s *saramaProducer) SendAsync(ctx context.Context, batch *MessageBatch) error {
	for i := range batch.Messages {
		msg := producerMessage(&batch.Messages[i])
		msg.Metadata = &saramaMeta{src: batch.Src}
		select {
		case s.ap.Input() <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *saramaProducer) SendSync(ctx context.Context, batch *MessageBatch) ([]model.ProduceResult, error) {
	resChs := make([]chan model.ProduceResult, len(batch.Messages))
	res := make([]model.ProduceResult, len(batch.Messages))
	for i := range batch.Messages {
		msg := producerMessage(&batch.Messages[i])
		resCh := make(chan model.ProduceResult, 1)
		resChs[i] = resCh
		msg.Metadata = &saramaMeta{src: batch.Src, resCh: resCh, ctx: ctx}
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

func (s *saramaProducer) Close() error {
	if s.ap != nil {
		return s.ap.Close()
	}
	return nil
}

func producerMessage(m *TopicAndMessage) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{Topic: m.Topic}
	if m.Message.Key != nil {
		msg.Key = sarama.StringEncoder(*m.Message.Key)
	}
	if m.Message.Value != nil {
		msg.Value = sarama.StringEncoder(*m.Message.Value)
	}
	if m.Message.Headers != nil && len(m.Message.Headers) > 0 {
		headers := make([]sarama.RecordHeader, len(m.Message.Headers))
		for j, h := range m.Message.Headers {
			headers[j] = sarama.RecordHeader{Key: []byte(*h.Key), Value: []byte(*h.Value)}
		}
		msg.Headers = headers
	}
	if m.Message.Timestamp != nil {
		msg.Timestamp = *m.Message.Timestamp
	}
	return msg
}

func (s *saramaProducer) setupMetrics() {
	go func() {
		for range time.Tick(s.cfg.MetricsFlushDuration) {
			s.metrics.RecordSaramMetrics(s.cfg.ClientConfig.MetricRegistry)
		}
	}()
}
