package producer

import (
	"context"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/metric"
	"koko/kafka-rest-producer/internal/model"
	"log/slog"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type segmentWriter interface {
	sendCallback(cb func(messages []kafka.Message, err error))
	writeMessages(ctx context.Context, msgs ...kafka.Message) error
	stats() kafka.WriterStats
	close() error
}

func NewSegmentWriter(cfg config.SegmentProducerConfig) (segmentWriter, error) {
	writer, err := config.ToSegmentWriter(cfg.ClientConfig)
	if err != nil {
		return nil, err
	}
	writer.Async = cfg.Async
	return &internalWriter{writer: writer}, nil
}

type internalWriter struct {
	writer *kafka.Writer
}

func (w *internalWriter) sendCallback(cb func(messages []kafka.Message, err error)) {
	w.writer.Completion = cb
}

func (w *internalWriter) writeMessages(ctx context.Context, msgs ...kafka.Message) error {
	return w.writer.WriteMessages(ctx, msgs...)
}

func (w *internalWriter) stats() kafka.WriterStats {
	return w.writer.Stats()
}

func (w *internalWriter) close() error {
	return w.writer.Close()
}

type segmentProducer struct {
	cfg     config.SegmentProducerConfig
	writer  segmentWriter
	metrics metric.Service
}

type segmentMeta struct {
	src   *config.Endpoint
	resCh chan model.ProduceResult
	ctx   context.Context
}

func NewSegmentBasedProducer(cfg config.SegmentProducerConfig, writer segmentWriter, ms metric.Service) (*segmentProducer, error) {
	slog.Info("Creating producer.", "config", cfg)
	sp := &segmentProducer{cfg: cfg, writer: writer, metrics: ms}
	writer.sendCallback(sp.sendCallback)
	if sp.metrics.Config().Enable.Producer {
		go func() {
			for range time.Tick(cfg.MetricsFlushDuration) {
				sp.metrics.RecordSegmentMetrics(sp.writer.stats())
			}
		}()
	}
	return sp, nil
}

func (s *segmentProducer) sendCallback(messages []kafka.Message, err error) {
	if s.cfg.Async {
		if err != nil {
			ctx := context.Background()
			for i := range messages {
				meta := messages[i].WriterData.(*segmentMeta)
				err := fmt.Sprintf("Delivery failure: %s", err.Error())
				slog.Error("Kafka delivery failure.", "error", err)
				s.metrics.RecordEndpointMessage(ctx, false, meta.src)
			}
		}
	} else {
		for i := range messages {
			meta := messages[i].WriterData.(*segmentMeta)
			if err != nil {
				err := fmt.Sprintf("Delivery failure: %s", err.Error())
				slog.Error("Kafka delivery failure.", "error", err)
				s.metrics.RecordEndpointMessage(meta.ctx, false, meta.src)
				meta.resCh <- model.ProduceResult{Error: &err}
			} else {
				s.metrics.RecordEndpointMessage(meta.ctx, true, meta.src)
				part := int32(messages[i].Partition)
				meta.resCh <- model.ProduceResult{Partition: &part, Offset: &messages[i].Offset}
			}
		}
	}
}

func (s *segmentProducer) Async() bool {
	return s.cfg.Async
}

func (s *segmentProducer) SendAsync(ctx context.Context, batch *MessageBatch) error {
	segmentMsgs := make([]kafka.Message, len(batch.Messages))
	for i := range batch.Messages {
		sm := segmentMessage(&batch.Messages[i])
		sm.WriterData = &segmentMeta{src: batch.Src}
		segmentMsgs[i] = *sm
	}
	s.writer.writeMessages(ctx, segmentMsgs...)
	return nil
}

func (s *segmentProducer) SendSync(ctx context.Context, batch *MessageBatch) ([]model.ProduceResult, error) {
	resChs := make([]chan model.ProduceResult, len(batch.Messages))
	segmentMsgs := make([]kafka.Message, len(batch.Messages))
	for i := range batch.Messages {
		sm := segmentMessage(&batch.Messages[i])
		resCh := make(chan model.ProduceResult, 1)
		sm.WriterData = &segmentMeta{src: batch.Src, resCh: resCh, ctx: ctx}
		resChs[i] = resCh
		segmentMsgs[i] = *sm
	}
	s.writer.writeMessages(ctx, segmentMsgs...)
	res := make([]model.ProduceResult, len(batch.Messages))
	for i := range batch.Messages {
		select {
		case r := <-resChs[i]:
			res[i] = r
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return res, nil
}

func segmentMessage(m *TopicAndMessage) *kafka.Message {
	msg := &kafka.Message{Topic: m.Topic}
	if m.Message.Key != nil {
		msg.Key = []byte(*m.Message.Key)
	}
	if m.Message.Value != nil {
		msg.Value = []byte(*m.Message.Value)
	}
	if m.Message.Headers != nil && len(m.Message.Headers) > 0 {
		headers := make([]kafka.Header, len(m.Message.Headers))
		for j, h := range m.Message.Headers {
			headers[j] = kafka.Header{Key: *h.Key, Value: []byte(*h.Value)}
		}
		msg.Headers = headers
	}
	if m.Message.Timestamp != nil {
		msg.Time = *m.Message.Timestamp
	}
	return msg
}

func (s *segmentProducer) Close() error {
	return s.writer.close()
}
