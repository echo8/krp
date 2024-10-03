package segment

import (
	"context"
	"echo8/kafka-rest-producer/internal/config"
	segmentcfg "echo8/kafka-rest-producer/internal/config/segment"
	"echo8/kafka-rest-producer/internal/metric"
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/producer"
	"fmt"
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

func newSegmentWriter(cfg *segmentcfg.ProducerConfig, async bool) (segmentWriter, error) {
	writer, err := cfg.ClientConfig.ToWriter()
	if err != nil {
		return nil, err
	}
	writer.Async = async
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

type kafkaProducer struct {
	cfg         *segmentcfg.ProducerConfig
	writerAsync segmentWriter
	writerSync  segmentWriter
	metrics     metric.Service
}

type meta struct {
	src   *config.Endpoint
	resCh chan model.ProduceResult
	ctx   context.Context
	pos   int
}

func NewProducer(cfg *segmentcfg.ProducerConfig, ms metric.Service) (producer.Producer, error) {
	slog.Info("Creating producer.", "config", cfg)
	writeAsync, err := newSegmentWriter(cfg, true)
	if err != nil {
		return nil, err
	}
	writerSync, err := newSegmentWriter(cfg, false)
	if err != nil {
		return nil, err
	}
	return newProducer(cfg, writeAsync, writerSync, ms)
}

func newProducer(cfg *segmentcfg.ProducerConfig, writerAsync, writerSync segmentWriter, ms metric.Service) (producer.Producer, error) {
	sp := &kafkaProducer{cfg: cfg, writerAsync: writerAsync, writerSync: writerSync, metrics: ms}
	writerAsync.sendCallback(sp.sendCallbackAsync)
	writerSync.sendCallback(sp.sendCallbackSync)
	if sp.metrics.Config().Enable.Producer {
		go func() {
			for range time.Tick(cfg.MetricsFlushDuration) {
				sp.metrics.RecordSegmentMetrics(sp.writerAsync.stats())
				sp.metrics.RecordSegmentMetrics(sp.writerSync.stats())
			}
		}()
	}
	return sp, nil
}

func (s *kafkaProducer) sendCallbackAsync(messages []kafka.Message, err error) {
	if err != nil {
		ctx := context.Background()
		for i := range messages {
			meta := messages[i].WriterData.(*meta)
			err := fmt.Sprintf("Delivery failure: %s", err.Error())
			slog.Error("Kafka delivery failure.", "error", err)
			s.metrics.RecordEndpointMessage(ctx, false, meta.src)
		}
	}
}

func (s *kafkaProducer) sendCallbackSync(messages []kafka.Message, err error) {
	for i := range messages {
		meta := messages[i].WriterData.(*meta)
		if err != nil {
			err := fmt.Sprintf("Delivery failure: %s", err.Error())
			slog.Error("Kafka delivery failure.", "error", err)
			s.metrics.RecordEndpointMessage(meta.ctx, false, meta.src)
			meta.resCh <- model.ProduceResult{Success: false, Pos: meta.pos}
		} else {
			s.metrics.RecordEndpointMessage(meta.ctx, true, meta.src)
			meta.resCh <- model.ProduceResult{Success: true, Pos: meta.pos}
		}
	}
}

func (s *kafkaProducer) SendAsync(ctx context.Context, batch *model.MessageBatch) error {
	segmentMsgs := make([]kafka.Message, len(batch.Messages))
	for i := range batch.Messages {
		sm := segmentMessage(&batch.Messages[i])
		sm.WriterData = &meta{src: batch.Src}
		segmentMsgs[i] = *sm
	}
	s.writerAsync.writeMessages(ctx, segmentMsgs...)
	return nil
}

func (s *kafkaProducer) SendSync(ctx context.Context, batch *model.MessageBatch) ([]model.ProduceResult, error) {
	resChs := make([]chan model.ProduceResult, len(batch.Messages))
	segmentMsgs := make([]kafka.Message, len(batch.Messages))
	for i := range batch.Messages {
		tm := &batch.Messages[i]
		sm := segmentMessage(tm)
		resCh := make(chan model.ProduceResult, 1)
		sm.WriterData = &meta{src: batch.Src, resCh: resCh, ctx: ctx, pos: tm.Pos}
		resChs[i] = resCh
		segmentMsgs[i] = *sm
	}
	s.writerSync.writeMessages(ctx, segmentMsgs...)
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

func segmentMessage(m *model.TopicAndMessage) *kafka.Message {
	msg := &kafka.Message{Topic: m.Topic}
	if m.Message.Key != nil {
		msg.Key = []byte(*m.Message.Key)
	}
	if m.Message.Value != nil {
		msg.Value = []byte(*m.Message.Value)
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
		msg.Time = *m.Message.Timestamp
	}
	return msg
}

func (s *kafkaProducer) Close() error {
	err := s.writerAsync.close()
	if err != nil {
		s.writerSync.close()
		return err
	}
	return s.writerSync.close()
}
