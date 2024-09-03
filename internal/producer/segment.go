package producer

import (
	"context"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/model"
	"log/slog"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
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
	cfg    config.SegmentProducerConfig
	writer segmentWriter
	meters *segmentProducerMeters
}

type segmentProducerMeters struct {
	gaugeMap map[string]metric.Float64Gauge
	countMap map[string]metric.Float64Counter
}

func NewSegmentBasedProducer(cfg config.SegmentProducerConfig, writer segmentWriter) (*segmentProducer, error) {
	sp := &segmentProducer{cfg: cfg, writer: writer}
	writer.sendCallback(sp.sendCallback)
	if cfg.MetricsEnabled {
		meters, err := newSegmentProducerMeters()
		if err != nil {
			return nil, err
		}
		sp.meters = meters
		go func() {
			for range time.Tick(cfg.MetricsFlushDuration) {
				sp.recordStats()
			}
		}()
	}
	return sp, nil
}

func (s *segmentProducer) sendCallback(messages []kafka.Message, err error) {
	if s.cfg.Async {
		if err != nil {
			for range messages {
				err := fmt.Sprintf("Delivery failure: %s", err.Error())
				slog.Error("Kafka delivery failure.", "error", err)
			}
		}
	} else {
		for _, m := range messages {
			pc := m.WriterData.(PositionAndChannel)
			if err != nil {
				err := fmt.Sprintf("Delivery failure: %s", err.Error())
				slog.Error("Kafka delivery failure.", "error", err)
				pc.ResCh <- &PositionAndResult{Position: pc.Position, Result: model.ProduceResult{Error: &err}}
			} else {
				part := int32(m.Partition)
				pc.ResCh <- &PositionAndResult{Position: pc.Position, Result: model.ProduceResult{Partition: &part, Offset: &m.Offset}}
			}
		}
	}
}

func (s *segmentProducer) Send(messages []TopicAndMessage) []model.ProduceResult {
	if s.cfg.Async {
		s.sendAsync(messages)
		return nil
	} else {
		return s.sendSync(messages)
	}
}

func (s *segmentProducer) sendAsync(messages []TopicAndMessage) {
	segmentMsgs := make([]kafka.Message, len(messages))
	for i, m := range messages {
		segmentMsgs[i] = *toSegmentMessage(&m)
	}
	s.writer.writeMessages(context.Background(), segmentMsgs...)
}

type PositionAndChannel struct {
	Position int
	ResCh    chan *PositionAndResult
}

func (s *segmentProducer) sendSync(messages []TopicAndMessage) []model.ProduceResult {
	resCh := make(chan *PositionAndResult, len(messages))
	segmentMsgs := make([]kafka.Message, len(messages))
	for i, m := range messages {
		sm := toSegmentMessage(&m)
		sm.WriterData = PositionAndChannel{Position: i, ResCh: resCh}
		segmentMsgs[i] = *sm
	}
	s.writer.writeMessages(context.Background(), segmentMsgs...)
	res := make([]model.ProduceResult, len(messages))
	for range messages {
		r := <-resCh
		res[r.Position] = r.Result
	}
	close(resCh)
	return res
}

func toSegmentMessage(m *TopicAndMessage) *kafka.Message {
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

type segmentMeter struct {
	Name        string
	Description string
	Unit        string
}

func newSegmentProducerMeters() (*segmentProducerMeters, error) {
	gauges := []segmentMeter{
		{"kafka.writer.batch.seconds.avg", "", ""},
		{"kafka.writer.batch.seconds.min", "", ""},
		{"kafka.writer.batch.seconds.max", "", ""},
		{"kafka.writer.batch.queue.seconds.avg", "", ""},
		{"kafka.writer.batch.queue.seconds.min", "", ""},
		{"kafka.writer.batch.queue.seconds.max", "", ""},
		{"kafka.writer.write.seconds.avg", "", ""},
		{"kafka.writer.write.seconds.min", "", ""},
		{"kafka.writer.write.seconds.max", "", ""},
		{"kafka.writer.wait.seconds.avg", "", ""},
		{"kafka.writer.wait.seconds.min", "", ""},
		{"kafka.writer.wait.seconds.max", "", ""},
		{"kafka.writer.batch.size.avg", "", ""},
		{"kafka.writer.batch.size.min", "", ""},
		{"kafka.writer.batch.size.max", "", ""},
		{"kafka.writer.batch.bytes.avg", "", ""},
		{"kafka.writer.batch.bytes.min", "", ""},
		{"kafka.writer.batch.bytes.max", "", ""},
	}
	counters := []segmentMeter{
		{"kafka.writer.write.count", "", ""},
		{"kafka.writer.message.count", "", ""},
		{"kafka.writer.message.bytes", "", ""},
		{"kafka.writer.error.count", "", ""},
		{"kafka.writer.batch.seconds.count", "", ""},
		{"kafka.writer.batch.seconds.sum", "", ""},
		{"kafka.writer.batch.queue.seconds.count", "", ""},
		{"kafka.writer.batch.queue.seconds.sum", "", ""},
		{"kafka.writer.write.seconds.count", "", ""},
		{"kafka.writer.write.seconds.sum", "", ""},
		{"kafka.writer.wait.seconds.count", "", ""},
		{"kafka.writer.wait.seconds.sum", "", ""},
		{"kafka.writer.retries.count", "", ""},
		{"kafka.writer.batch.size.count", "", ""},
		{"kafka.writer.batch.size.sum", "", ""},
		{"kafka.writer.batch.bytes.count", "", ""},
		{"kafka.writer.batch.bytes.sum", "", ""},
	}

	meter := otel.Meter("koko/kafka-rest-producer")

	gaugeMap := make(map[string]metric.Float64Gauge, len(gauges))
	for _, sMeter := range gauges {
		g, err := meter.Float64Gauge(sMeter.Name, metric.WithDescription(sMeter.Description), metric.WithUnit(sMeter.Unit))
		if err != nil {
			return nil, err
		}
		gaugeMap[sMeter.Name] = g
	}

	counterMap := make(map[string]metric.Float64Counter, len(counters))
	for _, sMeter := range counters {
		c, err := meter.Float64Counter(sMeter.Name, metric.WithDescription(sMeter.Description), metric.WithUnit(sMeter.Unit))
		if err != nil {
			return nil, err
		}
		counterMap[sMeter.Name] = c
	}

	return &segmentProducerMeters{gaugeMap, counterMap}, nil
}

func (s *segmentProducer) recordStats() {
	stats := s.writer.stats()
	meters := s.meters
	ctx := context.Background()

	meters.countMap["kafka.writer.write.count"].Add(ctx, float64(stats.Writes))
	meters.countMap["kafka.writer.message.count"].Add(ctx, float64(stats.Messages))
	meters.countMap["kafka.writer.message.bytes"].Add(ctx, float64(stats.Bytes))
	meters.countMap["kafka.writer.error.count"].Add(ctx, float64(stats.Errors))

	meters.gaugeMap["kafka.writer.batch.seconds.avg"].Record(ctx, stats.BatchTime.Avg.Seconds())
	meters.gaugeMap["kafka.writer.batch.seconds.min"].Record(ctx, stats.BatchTime.Min.Seconds())
	meters.gaugeMap["kafka.writer.batch.seconds.max"].Record(ctx, stats.BatchTime.Max.Seconds())
	meters.countMap["kafka.writer.batch.seconds.count"].Add(ctx, float64(stats.BatchTime.Count))
	meters.countMap["kafka.writer.batch.seconds.sum"].Add(ctx, stats.BatchTime.Sum.Seconds())

	meters.gaugeMap["kafka.writer.batch.queue.seconds.avg"].Record(ctx, stats.BatchQueueTime.Avg.Seconds())
	meters.gaugeMap["kafka.writer.batch.queue.seconds.min"].Record(ctx, stats.BatchQueueTime.Min.Seconds())
	meters.gaugeMap["kafka.writer.batch.queue.seconds.max"].Record(ctx, stats.BatchQueueTime.Max.Seconds())
	meters.countMap["kafka.writer.batch.queue.seconds.count"].Add(ctx, float64(stats.BatchQueueTime.Count))
	meters.countMap["kafka.writer.batch.queue.seconds.sum"].Add(ctx, stats.BatchQueueTime.Sum.Seconds())

	meters.gaugeMap["kafka.writer.write.seconds.avg"].Record(ctx, stats.WriteTime.Avg.Seconds())
	meters.gaugeMap["kafka.writer.write.seconds.min"].Record(ctx, stats.WriteTime.Min.Seconds())
	meters.gaugeMap["kafka.writer.write.seconds.max"].Record(ctx, stats.WriteTime.Max.Seconds())
	meters.countMap["kafka.writer.write.seconds.count"].Add(ctx, float64(stats.WriteTime.Count))
	meters.countMap["kafka.writer.write.seconds.sum"].Add(ctx, stats.WriteTime.Sum.Seconds())

	meters.gaugeMap["kafka.writer.wait.seconds.avg"].Record(ctx, stats.WaitTime.Avg.Seconds())
	meters.gaugeMap["kafka.writer.wait.seconds.min"].Record(ctx, stats.WaitTime.Min.Seconds())
	meters.gaugeMap["kafka.writer.wait.seconds.max"].Record(ctx, stats.WaitTime.Max.Seconds())
	meters.countMap["kafka.writer.wait.seconds.count"].Add(ctx, float64(stats.WaitTime.Count))
	meters.countMap["kafka.writer.wait.seconds.sum"].Add(ctx, stats.WaitTime.Sum.Seconds())

	meters.countMap["kafka.writer.retries.count"].Add(ctx, float64(stats.Retries))

	meters.gaugeMap["kafka.writer.batch.size.avg"].Record(ctx, float64(stats.BatchSize.Avg))
	meters.gaugeMap["kafka.writer.batch.size.min"].Record(ctx, float64(stats.BatchSize.Min))
	meters.gaugeMap["kafka.writer.batch.size.max"].Record(ctx, float64(stats.BatchSize.Max))
	meters.countMap["kafka.writer.batch.size.count"].Add(ctx, float64(stats.BatchSize.Count))
	meters.countMap["kafka.writer.batch.size.sum"].Add(ctx, float64(stats.BatchSize.Sum))

	meters.gaugeMap["kafka.writer.batch.bytes.avg"].Record(ctx, float64(stats.BatchBytes.Avg))
	meters.gaugeMap["kafka.writer.batch.bytes.min"].Record(ctx, float64(stats.BatchBytes.Min))
	meters.gaugeMap["kafka.writer.batch.bytes.max"].Record(ctx, float64(stats.BatchBytes.Max))
	meters.countMap["kafka.writer.batch.bytes.count"].Add(ctx, float64(stats.BatchBytes.Count))
	meters.countMap["kafka.writer.batch.bytes.sum"].Add(ctx, float64(stats.BatchBytes.Sum))
}
