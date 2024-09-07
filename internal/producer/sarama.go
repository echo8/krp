package producer

import (
	"context"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/model"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
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
	sc.Producer.Return.Successes = true
	if cfg.MetricsEnabled {
		sc.MetricRegistry = metrics.DefaultRegistry
	}
	addrs, err := config.ToSaramaAddrs(cfg.ClientConfig)
	if err != nil {
		return nil, err
	}
	p, err := sarama.NewAsyncProducer(addrs, sc)
	return p, err
}

type saramaProducer struct {
	cfg      config.SaramaProducerConfig
	ap       saramaAsyncProducer
	meterMap map[string]metric.Float64Gauge
}

func NewSaramaBasedProducer(cfg config.SaramaProducerConfig, ap saramaAsyncProducer) *saramaProducer {
	slog.Info("Creating producer.", "config", cfg)
	go func() {
		for e := range ap.Errors() {
			err := fmt.Sprintf("Delivery failure: %s", e.Error())
			slog.Error("Kafka delivery failure.", "error", err)
			if e.Msg.Metadata != nil {
				resCh := e.Msg.Metadata.(chan model.ProduceResult)
				resCh <- model.ProduceResult{Error: &err}
			}
		}
	}()
	if !cfg.Async {
		go func() {
			for msg := range ap.Successes() {
				resCh := msg.Metadata.(chan model.ProduceResult)
				resCh <- model.ProduceResult{Partition: &msg.Partition, Offset: &msg.Offset}
			}
		}()
	}
	p := &saramaProducer{cfg: cfg, ap: ap}
	if cfg.MetricsEnabled {
		p.setupMetrics()
	}
	return p
}

func (s *saramaProducer) Send(ctx context.Context, messages []TopicAndMessage) []model.ProduceResult {
	if s.cfg.Async {
		s.sendAsync(ctx, messages)
		return nil
	} else {
		return s.sendSync(ctx, messages)
	}
}

func (s *saramaProducer) sendAsync(ctx context.Context, messages []TopicAndMessage) {
	for _, m := range messages {
		msg := toProducerMessage(&m)
		select {
		case s.ap.Input() <- msg:
		case <-ctx.Done():
			err := fmt.Sprintf("Kafka delivery failure. Request canceled: %s", ctx.Err().Error())
			slog.Error("Kafka delivery failure. Request canceled.", "error", err)
		}
	}
}

func (s *saramaProducer) sendSync(ctx context.Context, messages []TopicAndMessage) []model.ProduceResult {
	resChs := make([]chan model.ProduceResult, len(messages))
	res := make([]model.ProduceResult, len(messages))
	for i, m := range messages {
		msg := toProducerMessage(&m)
		resCh := make(chan model.ProduceResult, 1)
		resChs[i] = resCh
		msg.Metadata = resCh
		select {
		case s.ap.Input() <- msg:
		case <-ctx.Done():
			err := fmt.Sprintf("Kafka delivery failure. Request canceled: %s", ctx.Err().Error())
			slog.Error("Kafka delivery failure. Request canceled.", "error", err)
			res[i] = model.ProduceResult{Error: &err}
		}
	}
	for i, resCh := range resChs {
		if res[i] == (model.ProduceResult{}) {
			select {
			case r := <-resCh:
				res[i] = r
			case <-ctx.Done():
				err := fmt.Sprintf("Kafka delivery failure. Request canceled: %s", ctx.Err().Error())
				slog.Error("Kafka delivery failure. Request canceled.", "error", err)
				res[i] = model.ProduceResult{Error: &err}
			}
		}
	}
	return res
}

func (s *saramaProducer) Close() error {
	if s.ap != nil {
		return s.ap.Close()
	}
	return nil
}

func toProducerMessage(m *TopicAndMessage) *sarama.ProducerMessage {
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

func getOrCreateGauge(m map[string]metric.Float64Gauge, name string) metric.Float64Gauge {
	g, ok := m[name]
	if ok {
		return g
	} else {
		meter := otel.Meter("koko/kafka-rest-producer")
		g, err := meter.Float64Gauge("sarama." + name)
		if err != nil {
			slog.Error("Failed to create gauge meter.", "error", err.Error())
			return nil
		}
		return g
	}
}

func (s *saramaProducer) setupMetrics() {
	go func() {
		ctx := context.Background()
		for range time.Tick(s.cfg.MetricsFlushDuration) {
			slog.Info("Recording sarama metrics.")
			metrics.DefaultRegistry.Each(func(name string, i interface{}) {
				switch metric := i.(type) {
				case metrics.Counter:
					if g := getOrCreateGauge(s.meterMap, name); g != nil {
						g.Record(ctx, float64(metric.Count()))
					}
				case metrics.Gauge:
					if g := getOrCreateGauge(s.meterMap, name); g != nil {
						g.Record(ctx, float64(metric.Value()))
					}
				case metrics.GaugeFloat64:
					if g := getOrCreateGauge(s.meterMap, name); g != nil {
						g.Record(ctx, metric.Value())
					}
				case metrics.Histogram:
					h := metric.Snapshot()
					ps := h.Percentiles([]float64{0.5, 0.75, 0.90, 0.95, 0.99, 0.9999})
					if g := getOrCreateGauge(s.meterMap, name+".count"); g != nil {
						g.Record(ctx, float64(h.Count()))
					}
					if g := getOrCreateGauge(s.meterMap, name+".min"); g != nil {
						g.Record(ctx, float64(h.Min()))
					}
					if g := getOrCreateGauge(s.meterMap, name+".max"); g != nil {
						g.Record(ctx, float64(h.Max()))
					}
					if g := getOrCreateGauge(s.meterMap, name+".mean"); g != nil {
						g.Record(ctx, h.Mean())
					}
					if g := getOrCreateGauge(s.meterMap, name+".stddev"); g != nil {
						g.Record(ctx, h.StdDev())
					}
					if g := getOrCreateGauge(s.meterMap, name+".median"); g != nil {
						g.Record(ctx, ps[0])
					}
					if g := getOrCreateGauge(s.meterMap, name+".p75"); g != nil {
						g.Record(ctx, ps[1])
					}
					if g := getOrCreateGauge(s.meterMap, name+".p90"); g != nil {
						g.Record(ctx, ps[2])
					}
					if g := getOrCreateGauge(s.meterMap, name+".p95"); g != nil {
						g.Record(ctx, ps[3])
					}
					if g := getOrCreateGauge(s.meterMap, name+".p99"); g != nil {
						g.Record(ctx, ps[4])
					}
					if g := getOrCreateGauge(s.meterMap, name+".p99_99"); g != nil {
						g.Record(ctx, ps[5])
					}
				case metrics.Meter:
					m := metric.Snapshot()
					if g := getOrCreateGauge(s.meterMap, name+".count"); g != nil {
						g.Record(ctx, float64(m.Count()))
					}
					if g := getOrCreateGauge(s.meterMap, name+".rate.1min"); g != nil {
						g.Record(ctx, m.Rate1())
					}
					if g := getOrCreateGauge(s.meterMap, name+".rate.5min"); g != nil {
						g.Record(ctx, m.Rate5())
					}
					if g := getOrCreateGauge(s.meterMap, name+".rate.15min"); g != nil {
						g.Record(ctx, m.Rate15())
					}
					if g := getOrCreateGauge(s.meterMap, name+".rate.mean"); g != nil {
						g.Record(ctx, m.RateMean())
					}
				case metrics.Timer:
					t := metric.Snapshot()
					ps := t.Percentiles([]float64{0.5, 0.75, 0.90, 0.95, 0.99, 0.9999})
					if g := getOrCreateGauge(s.meterMap, name+".count"); g != nil {
						g.Record(ctx, float64(t.Count()))
					}
					if g := getOrCreateGauge(s.meterMap, name+".min"); g != nil {
						g.Record(ctx, float64(t.Min()))
					}
					if g := getOrCreateGauge(s.meterMap, name+".max"); g != nil {
						g.Record(ctx, float64(t.Max()))
					}
					if g := getOrCreateGauge(s.meterMap, name+".mean"); g != nil {
						g.Record(ctx, t.Mean())
					}
					if g := getOrCreateGauge(s.meterMap, name+".stddev"); g != nil {
						g.Record(ctx, t.StdDev())
					}
					if g := getOrCreateGauge(s.meterMap, name+".median"); g != nil {
						g.Record(ctx, ps[0])
					}
					if g := getOrCreateGauge(s.meterMap, name+".p75"); g != nil {
						g.Record(ctx, ps[1])
					}
					if g := getOrCreateGauge(s.meterMap, name+".p90"); g != nil {
						g.Record(ctx, ps[2])
					}
					if g := getOrCreateGauge(s.meterMap, name+".p95"); g != nil {
						g.Record(ctx, ps[3])
					}
					if g := getOrCreateGauge(s.meterMap, name+".p99"); g != nil {
						g.Record(ctx, ps[4])
					}
					if g := getOrCreateGauge(s.meterMap, name+".p99_99"); g != nil {
						g.Record(ctx, ps[5])
					}
					if g := getOrCreateGauge(s.meterMap, name+".rate.1min"); g != nil {
						g.Record(ctx, t.Rate1())
					}
					if g := getOrCreateGauge(s.meterMap, name+".rate.5min"); g != nil {
						g.Record(ctx, t.Rate5())
					}
					if g := getOrCreateGauge(s.meterMap, name+".rate.15min"); g != nil {
						g.Record(ctx, t.Rate15())
					}
					if g := getOrCreateGauge(s.meterMap, name+".rate.mean"); g != nil {
						g.Record(ctx, t.RateMean())
					}
				}
			})
		}
	}()
}
