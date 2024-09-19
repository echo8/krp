package metric

import (
	"context"
	"log/slog"

	gometrics "github.com/rcrowley/go-metrics"
	"go.opentelemetry.io/otel"
	otm "go.opentelemetry.io/otel/metric"
)

type saramaMeters struct {
	meterMap map[string]otm.Float64Gauge
}

func newSaramaMeters() *saramaMeters {
	sm := &saramaMeters{meterMap: make(map[string]otm.Float64Gauge)}
	return sm
}

func (s *saramaMeters) getOrCreateGauge(name string) (otm.Float64Gauge, error) {
	g, ok := s.meterMap[name]
	if ok {
		return g, nil
	} else {
		meter := otel.Meter("koko/kafka-rest-producer")
		return meter.Float64Gauge("sarama." + name)
	}
}

func (s *saramaMeters) record(ctx context.Context, name string, val float64) {
	g, err := s.getOrCreateGauge(name)
	if err != nil {
		slog.Error("Failed to create gauge meter.", "error", err.Error())
	} else {
		g.Record(ctx, val)
	}
}

func (s *service) RecordSaramMetrics(registry gometrics.Registry) {
	ctx := context.Background()
	registry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case gometrics.Counter:
			s.meters.sarama.record(ctx, name, float64(metric.Count()))
		case gometrics.Gauge:
			s.meters.sarama.record(ctx, name, float64(metric.Value()))
		case gometrics.GaugeFloat64:
			s.meters.sarama.record(ctx, name, float64(metric.Value()))
		case gometrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.90, 0.95, 0.99, 0.9999})
			s.meters.sarama.record(ctx, name+".count", float64(h.Count()))
			s.meters.sarama.record(ctx, name+".min", float64(h.Min()))
			s.meters.sarama.record(ctx, name+".max", float64(h.Max()))
			s.meters.sarama.record(ctx, name+".mean", float64(h.Mean()))
			s.meters.sarama.record(ctx, name+".stddev", float64(h.StdDev()))
			s.meters.sarama.record(ctx, name+".median", ps[0])
			s.meters.sarama.record(ctx, name+".p75", ps[1])
			s.meters.sarama.record(ctx, name+".p90", ps[2])
			s.meters.sarama.record(ctx, name+".p95", ps[3])
			s.meters.sarama.record(ctx, name+".p99", ps[4])
			s.meters.sarama.record(ctx, name+".p99_99", ps[5])
		case gometrics.Meter:
			m := metric.Snapshot()
			s.meters.sarama.record(ctx, name+".count", float64(m.Count()))
			s.meters.sarama.record(ctx, name+".rate.1min", float64(m.Rate1()))
			s.meters.sarama.record(ctx, name+".rate.5min", float64(m.Rate5()))
			s.meters.sarama.record(ctx, name+".rate.15min", float64(m.Rate15()))
			s.meters.sarama.record(ctx, name+".rate.mean", float64(m.RateMean()))
		case gometrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.90, 0.95, 0.99, 0.9999})
			s.meters.sarama.record(ctx, name+".count", float64(t.Count()))
			s.meters.sarama.record(ctx, name+".min", float64(t.Min()))
			s.meters.sarama.record(ctx, name+".max", float64(t.Max()))
			s.meters.sarama.record(ctx, name+".mean", float64(t.Mean()))
			s.meters.sarama.record(ctx, name+".stddev", float64(t.StdDev()))
			s.meters.sarama.record(ctx, name+".median", ps[0])
			s.meters.sarama.record(ctx, name+".p75", ps[1])
			s.meters.sarama.record(ctx, name+".p90", ps[2])
			s.meters.sarama.record(ctx, name+".p95", ps[3])
			s.meters.sarama.record(ctx, name+".p99", ps[4])
			s.meters.sarama.record(ctx, name+".p99_99", ps[5])
			s.meters.sarama.record(ctx, name+".count", float64(t.Count()))
			s.meters.sarama.record(ctx, name+".rate.1min", float64(t.Rate1()))
			s.meters.sarama.record(ctx, name+".rate.5min", float64(t.Rate5()))
			s.meters.sarama.record(ctx, name+".rate.15min", float64(t.Rate15()))
			s.meters.sarama.record(ctx, name+".rate.mean", float64(t.RateMean()))
		}
	})
}
