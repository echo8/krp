package sarama

import (
	"context"
	"log/slog"

	gometrics "github.com/rcrowley/go-metrics"
	"go.opentelemetry.io/otel"
	otm "go.opentelemetry.io/otel/metric"
)

type Meters struct {
	meterMap map[string]otm.Float64Gauge
}

func NewMeters() *Meters {
	sm := &Meters{meterMap: make(map[string]otm.Float64Gauge)}
	return sm
}

func (m *Meters) getOrCreateGauge(name string) (otm.Float64Gauge, error) {
	g, ok := m.meterMap[name]
	if ok {
		return g, nil
	} else {
		meter := otel.Meter("koko/kafka-rest-producer")
		return meter.Float64Gauge("sarama." + name)
	}
}

func (m *Meters) recordValue(ctx context.Context, name string, val float64) {
	g, err := m.getOrCreateGauge(name)
	if err != nil {
		slog.Error("Failed to create gauge meter.", "error", err.Error())
	} else {
		g.Record(ctx, val)
	}
}

func (m *Meters) Record(registry gometrics.Registry) {
	ctx := context.Background()
	registry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case gometrics.Counter:
			m.recordValue(ctx, name, float64(metric.Count()))
		case gometrics.Gauge:
			m.recordValue(ctx, name, float64(metric.Value()))
		case gometrics.GaugeFloat64:
			m.recordValue(ctx, name, float64(metric.Value()))
		case gometrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.90, 0.95, 0.99, 0.9999})
			m.recordValue(ctx, name+".count", float64(h.Count()))
			m.recordValue(ctx, name+".min", float64(h.Min()))
			m.recordValue(ctx, name+".max", float64(h.Max()))
			m.recordValue(ctx, name+".mean", float64(h.Mean()))
			m.recordValue(ctx, name+".stddev", float64(h.StdDev()))
			m.recordValue(ctx, name+".median", ps[0])
			m.recordValue(ctx, name+".p75", ps[1])
			m.recordValue(ctx, name+".p90", ps[2])
			m.recordValue(ctx, name+".p95", ps[3])
			m.recordValue(ctx, name+".p99", ps[4])
			m.recordValue(ctx, name+".p99_99", ps[5])
		case gometrics.Meter:
			mt := metric.Snapshot()
			m.recordValue(ctx, name+".count", float64(mt.Count()))
			m.recordValue(ctx, name+".rate.1min", float64(mt.Rate1()))
			m.recordValue(ctx, name+".rate.5min", float64(mt.Rate5()))
			m.recordValue(ctx, name+".rate.15min", float64(mt.Rate15()))
			m.recordValue(ctx, name+".rate.mean", float64(mt.RateMean()))
		case gometrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.90, 0.95, 0.99, 0.9999})
			m.recordValue(ctx, name+".count", float64(t.Count()))
			m.recordValue(ctx, name+".min", float64(t.Min()))
			m.recordValue(ctx, name+".max", float64(t.Max()))
			m.recordValue(ctx, name+".mean", float64(t.Mean()))
			m.recordValue(ctx, name+".stddev", float64(t.StdDev()))
			m.recordValue(ctx, name+".median", ps[0])
			m.recordValue(ctx, name+".p75", ps[1])
			m.recordValue(ctx, name+".p90", ps[2])
			m.recordValue(ctx, name+".p95", ps[3])
			m.recordValue(ctx, name+".p99", ps[4])
			m.recordValue(ctx, name+".p99_99", ps[5])
			m.recordValue(ctx, name+".count", float64(t.Count()))
			m.recordValue(ctx, name+".rate.1min", float64(t.Rate1()))
			m.recordValue(ctx, name+".rate.5min", float64(t.Rate5()))
			m.recordValue(ctx, name+".rate.15min", float64(t.Rate15()))
			m.recordValue(ctx, name+".rate.mean", float64(t.RateMean()))
		}
	})
}
