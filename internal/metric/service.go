package metric

import (
	"context"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/model"

	gometrics "github.com/rcrowley/go-metrics"
	segment "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
)

type Service interface {
	RecordEndpointSizes(ctx context.Context, req model.ProduceRequest, src *config.Endpoint)
	RecordEndpointMessage(ctx context.Context, success bool, src *config.Endpoint)

	RecordRdkMetrics(statsJson string, rdkLen, asyncLen int)
	RecordSaramMetrics(registry gometrics.Registry)
	RecordSegmentMetrics(stats segment.WriterStats)

	Config() *config.MetricsConfig
}

type service struct {
	cfg    *config.MetricsConfig
	meters *meters
}

type meters struct {
	endpoint *endpointMeters
	rdk      *rdkMeters
	sarama   *saramaMeters
	segment  *segmentMeters
}

func NewService(cfg *config.MetricsConfig) (Service, error) {
	meters := &meters{}
	var err error
	if cfg.Enable.Endpoint {
		if meters.endpoint, err = newEndpointMeters(); err != nil {
			return nil, err
		}
	}
	if cfg.Enable.Producer {
		if meters.rdk, err = newRdkMeters(); err != nil {
			return nil, err
		}
		meters.sarama = newSaramaMeters()
		if meters.segment, err = newSegmentMeters(); err != nil {
			return nil, err
		}
	}
	if cfg.Enable.Host {
		if err := host.Start(); err != nil {
			return nil, err
		}
	}
	if cfg.Enable.Runtime {
		if err := runtime.Start(); err != nil {
			return nil, err
		}
	}
	return &service{cfg: cfg, meters: meters}, nil
}

func (s *service) Config() *config.MetricsConfig {
	return s.cfg
}
