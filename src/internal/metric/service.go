package metric

import (
	"context"

	"github.com/echo8/krp/internal/config"
	"github.com/echo8/krp/internal/metric/confluent"
	"github.com/echo8/krp/internal/metric/sarama"
	segmentmetric "github.com/echo8/krp/internal/metric/segment"
	"github.com/echo8/krp/model"

	gometrics "github.com/rcrowley/go-metrics"
	segment "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Service interface {
	RecordEndpointSizes(ctx context.Context, req model.ProduceRequest, src *config.Endpoint)
	RecordEndpointMessage(ctx context.Context, success bool, src *config.Endpoint)
	RecordEndpointUnmatched(ctx context.Context, count int, src *config.Endpoint)

	RecordConfluentMetrics(statsJson string, confluentLen, asyncLen int)
	RecordSaramMetrics(registry gometrics.Registry)
	RecordSegmentMetrics(stats segment.WriterStats)

	Config() *config.MetricsConfig
}

type service struct {
	cfg    *config.MetricsConfig
	meters *meters
}

type meters struct {
	endpoint  *endpointMeters
	confluent *confluent.Meters
	sarama    *sarama.Meters
	segment   *segmentmetric.Meters
}

func NewService(cfg *config.MetricsConfig) (Service, error) {
	s := &service{cfg: cfg}
	err := s.setup()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *service) setup() error {
	ctx := context.Background()
	if s.cfg.Enabled() {
		tlsCfg, err := s.cfg.Otel.Tls.LoadTLSConfig(ctx)
		if err != nil {
			return err
		}
		cred := insecure.NewCredentials()
		if tlsCfg != nil {
			cred = credentials.NewTLS(tlsCfg)
		}
		conn, err := grpc.NewClient(s.cfg.Otel.Endpoint, grpc.WithTransportCredentials(cred))
		if err != nil {
			return err
		}
		metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
		if err != nil {
			return err
		}
		meterProvider := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(s.cfg.Otel.ExportInterval))),
		)
		otel.SetMeterProvider(meterProvider)
	}

	meters := &meters{}
	var err error
	if s.cfg.Enable.Endpoint {
		if meters.endpoint, err = newEndpointMeters(); err != nil {
			return err
		}
	}
	if s.cfg.Enable.Producer {
		if meters.confluent, err = confluent.NewMeters(); err != nil {
			return err
		}
		meters.sarama = sarama.NewMeters()
		if meters.segment, err = segmentmetric.NewMeters(); err != nil {
			return err
		}
	}
	if s.cfg.Enable.Host {
		if err := host.Start(); err != nil {
			return err
		}
	}
	if s.cfg.Enable.Runtime {
		if err := runtime.Start(); err != nil {
			return err
		}
	}
	s.meters = meters
	return nil
}

func (s *service) Config() *config.MetricsConfig {
	return s.cfg
}
