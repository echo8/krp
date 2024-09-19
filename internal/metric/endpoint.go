package metric

import (
	"context"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/model"

	"go.opentelemetry.io/otel/attribute"
	otm "go.opentelemetry.io/otel/metric"
)

func newEndpointMeters() (*endpointMeters, error) {
	em := &endpointMeters{}
	if err := createMeters(em); err != nil {
		return nil, err
	}
	return em, nil
}

type endpointMeters struct {
	RequestSize  otm.Int64Histogram `name:"krp.endpoint.request.size" description:"hello here" unit:"byt"`
	MessageSize  otm.Int64Histogram `name:"krp.endpoint.message.size" description:"hello here" unit:"byt"`
	MessageCount otm.Int64Counter   `name:"krp.endpoint.message.produced" description:"hello here" unit:"byt"`
}

func (s *service) RecordEndpointSizes(ctx context.Context, req model.ProduceRequest, src *config.Endpoint) {
	if s.cfg.Enable.Endpoint {
		attributes := endpointAttributes(src)
		s.meters.endpoint.RequestSize.Record(ctx, int64(req.Size()), attributes)
		for _, msg := range req.Messages {
			s.meters.endpoint.MessageSize.Record(ctx, int64(msg.Size()), attributes)
		}
	}
}

func (s *service) RecordEndpointMessage(ctx context.Context, success bool, src *config.Endpoint) {
	if s.cfg.Enable.Endpoint {
		successAttribute := attribute.Bool("success", success)
		attributes := otm.WithAttributeSet(attribute.NewSet(successAttribute))
		s.meters.endpoint.MessageCount.Add(ctx, 1, attributes, endpointAttributes(src))
	}
}

func endpointAttributes(src *config.Endpoint) otm.MeasurementOption {
	return otm.WithAttributeSet(
		attribute.NewSet(
			attribute.String("endpoint_ns", src.Namespace),
			attribute.String("endpoint_id", src.Id)))
}
