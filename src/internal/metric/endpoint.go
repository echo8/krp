package metric

import (
	"context"

	"github.com/echo8/krp/internal/config"
	"github.com/echo8/krp/internal/util"
	"github.com/echo8/krp/model"

	"go.opentelemetry.io/otel/attribute"
	otm "go.opentelemetry.io/otel/metric"
)

func newEndpointMeters() (*endpointMeters, error) {
	em := &endpointMeters{}
	if err := util.CreateMeters(em); err != nil {
		return nil, err
	}
	return em, nil
}

type endpointMeters struct {
	RequestSize    otm.Int64Histogram `name:"krp.endpoint.request.size" description:"Measures the size of request objects attributed by endpoint path." unit:"By"`
	MessageSize    otm.Int64Histogram `name:"krp.endpoint.message.size" description:"Measures the size of message objects attributed by endpoint path." unit:"By"`
	MessageCount   otm.Int64Counter   `name:"krp.endpoint.message.produced" description:"Count of messages produced to Kafka attributed by endpoint path and success result." unit:"{message}"`
	UnmatchedCount otm.Int64Counter   `name:"krp.endpoint.message.unmatched" description:"Count of messages that were received but did not match any of an endpoint's routes attributed by endpoint path." unit:"{message}"`
}

func (s *service) RecordEndpointSizes(ctx context.Context, req model.ProduceRequest, src *config.Endpoint) {
	if s.cfg.Enable.Endpoint {
		attributes := endpointAttributes(src)
		s.meters.endpoint.RequestSize.Record(ctx, int64(req.Size()), attributes)
		for i := range req.Messages {
			s.meters.endpoint.MessageSize.Record(ctx, int64(req.Messages[i].Size()), attributes)
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

func (s *service) RecordEndpointUnmatched(ctx context.Context, count int, src *config.Endpoint) {
	if s.cfg.Enable.Endpoint {
		s.meters.endpoint.UnmatchedCount.Add(ctx, 1, endpointAttributes(src))
	}
}

func endpointAttributes(src *config.Endpoint) otm.MeasurementOption {
	return otm.WithAttributeSet(
		attribute.NewSet(
			attribute.String("endpoint_path", string(src.Path))))
}
