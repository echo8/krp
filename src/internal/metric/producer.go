package metric

import (
	"context"
	"time"

	gometrics "github.com/rcrowley/go-metrics"
	segment "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *service) RecordConfluentMetrics(statsJson string, confluentLen, asyncLen int) {
	s.meters.confluent.Record(statsJson, confluentLen, asyncLen)
}

func (s *service) RecordSaramaMetrics(registry gometrics.Registry) {
	s.meters.sarama.Record(registry)
}

func (s *service) RecordSegmentMetrics(stats segment.WriterStats) {
	s.meters.segment.Record(stats)
}

func (s *service) GetFranzHooks() []kgo.Hook {
	return s.meters.franz.GetHooks()
}

func (s *service) RecordFranzBufferedDuration(ctx context.Context, timestamp time.Time) {
	if s.cfg.Enable.Producer {
		s.meters.franz.RecordBufferedDuration(ctx, timestamp)
	}
}
