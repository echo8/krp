package metric

import (
	gometrics "github.com/rcrowley/go-metrics"
	segment "github.com/segmentio/kafka-go"
)

func (s *service) RecordConfluentMetrics(statsJson string, confluentLen, asyncLen int) {
	s.meters.confluent.Record(statsJson, confluentLen, asyncLen)
}

func (s *service) RecordSaramMetrics(registry gometrics.Registry) {
	s.meters.sarama.Record(registry)
}

func (s *service) RecordSegmentMetrics(stats segment.WriterStats) {
	s.meters.segment.Record(stats)
}
