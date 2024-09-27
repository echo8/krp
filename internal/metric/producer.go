package metric

import (
	gometrics "github.com/rcrowley/go-metrics"
	segment "github.com/segmentio/kafka-go"
)

func (s *service) RecordRdkMetrics(statsJson string, rdkLen, asyncLen int) {
	s.meters.rdk.Record(statsJson, rdkLen, asyncLen)
}

func (s *service) RecordSaramMetrics(registry gometrics.Registry) {

}

func (s *service) RecordSegmentMetrics(stats segment.WriterStats) {

}
