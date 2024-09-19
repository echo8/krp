package metric

import (
	"context"

	segment "github.com/segmentio/kafka-go"
	otm "go.opentelemetry.io/otel/metric"
)

func newSegmentMeters() (*segmentMeters, error) {
	sm := &segmentMeters{}
	if err := createMeters(sm); err != nil {
		return nil, err
	}
	return sm, nil
}

type segmentMeters struct {
	Writes   otm.Int64Counter `name:"segment.writer.write.count" description:"" unit:""`
	Messages otm.Int64Counter `name:"segment.writer.message.count" description:"" unit:""`
	Bytes    otm.Int64Counter `name:"segment.writer.message.bytes" description:"" unit:""`
	Errors   otm.Int64Counter `name:"segment.writer.error.count" description:"" unit:""`

	BatchTimeAvg   otm.Float64Gauge   `name:"segment.writer.batch.seconds.avg" description:"" unit:""`
	BatchTimeMin   otm.Float64Gauge   `name:"segment.writer.batch.seconds.min" description:"" unit:""`
	BatchTimeMax   otm.Float64Gauge   `name:"segment.writer.batch.seconds.max" description:"" unit:""`
	BatchTimeCount otm.Int64Counter   `name:"segment.writer.batch.seconds.count" description:"" unit:""`
	BatchTimeSum   otm.Float64Counter `name:"segment.writer.batch.seconds.sum" description:"" unit:""`

	BatchQueueAvg   otm.Float64Gauge   `name:"segment.writer.batch.queue.seconds.avg" description:"" unit:""`
	BatchQueueMin   otm.Float64Gauge   `name:"segment.writer.batch.queue.seconds.min" description:"" unit:""`
	BatchQueueMax   otm.Float64Gauge   `name:"segment.writer.batch.queue.seconds.max" description:"" unit:""`
	BatchQueueCount otm.Int64Counter   `name:"segment.writer.batch.queue.seconds.count" description:"" unit:""`
	BatchQueueSum   otm.Float64Counter `name:"segment.writer.batch.queue.seconds.sum" description:"" unit:""`

	WriteTimeAvg   otm.Float64Gauge   `name:"segment.writer.write.seconds.avg" description:"" unit:""`
	WriteTimeMin   otm.Float64Gauge   `name:"segment.writer.write.seconds.min" description:"" unit:""`
	WriteTimeMax   otm.Float64Gauge   `name:"segment.writer.write.seconds.max" description:"" unit:""`
	WriteTimeCount otm.Int64Counter   `name:"segment.writer.write.seconds.count" description:"" unit:""`
	WriteTimeSum   otm.Float64Counter `name:"segment.writer.write.seconds.sum" description:"" unit:""`

	WaitTimeAvg   otm.Float64Gauge   `name:"segment.writer.wait.seconds.avg" description:"" unit:""`
	WaitTimeMin   otm.Float64Gauge   `name:"segment.writer.wait.seconds.min" description:"" unit:""`
	WaitTimeMax   otm.Float64Gauge   `name:"segment.writer.wait.seconds.max" description:"" unit:""`
	WaitTimeCount otm.Int64Counter   `name:"segment.writer.wait.seconds.count" description:"" unit:""`
	WaitTimeSum   otm.Float64Counter `name:"segment.writer.wait.seconds.sum" description:"" unit:""`

	Retries otm.Int64Counter `name:"segment.writer.retries.count" description:"" unit:""`

	BatchSizeAvg   otm.Int64Gauge   `name:"segment.writer.batch.size.avg" description:"" unit:""`
	BatchSizeMin   otm.Int64Gauge   `name:"segment.writer.batch.size.min" description:"" unit:""`
	BatchSizeMax   otm.Int64Gauge   `name:"segment.writer.batch.size.max" description:"" unit:""`
	BatchSizeCount otm.Int64Counter `name:"segment.writer.batch.size.count" description:"" unit:""`
	BatchSizeSum   otm.Int64Counter `name:"segment.writer.batch.size.sum" description:"" unit:""`

	BatchBytesAvg   otm.Int64Gauge   `name:"segment.writer.batch.bytes.avg" description:"" unit:""`
	BatchBytesMin   otm.Int64Gauge   `name:"segment.writer.batch.bytes.min" description:"" unit:""`
	BatchBytesMax   otm.Int64Gauge   `name:"segment.writer.batch.bytes.max" description:"" unit:""`
	BatchBytesCount otm.Int64Counter `name:"segment.writer.batch.bytes.count" description:"" unit:""`
	BatchBytesSum   otm.Int64Counter `name:"segment.writer.batch.bytes.sum" description:"" unit:""`
}

func (s *service) RecordSegmentMetrics(stats segment.WriterStats) {
	ctx := context.Background()

	s.meters.segment.Writes.Add(ctx, stats.Writes)
	s.meters.segment.Messages.Add(ctx, stats.Messages)
	s.meters.segment.Bytes.Add(ctx, stats.Bytes)
	s.meters.segment.Errors.Add(ctx, stats.Errors)

	s.meters.segment.BatchTimeAvg.Record(ctx, stats.BatchTime.Avg.Seconds())
	s.meters.segment.BatchTimeMin.Record(ctx, stats.BatchTime.Min.Seconds())
	s.meters.segment.BatchTimeMax.Record(ctx, stats.BatchTime.Max.Seconds())
	s.meters.segment.BatchTimeCount.Add(ctx, stats.BatchTime.Count)
	s.meters.segment.BatchTimeSum.Add(ctx, stats.BatchTime.Sum.Seconds())

	s.meters.segment.BatchQueueAvg.Record(ctx, stats.BatchQueueTime.Avg.Seconds())
	s.meters.segment.BatchQueueMin.Record(ctx, stats.BatchQueueTime.Min.Seconds())
	s.meters.segment.BatchQueueMax.Record(ctx, stats.BatchQueueTime.Max.Seconds())
	s.meters.segment.BatchQueueCount.Add(ctx, stats.BatchQueueTime.Count)
	s.meters.segment.BatchQueueSum.Add(ctx, stats.BatchQueueTime.Sum.Seconds())

	s.meters.segment.WriteTimeAvg.Record(ctx, stats.WriteTime.Avg.Seconds())
	s.meters.segment.WriteTimeMin.Record(ctx, stats.WriteTime.Min.Seconds())
	s.meters.segment.WriteTimeMax.Record(ctx, stats.WriteTime.Max.Seconds())
	s.meters.segment.WriteTimeCount.Add(ctx, stats.WriteTime.Count)
	s.meters.segment.WriteTimeSum.Add(ctx, stats.WriteTime.Sum.Seconds())

	s.meters.segment.WaitTimeAvg.Record(ctx, stats.WaitTime.Avg.Seconds())
	s.meters.segment.WaitTimeMin.Record(ctx, stats.WaitTime.Min.Seconds())
	s.meters.segment.WaitTimeMax.Record(ctx, stats.WaitTime.Max.Seconds())
	s.meters.segment.WaitTimeCount.Add(ctx, stats.WaitTime.Count)
	s.meters.segment.WaitTimeSum.Add(ctx, stats.WaitTime.Sum.Seconds())

	s.meters.segment.Retries.Add(ctx, stats.Retries)

	s.meters.segment.BatchSizeAvg.Record(ctx, stats.BatchSize.Avg)
	s.meters.segment.BatchSizeMin.Record(ctx, stats.BatchSize.Min)
	s.meters.segment.BatchSizeMax.Record(ctx, stats.BatchSize.Max)
	s.meters.segment.BatchSizeCount.Add(ctx, stats.BatchSize.Count)
	s.meters.segment.BatchSizeSum.Add(ctx, stats.BatchSize.Sum)

	s.meters.segment.BatchBytesAvg.Record(ctx, stats.BatchBytes.Avg)
	s.meters.segment.BatchBytesMin.Record(ctx, stats.BatchBytes.Min)
	s.meters.segment.BatchBytesMax.Record(ctx, stats.BatchBytes.Max)
	s.meters.segment.BatchBytesCount.Add(ctx, stats.BatchBytes.Count)
	s.meters.segment.BatchBytesSum.Add(ctx, stats.BatchBytes.Sum)
}
