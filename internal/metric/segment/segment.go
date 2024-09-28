package segment

import (
	"context"
	"echo8/kafka-rest-producer/internal/util"

	segment "github.com/segmentio/kafka-go"
	otm "go.opentelemetry.io/otel/metric"
)

func NewMeters() (*Meters, error) {
	sm := &Meters{}
	if err := util.CreateMeters(sm); err != nil {
		return nil, err
	}
	return sm, nil
}

type Meters struct {
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

func (m *Meters) Record(stats segment.WriterStats) {
	ctx := context.Background()

	m.Writes.Add(ctx, stats.Writes)
	m.Messages.Add(ctx, stats.Messages)
	m.Bytes.Add(ctx, stats.Bytes)
	m.Errors.Add(ctx, stats.Errors)

	m.BatchTimeAvg.Record(ctx, stats.BatchTime.Avg.Seconds())
	m.BatchTimeMin.Record(ctx, stats.BatchTime.Min.Seconds())
	m.BatchTimeMax.Record(ctx, stats.BatchTime.Max.Seconds())
	m.BatchTimeCount.Add(ctx, stats.BatchTime.Count)
	m.BatchTimeSum.Add(ctx, stats.BatchTime.Sum.Seconds())

	m.BatchQueueAvg.Record(ctx, stats.BatchQueueTime.Avg.Seconds())
	m.BatchQueueMin.Record(ctx, stats.BatchQueueTime.Min.Seconds())
	m.BatchQueueMax.Record(ctx, stats.BatchQueueTime.Max.Seconds())
	m.BatchQueueCount.Add(ctx, stats.BatchQueueTime.Count)
	m.BatchQueueSum.Add(ctx, stats.BatchQueueTime.Sum.Seconds())

	m.WriteTimeAvg.Record(ctx, stats.WriteTime.Avg.Seconds())
	m.WriteTimeMin.Record(ctx, stats.WriteTime.Min.Seconds())
	m.WriteTimeMax.Record(ctx, stats.WriteTime.Max.Seconds())
	m.WriteTimeCount.Add(ctx, stats.WriteTime.Count)
	m.WriteTimeSum.Add(ctx, stats.WriteTime.Sum.Seconds())

	m.WaitTimeAvg.Record(ctx, stats.WaitTime.Avg.Seconds())
	m.WaitTimeMin.Record(ctx, stats.WaitTime.Min.Seconds())
	m.WaitTimeMax.Record(ctx, stats.WaitTime.Max.Seconds())
	m.WaitTimeCount.Add(ctx, stats.WaitTime.Count)
	m.WaitTimeSum.Add(ctx, stats.WaitTime.Sum.Seconds())

	m.Retries.Add(ctx, stats.Retries)

	m.BatchSizeAvg.Record(ctx, stats.BatchSize.Avg)
	m.BatchSizeMin.Record(ctx, stats.BatchSize.Min)
	m.BatchSizeMax.Record(ctx, stats.BatchSize.Max)
	m.BatchSizeCount.Add(ctx, stats.BatchSize.Count)
	m.BatchSizeSum.Add(ctx, stats.BatchSize.Sum)

	m.BatchBytesAvg.Record(ctx, stats.BatchBytes.Avg)
	m.BatchBytesMin.Record(ctx, stats.BatchBytes.Min)
	m.BatchBytesMax.Record(ctx, stats.BatchBytes.Max)
	m.BatchBytesCount.Add(ctx, stats.BatchBytes.Count)
	m.BatchBytesSum.Add(ctx, stats.BatchBytes.Sum)
}
