package franz

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/echo8/krp/internal/util"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/attribute"
	otm "go.opentelemetry.io/otel/metric"
)

func NewMeters() (*Meters, error) {
	kotelService := kotel.NewKotel(kotel.WithMeter(kotel.NewMeter()))
	moreMeters := &moreMeters{}
	if err := util.CreateMeters(moreMeters); err != nil {
		return nil, err
	}
	return &Meters{
		kotelService: kotelService,
		moreMeters:   moreMeters,
	}, nil
}

type Meters struct {
	kotelService *kotel.Kotel
	moreMeters   *moreMeters
}

func (m *Meters) GetHooks() []kgo.Hook {
	hooks := m.kotelService.Hooks()
	hooks = append(hooks, m.moreMeters)
	return hooks
}

func (m *Meters) RecordBufferedDuration(ctx context.Context, timestamp time.Time) {
	duration := float64(time.Since(timestamp)) / float64(time.Millisecond)
	m.moreMeters.BufferedDuration.Record(ctx, duration)
}

type moreMeters struct {
	RequestLatency   otm.Float64Histogram `name:"messaging.kafka.request.latency" description:"Request latency, by broker" unit:"ms"`
	BufferedDuration otm.Float64Histogram `name:"messaging.kafka.produce_buffered.duration" description:"Duration of time between receiving and delivering a record" unit:"ms"`
}

func (m *moreMeters) OnBrokerE2E(meta kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {
	node := strnode(meta.NodeID)
	attributes := attribute.NewSet(attribute.String("node_id", node))
	m.RequestLatency.Record(
		context.Background(),
		float64(e2e.DurationE2E()) / float64(time.Millisecond),
		otm.WithAttributeSet(attributes),
	)
}

func strnode(node int32) string {
	if node < 0 {
		return "seed_" + strconv.Itoa(int(node)-math.MinInt32)
	}
	return strconv.Itoa(int(node))
}
