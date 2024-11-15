package franz

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/echo8/krp/internal/config"
	"github.com/echo8/krp/internal/config/franz"
	"github.com/echo8/krp/internal/metric"
	pmodel "github.com/echo8/krp/internal/model"
	"github.com/echo8/krp/internal/producer"
	"github.com/echo8/krp/internal/serializer"
	"github.com/echo8/krp/model"
	"github.com/twmb/franz-go/pkg/kgo"
)

type franzClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Close()
}

func NewProducer(cfg *franz.ProducerConfig, metrics metric.Service,
	keySerializer serializer.Serializer, valueSerializer serializer.Serializer) (producer.Producer, error) {
	opts, err := cfg.ClientConfig.ToOpts()
	if err != nil {
		return nil, err
	}
	if metrics.Config().Enable.Producer {
		opts = append(opts, kgo.WithHooks(metrics.GetFranzHooks()...))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return newProducer(cfg, client, metrics, keySerializer, valueSerializer), nil
}

func newProducer(cfg *franz.ProducerConfig, client franzClient, metrics metric.Service,
	keySerializer serializer.Serializer, valueSerializer serializer.Serializer) producer.Producer {
	producer := &kafkaProducer{
		cfg:             cfg,
		client:          client,
		metrics:         metrics,
		keySerializer:   keySerializer,
		valueSerializer: valueSerializer,
	}
	return producer
}

type kafkaProducer struct {
	cfg             *franz.ProducerConfig
	client          franzClient
	metrics         metric.Service
	keySerializer   serializer.Serializer
	valueSerializer serializer.Serializer
}

type ctxPosKey struct{}
type ctxSrcKey struct{}

func (k *kafkaProducer) asyncPromise(r *kgo.Record, err error) {
	ctx := context.Background()
	src := r.Context.Value(ctxSrcKey{}).(*config.Endpoint)
	if err != nil {
		slog.Error("Kafka delivery failure.", "error", err.Error())
		k.metrics.RecordEndpointMessage(ctx, false, src)
	}
	k.metrics.RecordEndpointMessage(ctx, true, src)
	k.metrics.RecordFranzBufferedDuration(ctx, r.Timestamp)
}

func (k *kafkaProducer) SendAsync(ctx context.Context, batch *pmodel.MessageBatch) error {
	bgCtx := context.Background()
	for i := range batch.Messages {
		record, err := k.franzRecord(bgCtx, &batch.Messages[i], batch.Src)
		if err != nil {
			return err
		}
		k.client.Produce(bgCtx, record, k.asyncPromise)
	}
	return nil
}

func (k *kafkaProducer) SendSync(ctx context.Context, batch *pmodel.MessageBatch) ([]model.ProduceResult, error) {
	records := make([]*kgo.Record, 0, len(batch.Messages))
	for i := range batch.Messages {
		record, err := k.franzRecord(ctx, &batch.Messages[i], batch.Src)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	results := k.client.ProduceSync(ctx, records...)
	produceResults := make([]model.ProduceResult, 0, len(batch.Messages))
	for i := range results {
		success := results[i].Err == nil
		if !success {
			slog.Error("Kafka delivery failure.", "error", results[i].Err.Error())
		}
		k.metrics.RecordEndpointMessage(ctx, success, batch.Src)
		k.metrics.RecordFranzBufferedDuration(ctx, results[i].Record.Timestamp)
		produceResults = append(produceResults, model.ProduceResult{
			Success: success,
			Pos:     results[i].Record.Context.Value(ctxPosKey{}).(int),
		})
	}
	return produceResults, nil
}

func (s *kafkaProducer) franzRecord(ctx context.Context, m *pmodel.TopicAndMessage, src *config.Endpoint) (*kgo.Record, error) {
	record := &kgo.Record{Topic: m.Topic}
	if m.Message.Key != nil {
		keyBytes, err := s.keySerializer.Serialize(m.Topic, m.Message)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize key: %w", err)
		}
		record.Key = keyBytes
	}
	if m.Message.Value != nil {
		valueBytes, err := s.valueSerializer.Serialize(m.Topic, m.Message)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize value: %w", err)
		}
		record.Value = valueBytes
	}
	if len(m.Message.Headers) > 0 {
		headers := make([]kgo.RecordHeader, len(m.Message.Headers))
		j := 0
		for k, v := range m.Message.Headers {
			headers[j] = kgo.RecordHeader{Key: k, Value: []byte(v)}
			j += 1
		}
		record.Headers = headers
	}
	if m.Message.Timestamp != nil {
		record.Timestamp = *m.Message.Timestamp
	}
	record.Context = context.WithValue(ctx, ctxPosKey{}, m.Pos)
	record.Context = context.WithValue(record.Context, ctxSrcKey{}, src)
	return record, nil
}

func (k *kafkaProducer) Close() error {
	k.client.Close()
	return nil
}
