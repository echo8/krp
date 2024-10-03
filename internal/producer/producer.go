package producer

import (
	"context"
	"echo8/kafka-rest-producer/internal/model"
)

type Producer interface {
	SendAsync(ctx context.Context, batch *model.MessageBatch) error
	SendSync(ctx context.Context, batch *model.MessageBatch) ([]model.ProduceResult, error)
	Close() error
}

type TestProducer struct {
	Batch   model.MessageBatch
	Result  []model.ProduceResult
	Error   error
}

func (k *TestProducer) SendAsync(ctx context.Context, batch *model.MessageBatch) error {
	k.Batch = *batch
	if k.Error != nil {
		return k.Error
	} else {
		return nil
	}
}

func (k *TestProducer) SendSync(ctx context.Context, batch *model.MessageBatch) ([]model.ProduceResult, error) {
	k.Batch = *batch
	if k.Result != nil {
		return k.Result, nil
	} else if k.Error != nil {
		return nil, k.Error
	} else {
		return []model.ProduceResult{}, nil
	}
}

func (k *TestProducer) Close() error {
	return nil
}
