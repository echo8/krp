package producer

import (
	"context"
	"echo8/kafka-rest-producer/internal/model"
)

type Producer interface {
	Async() bool
	SendAsync(ctx context.Context, batch *model.MessageBatch) error
	SendSync(ctx context.Context, batch *model.MessageBatch) ([]model.ProduceResult, error)
	Close() error
}
