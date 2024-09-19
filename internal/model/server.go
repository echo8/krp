package model

import (
	"time"
)

type ProduceRequest struct {
	Messages []ProduceMessage `json:"messages" binding:"required,gt=0,dive"`
}

func (r *ProduceRequest) Size() int {
	total := 0
	for _, msg := range r.Messages {
		total += msg.Size()
	}
	return total
}

type ProduceMessage struct {
	Key       *string         `json:"key"`
	Value     *string         `json:"value" binding:"required"`
	Headers   []ProduceHeader `json:"headers" binding:"isdefault|gt=0,dive"`
	Timestamp *time.Time      `json:"timestamp"`
}

func (m *ProduceMessage) Size() int {
	total := 0
	total += len(*m.Key) + len(*m.Value)
	for _, h := range m.Headers {
		total += h.Size()
	}
	return total
}

type ProduceHeader struct {
	Key   *string `json:"key" binding:"required"`
	Value *string `json:"value" binding:"required"`
}

func (h *ProduceHeader) Size() int {
	return len(*h.Key) + len(*h.Value)
}

type ProduceResponse struct {
	Results []ProduceResult `json:"results"`
}

type ProduceResult struct {
	Partition *int32  `json:"partition,omitempty"`
	Offset    *int64  `json:"offset,omitempty"`
	Error     *string `json:"error,omitempty"`
}
