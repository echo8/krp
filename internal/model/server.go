package model

import (
	"time"
)

type ProduceRequest struct {
	Messages []ProduceMessage `json:"messages" binding:"required,gt=0,dive"`
}

func (r *ProduceRequest) Size() int {
	total := 0
	for i := range r.Messages {
		total += r.Messages[i].Size()
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
	if m.Key != nil {
		total += len(*m.Key)
	}
	if m.Value != nil {
		total += len(*m.Value)
	}
	if m.Headers != nil {
		for i := range m.Headers {
			total += m.Headers[i].Size()
		}
	}
	return total
}

type ProduceHeader struct {
	Key   *string `json:"key" binding:"required"`
	Value *string `json:"value" binding:"required"`
}

func (h *ProduceHeader) Size() int {
	total := 0
	if h.Key != nil {
		total += len(*h.Key)
	}
	if h.Value != nil {
		total += len(*h.Value)
	}
	return total
}

type ProduceResponse struct {
	Results []ProduceResult `json:"results"`
}

type ProduceResult struct {
	Partition *int32  `json:"partition,omitempty"`
	Offset    *int64  `json:"offset,omitempty"`
	Error     *string `json:"error,omitempty"`
}
