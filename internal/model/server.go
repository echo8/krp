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
	Key       *string           `json:"key"`
	Value     *string           `json:"value" binding:"required"`
	Headers   map[string]string `json:"headers" binding:"isdefault|gt=0"`
	Timestamp *time.Time        `json:"timestamp"`
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
		for k, v := range m.Headers {
			total += len(k) + len(v)
		}
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
