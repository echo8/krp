package model

import (
	"time"
)

type ProduceRequest struct {
	Messages []ProduceMessage `json:"messages" binding:"required,gt=0,dive"`
}

type ProduceMessage struct {
	Key       *string         `json:"key"`
	Value     *string         `json:"value" binding:"required"`
	Headers   []ProduceHeader `json:"headers" binding:"isdefault|gt=0,dive"`
	Timestamp *time.Time      `json:"timestamp"`
}

type ProduceHeader struct {
	Key   *string `json:"key" binding:"required"`
	Value *string `json:"value" binding:"required"`
}

type ProduceResponse struct {
	Results []ProduceResult `json:"results"`
}

type ProduceResult struct {
	Partition *int32  `json:"partition,omitempty"`
	Offset    *int64  `json:"offset,omitempty"`
	Error     *string `json:"error,omitempty"`
}
