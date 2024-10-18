package model

import (
	"encoding/base64"
	"log/slog"
	"reflect"
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
	Key       *ProduceData      `json:"key" expr:"key"`
	Value     *ProduceData      `json:"value" expr:"value" binding:"required"`
	Headers   map[string]string `json:"headers" expr:"headers" binding:"isdefault|gt=0"`
	Timestamp *time.Time        `json:"timestamp" expr:"timestamp"`
}

func (m *ProduceMessage) Size() int {
	total := 0
	if m.Key != nil {
		total += m.Key.Size()
	}
	if m.Value != nil {
		total += m.Value.Size()
	}
	if m.Headers != nil {
		for k, v := range m.Headers {
			total += len(k) + len(v)
		}
	}
	return total
}

type ProduceData struct {
	String           *string           `json:"string" expr:"string" binding:"required_without=Bytes"`
	Bytes            *string           `json:"bytes" expr:"bytes" binding:"required_without=String"`
	SchemaRecordName *string           `json:"schemaRecordName" expr:"schemaRecordName"`
	SchemaId         *int              `json:"schemaId" expr:"schemaId" validate:"excluded_with=SchemaMetadata"`
	SchemaMetadata   map[string]string `json:"schemaMetadata" expr:"schemaMetadata" validate:"excluded_with=SchemaId"`
}

func (d *ProduceData) Size() int {
	total := 0
	if d.String != nil {
		total += len(*d.String)
	}
	if d.Bytes != nil {
		total += len(*d.Bytes)
	}
	if d.SchemaRecordName != nil {
		total += len(*d.SchemaRecordName)
	}
	if d.SchemaId != nil {
		total += int(reflect.TypeOf(*d.SchemaId).Size())
	}
	if d.SchemaMetadata != nil {
		for k, v := range d.SchemaMetadata {
			total += len(k) + len(v)
		}
	}
	return total
}

func (d *ProduceData) GetBytes() []byte {
	if d.Bytes != nil {
		bytes, err := base64.StdEncoding.DecodeString(*d.Bytes)
		if err != nil {
			slog.Error("failed to decode bytes field in produce request", "error", err)
			return nil
		}
		return bytes
	}
	return nil
}

type ProduceResponse struct {
	Results []ProduceResult `json:"results"`
}

type ProduceResult struct {
	Success bool `json:"success"`
	Pos     int  `json:"-"`
}

type ProduceErrorResponse struct {
	Error string `json:"error"`
}
