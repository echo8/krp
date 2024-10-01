package config

import (
	"echo8/kafka-rest-producer/internal/util"
	"errors"
)

type ProducerId string

func (p ProducerId) HasTemplate() bool {
	return util.HasMsgVar(string(p))
}

type ProducerIdList []ProducerId

var errProducerWrongType = errors.New("producer field is the wrong type (expected string or list of string)")

func parseProducer(rawMap map[string]any) (any, error) {
	rawProducer, ok := rawMap["producer"]
	if !ok {
		return nil, errors.New("producer field is missing")
	}
	switch pid := rawProducer.(type) {
	case string:
		// single producer id
		return ProducerId(pid), nil
	case []any:
		// list of producer ids
		pids := make([]ProducerId, 0, len(pid))
		for _, id := range pid {
			str, ok := id.(string)
			if !ok {
				return nil, errProducerWrongType
			}
			pids = append(pids, ProducerId(str))
		}
		return ProducerIdList(pids), nil
	default:
		return nil, errProducerWrongType
	}
}
