package config

import (
	"errors"
	"fmt"

	"github.com/echo8/krp/internal/config/confluent"
	"github.com/echo8/krp/internal/config/sarama"
	"github.com/echo8/krp/internal/config/schemaregistry"
	"github.com/echo8/krp/internal/config/segment"
	"github.com/echo8/krp/internal/util"
)

type ProducerId string

func (p ProducerId) HasTemplate() bool {
	return util.HasMsgVar(string(p))
}

type ProducerConfig interface {
	Load(v any) error
	SchemaRegistryCfg() *schemaregistry.Config
}

type ProducerConfigs map[ProducerId]ProducerConfig

func (c *ProducerConfigs) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var rawMap map[string]interface{}
	if err := unmarshal(&rawMap); err != nil {
		return err
	}
	*c = ProducerConfigs{}
	for k, v := range rawMap {
		if !util.NotBlankStr(k) {
			return fmt.Errorf("invalid config, producer ids cannot be blank")
		}
		switch v := v.(type) {
		case map[string]interface{}:
			var cfg ProducerConfig
			typ, ok := v["type"]
			if ok {
				switch typ {
				case "confluent":
					cfg = &confluent.ProducerConfig{}
				case "sarama":
					cfg = &sarama.ProducerConfig{}
				case "segment":
					cfg = &segment.ProducerConfig{}
				default:
					return fmt.Errorf("invalid config, unknown producer type: %v", typ)
				}
			} else {
				// default to confluent client
				cfg = &confluent.ProducerConfig{}
			}
			if err := cfg.Load(v); err != nil {
				return fmt.Errorf("invalid config, failed to load %v producer config: %w", typ, err)
			}
			(*c)[ProducerId(k)] = cfg
		default:
			return fmt.Errorf("invalid config, invalid producer config: %v", k)
		}
	}
	return nil
}

type ProducerIdList []ProducerId

var errProducerWrongType = errors.New("producer field is the wrong type (expected string or list of string)")

func parseProducer(rawMap map[string]any) (any, error) {
	rawProducer := rawMap["producer"]
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
	case nil:
		return nil, nil
	default:
		return nil, errProducerWrongType
	}
}
