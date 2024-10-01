package config

import (
	"fmt"
)

type EndpointConfigV2 struct {
	Routes []RouteConfig
}

type RouteConfig struct {
	Match    string
	Topic    any
	Producer any
}

func (c *RouteConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var rawMap map[string]any
	if err := unmarshal(&rawMap); err != nil {
		return err
	}
	topic, err := parseTopic(rawMap)
	if err != nil {
		return fmt.Errorf("failed to parse topic from %v: %w", rawMap, err)
	}
	c.Topic = topic
	producer, err := parseProducer(rawMap)
	if err != nil {
		return fmt.Errorf("failed to parse producer from %v: %w", rawMap, err)
	}
	c.Producer = producer
	return nil
}
