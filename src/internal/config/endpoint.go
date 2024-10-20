package config

import (
	"fmt"
)

type EndpointConfig struct {
	Endpoint *Endpoint
	Async    bool
	Routes   []*RouteConfig
}

func (c EndpointConfig) NeedsRouter() bool {
	if len(c.Routes) > 1 {
		return true
	}
	route := c.Routes[0]
	if len(route.Match) > 0 {
		return true
	}
	switch topic := route.Topic.(type) {
	case Topic:
		return topic.HasTemplate()
	case TopicList:
		return true
	}
	switch pid := route.Producer.(type) {
	case ProducerId:
		return pid.HasTemplate()
	case ProducerIdList:
		return true
	}
	return false
}

type Endpoint struct {
	Path EndpointPath
}

type EndpointPath string
type EndpointConfigs map[EndpointPath]EndpointConfig

func (c *EndpointConfigs) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var cfgs EndpointConfigs
	type plain EndpointConfigs
	if err := unmarshal((*plain)(&cfgs)); err != nil {
		return err
	}
	for path, cfg := range cfgs {
		cfg.Endpoint = &Endpoint{Path: path}
		cfgs[path] = cfg
	}
	*c = cfgs
	return nil
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
	rawMatch, ok := rawMap["match"]
	if ok {
		switch match := rawMatch.(type) {
		case string:
			c.Match = match
		}
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

func (c *RouteConfig) HasMultipleTopics() bool {
	switch c.Topic.(type) {
	case TopicList:
		return true
	}
	return false
}

func (c *RouteConfig) HasMultipleProducers() bool {
	switch c.Producer.(type) {
	case ProducerIdList:
		return true
	}
	return false
}

func (c *RouteConfig) Topics() []Topic {
	topics := make([]Topic, 0)
	switch topic := c.Topic.(type) {
	case Topic:
		topics = append(topics, topic)
	case TopicList:
		topics = append(topics, topic...)
	}
	return topics
}

func (c *RouteConfig) Producers() []ProducerId {
	pids := make([]ProducerId, 0)
	switch pid := c.Producer.(type) {
	case ProducerId:
		pids = append(pids, pid)
	case ProducerIdList:
		pids = append(pids, pid...)
	}
	return pids
}
