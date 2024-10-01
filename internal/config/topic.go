package config

import (
	"errors"
	"fmt"
)

type Topic string
type TopicList []Topic

var errTopicWrongType = errors.New("topic field is the wrong type (expected string or list of string)")

func parseTopic(rawMap map[string]any) (any, error) {
	rawTopic, ok := rawMap["topic"]
	if !ok {
		return nil, errors.New("topic field is missing")
	}
	switch topic := rawTopic.(type) {
	case string:
		// single topic
		return Topic(topic), nil
	case []any:
		// list of topics
		topics := make([]Topic, 0, len(topic))
		for _, t := range topic {
			str, ok := t.(string)
			if !ok {
				return nil, errTopicWrongType
			}
			topics = append(topics, Topic(str))
		}
		return TopicList(topics), nil
	default:
		fmt.Printf("topic type: %T", rawTopic)
		return nil, errTopicWrongType
	}
}
