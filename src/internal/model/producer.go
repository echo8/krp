package model

import "github.com/echo8/krp/internal/config"

type MessageBatch struct {
	Messages []TopicAndMessage
	Src      *config.Endpoint
}

type TopicAndMessage struct {
	Topic   string
	Message *ProduceMessage
	Pos     int
}
