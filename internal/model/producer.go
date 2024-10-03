package model

import "echo8/kafka-rest-producer/internal/config"

type MessageBatch struct {
	Messages []TopicAndMessage
	Src      *config.Endpoint
}

type TopicAndMessage struct {
	Topic   string
	Message *ProduceMessage
	Pos     int
}
