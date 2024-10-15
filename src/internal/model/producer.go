package model

import (
	"github.com/echo8/krp/internal/config"
	server "github.com/echo8/krp/model"
)

type MessageBatch struct {
	Messages []TopicAndMessage
	Src      *config.Endpoint
}

type TopicAndMessage struct {
	Topic   string
	Message *server.ProduceMessage
	Pos     int
}
