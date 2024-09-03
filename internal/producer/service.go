package producer

import (
	"koko/kafka-rest-producer/internal/config"
	"log/slog"
)

type Service interface {
	GetProducer(id config.ProducerId) Producer
	CloseProducers()
}

type service struct {
	producers map[config.ProducerId]Producer
}

func NewService(newProducers map[config.ProducerId]Producer) (Service, error) {
	return &service{producers: newProducers}, nil
}

func NewServiceFrom(id config.ProducerId, p Producer) (Service, error) {
	producers := make(map[config.ProducerId]Producer, 1)
	producers[id] = p
	return &service{producers: producers}, nil
}

func (s *service) GetProducer(id config.ProducerId) Producer {
	return s.producers[id]
}

func (s *service) CloseProducers() {
	for pid, p := range s.producers {
		if err := p.Close(); err != nil {
			slog.Error("Error occurred when closing producer.", "pid", pid, "error", err.Error())
		} else {
			slog.Info("Producer closed successfully.", "pid", pid)
		}
	}
}
