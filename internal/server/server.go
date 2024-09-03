package server

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/facebookgo/grace/gracehttp"
	"github.com/gin-gonic/gin"

	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/model"
	"koko/kafka-rest-producer/internal/producer"
)

type server struct {
	cfg    *config.ServerConfig
	ps     producer.Service
	engine *gin.Engine
	srv    *http.Server
}

func NewServer(cfg *config.ServerConfig, ps producer.Service) *server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: engine,
	}
	s := server{cfg: cfg, ps: ps, engine: engine, srv: srv}
	s.registerRoutes()
	return &s
}

func (s *server) registerRoutes() {
	for ns, cfgs := range s.cfg.Endpoints {
		for eid, cfg := range cfgs {
			var path string
			if ns == config.DefaultNamespace {
				path = fmt.Sprintf("/%v", eid)
			} else {
				path = fmt.Sprintf("/%v/%v", ns, eid)
			}
			s.engine.POST(path, s.newProduceHandler(&cfg, s.ps.GetProducer(cfg.Producer)))
			slog.Info("Added endpoint.", "path", path, "topic", cfg.Topic, "pid", cfg.Producer)
		}
	}
}

func (s *server) newProduceHandler(cfg *config.EndpointConfig, producer producer.Producer) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.ProduceRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		res := producer.Send(s.toTopicMessages(cfg.Topic, req.Messages))
		if res != nil {
			resp := model.ProduceResponse{Results: res}
			c.JSON(http.StatusOK, &resp)
		} else {
			c.Status(http.StatusNoContent)
		}
	}
}

func (s *server) toTopicMessages(topic string, messages []model.ProduceMessage) []producer.TopicAndMessage {
	res := make([]producer.TopicAndMessage, len(messages))
	for i, msg := range messages {
		res[i] = producer.TopicAndMessage{Topic: topic, Message: &msg}
	}
	return res
}

func (s *server) Run() error {
	return gracehttp.Serve(s.srv)
}
