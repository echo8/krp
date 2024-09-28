package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/facebookgo/grace/gracehttp"
	"github.com/gin-gonic/gin"

	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/metric"
	"koko/kafka-rest-producer/internal/model"
	"koko/kafka-rest-producer/internal/producer"
)

type Server interface {
	Run() error
	ServeHTTP(w http.ResponseWriter, req *http.Request)
}

type server struct {
	cfg     *config.ServerConfig
	ps      producer.Service
	metrics metric.Service

	engine *gin.Engine
	srv    *http.Server
}

func NewServer(cfg *config.ServerConfig, ps producer.Service, ms metric.Service) Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	if cfg.Metrics.Enable.Http {
		engine.Use(metric.GinMiddleware())
	}
	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: engine,
	}
	s := server{cfg: cfg, ps: ps, metrics: ms, engine: engine, srv: srv}
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
			handler := s.newProduceHandler(&cfg, s.ps.GetProducer(cfg.Producer))
			s.engine.POST(path, handler)
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
		ctx := c.Request.Context()
		s.metrics.RecordEndpointSizes(ctx, req, cfg.Endpoint)
		if producer.Async() {
			if err := producer.SendAsync(ctx, messageBatch(cfg.Topic, req.Messages, cfg.Endpoint)); err != nil {
				handleProducerError(err, c)
			} else {
				c.Status(http.StatusNoContent)
			}
		} else {
			if res, err := producer.SendSync(ctx, messageBatch(cfg.Topic, req.Messages, cfg.Endpoint)); err != nil {
				handleProducerError(err, c)
			} else {
				resp := model.ProduceResponse{Results: res}
				c.JSON(http.StatusOK, &resp)
			}
		}
	}
}

func messageBatch(topic string, messages []model.ProduceMessage, src *config.Endpoint) *model.MessageBatch {
	mts := make([]model.TopicAndMessage, len(messages))
	for i := range messages {
		mts[i] = model.TopicAndMessage{Topic: topic, Message: &messages[i]}
	}
	return &model.MessageBatch{Messages: mts, Src: src}
}

func handleProducerError(err error, c *gin.Context) {
	if !errors.Is(err, context.Canceled) {
		slog.Error("Producer request failed.", "error", err)
		c.Status(http.StatusInternalServerError)
	} else {
		c.Status(499)
	}
}

func (s *server) Run() error {
	return gracehttp.Serve(s.srv)
}

func (s *server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.engine.ServeHTTP(w, req)
}
