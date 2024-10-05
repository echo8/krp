package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/facebookgo/grace/gracehttp"
	"github.com/gin-gonic/gin"

	"echo8/kafka-rest-producer/internal/config"
	"echo8/kafka-rest-producer/internal/metric"
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/producer"
	"echo8/kafka-rest-producer/internal/router"
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

func NewServer(cfg *config.ServerConfig, ps producer.Service, ms metric.Service) (Server, error) {
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
	err := s.registerRoutes()
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (s *server) registerRoutes() error {
	for path, cfg := range s.cfg.Endpoints {
		if cfg.NeedsRouter() {
			r, err := router.New(&cfg, s.ps)
			if err != nil {
				return err
			}
			handler := s.newRoutedProduceHandler(&cfg, r)
			s.engine.POST("/"+string(path), handler)
			slog.Info("Added endpoint.", "path", path)
		} else {
			route := cfg.Routes[0]
			topic := string(route.Topic.(config.Topic))
			pid := route.Producer.(config.ProducerId)
			handler := s.newProduceHandler(&cfg, topic, s.ps.GetProducer(pid))
			s.engine.POST("/"+string(path), handler)
			slog.Info("Added endpoint.", "path", path, "topic", route.Topic, "pid", route.Producer)
		}
	}
	return nil
}

func (s *server) newProduceHandler(cfg *config.EndpointConfig, topic string, producer producer.Producer) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.ProduceRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ctx := c.Request.Context()
		s.metrics.RecordEndpointSizes(ctx, req, cfg.Endpoint)
		if cfg.Async {
			if err := producer.SendAsync(ctx, messageBatch(topic, req.Messages, cfg.Endpoint)); err != nil {
				handleProducerError(err, c)
			} else {
				c.Status(http.StatusNoContent)
			}
		} else {
			if res, err := producer.SendSync(ctx, messageBatch(topic, req.Messages, cfg.Endpoint)); err != nil {
				handleProducerError(err, c)
			} else {
				resp := model.ProduceResponse{Results: res}
				c.JSON(http.StatusOK, &resp)
			}
		}
	}
}

func (s *server) newRoutedProduceHandler(cfg *config.EndpointConfig, router router.Router) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.ProduceRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ctx := c.Request.Context()
		s.metrics.RecordEndpointSizes(ctx, req, cfg.Endpoint)
		if cfg.Async {
			if err := router.SendAsync(ctx, c.Request, req.Messages); err != nil {
				handleProducerError(err, c)
			} else {
				c.Status(http.StatusNoContent)
			}
		} else {
			if res, err := router.SendSync(ctx, c.Request, req.Messages); err != nil {
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
