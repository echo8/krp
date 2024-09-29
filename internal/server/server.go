package server

import (
	"context"
	"errors"
	"fmt"
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
	for ns, cfgs := range s.cfg.Endpoints {
		for eid, cfg := range cfgs {
			var path string
			if ns == config.DefaultNamespace {
				path = fmt.Sprintf("/%v", eid)
			} else {
				path = fmt.Sprintf("/%v/%v", ns, eid)
			}
			if len(cfg.Topics) > 0 || len(cfg.Producers) > 0 || cfg.HasTemplates() {
				r, err := router.New(&cfg, s.ps)
				if err != nil {
					return err
				}
				handler := s.newRoutedProduceHandler(&cfg, r)
				s.engine.POST(path, handler)
				var t, p any
				if len(cfg.Topics) > 0 {
					t = cfg.Topics
				} else {
					t = cfg.Topic
				}
				if len(cfg.Producers) > 0 {
					p = cfg.Producers
				} else {
					p = cfg.Producer
				}
				slog.Info("Added endpoint.", "path", path, "topic(s)", t, "pid(s)", p)
			} else {
				handler := s.newProduceHandler(&cfg, s.ps.GetProducer(cfg.Producer))
				s.engine.POST(path, handler)
				slog.Info("Added endpoint.", "path", path, "topic", cfg.Topic, "pid", cfg.Producer)
			}
		}
	}
	return nil
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

func (s *server) newRoutedProduceHandler(cfg *config.EndpointConfig, router router.Router) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.ProduceRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ctx := c.Request.Context()
		s.metrics.RecordEndpointSizes(ctx, req, cfg.Endpoint)
		if res, err := router.Send(ctx, req.Messages); err != nil {
			handleProducerError(err, c)
		} else if res != nil {
			resp := model.ProduceResponse{Results: res}
			c.JSON(http.StatusOK, &resp)
		} else {
			c.Status(http.StatusNoContent)
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
