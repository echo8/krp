package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/facebookgo/grace/gracehttp"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"

	"github.com/echo8/krp/internal/config"
	"github.com/echo8/krp/internal/metric"
	pmodel "github.com/echo8/krp/internal/model"
	"github.com/echo8/krp/internal/producer"
	"github.com/echo8/krp/internal/router"
	"github.com/echo8/krp/internal/serializer"
	"github.com/echo8/krp/model"
)

type Server interface {
	Run() error
	ServeHTTP(w http.ResponseWriter, req *http.Request)
}

type server struct {
	cfg     *config.AppConfig
	ps      producer.Service
	metrics metric.Service

	engine *gin.Engine
	srv    *http.Server
}

func NewServer(cfg *config.AppConfig, ps producer.Service, ms metric.Service) (Server, error) {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	if cfg.Metrics.Enable.Http {
		engine.Use(metric.GinMiddleware())
	}
	if cfg.Server.Cors != nil {
		engine.Use(cors.New(cfg.Server.Cors.GinConfig()))
	}
	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		ReadTimeout:       cfg.Server.ReadTimeout,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
		MaxHeaderBytes:    cfg.Server.MaxHeaderBytes,
		Handler:           engine,
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
			r, err := router.New(&cfg, s.ps, s.metrics)
			if err != nil {
				return err
			}
			handler := s.newRoutedProduceHandler(&cfg, r)
			s.engine.POST(string(path), handler)
			slog.Info("added endpoint.", "path", path)
		} else {
			route := cfg.Routes[0]
			topic := string(route.Topic.(config.Topic))
			pid := route.Producer.(config.ProducerId)
			handler := s.newProduceHandler(&cfg, topic, s.ps.GetProducer(pid))
			s.engine.POST(string(path), handler)
			slog.Info("added endpoint.", "path", path, "topic", route.Topic, "pid", route.Producer)
		}
	}
	s.engine.GET("/healthcheck", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	return nil
}

func (s *server) newProduceHandler(cfg *config.EndpointConfig, topic string, producer producer.Producer) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.ProduceRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			handleBindError(err, c)
			return
		}
		ctx := c.Request.Context()
		s.metrics.RecordEndpointSizes(ctx, req, cfg.Endpoint)
		if cfg.Async {
			if err := producer.SendAsync(ctx, messageBatch(topic, req.Messages, cfg.Endpoint)); err != nil {
				handleError(err, c)
			} else {
				c.Status(http.StatusNoContent)
			}
		} else {
			if res, err := producer.SendSync(ctx, messageBatch(topic, req.Messages, cfg.Endpoint)); err != nil {
				handleError(err, c)
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
			handleBindError(err, c)
			return
		}
		ctx := c.Request.Context()
		s.metrics.RecordEndpointSizes(ctx, req, cfg.Endpoint)
		if cfg.Async {
			if err := router.SendAsync(ctx, c.Request, req.Messages); err != nil {
				handleError(err, c)
			} else {
				c.Status(http.StatusNoContent)
			}
		} else {
			if res, err := router.SendSync(ctx, c.Request, req.Messages); err != nil {
				handleError(err, c)
			} else {
				resp := model.ProduceResponse{Results: res}
				c.JSON(http.StatusOK, &resp)
			}
		}
	}
}

func messageBatch(topic string, messages []model.ProduceMessage, src *config.Endpoint) *pmodel.MessageBatch {
	mts := make([]pmodel.TopicAndMessage, len(messages))
	for i := range messages {
		mts[i] = pmodel.TopicAndMessage{Topic: topic, Message: &messages[i]}
	}
	return &pmodel.MessageBatch{Messages: mts, Src: src}
}

func handleError(err error, c *gin.Context) {
	if errors.Is(err, serializer.ErrSerialization) {
		c.JSON(http.StatusBadRequest, &model.ProduceErrorResponse{Error: err.Error()})
	} else if errors.Is(err, context.Canceled) {
		c.JSON(499, &model.ProduceErrorResponse{Error: "timeout"})
	} else {
		slog.Error("Producer request failed.", "error", err)
		c.JSON(http.StatusInternalServerError,
			&model.ProduceErrorResponse{Error: "internal error occurred"})
	}
}

func handleBindError(err error, c *gin.Context) {
	var validationErrors validator.ValidationErrors
	if errors.As(err, &validationErrors) {
		errorMap := make(map[string]bool, len(validationErrors))
		for _, ve := range validationErrors {
			var errMsg string
			switch ve.Tag() {
			case "required":
				if strings.HasPrefix(ve.Field(), "Headers") {
					errMsg = fmt.Sprint("'", strings.ToLower(ve.Field()), "' must not be null or blank")
				} else {
					errMsg = fmt.Sprint("'", strings.ToLower(ve.Field()), "' field is required")
				}
			case "gt":
				errMsg = fmt.Sprint("'", strings.ToLower(ve.Field()), "' field cannot be empty")
			case "required_without":
				errMsg = "'string' OR 'bytes' field must be specified"
			case "excluded_with":
				errMsg = "'schemaId' and 'schemaMetadata' fields cannot both be specified"
			case "isdefault|gt=0":
				errMsg = "'headers' field cannot be empty"
			default:
				slog.Warn("failed to parse validation error tag", "tag", ve.Tag())
			}
			if len(errMsg) > 0 {
				errorMap[errMsg] = true
			} else {
				errorMap[ve.Error()] = true
			}
		}

		errorMsgs := make([]string, 0, len(errorMap))
		for errorMsg := range errorMap {
			errorMsgs = append(errorMsgs, errorMsg)
		}
		slices.Sort(errorMsgs)

		c.JSON(http.StatusBadRequest, &model.ProduceErrorResponse{
			Error: strings.Join(errorMsgs, ", "),
		})
		return
	}
	var timeParseError *time.ParseError
	if errors.As(err, &timeParseError) {
		c.JSON(http.StatusBadRequest, &model.ProduceErrorResponse{
			Error: fmt.Sprint("failed to parse 'timestamp' field, ", err.Error()),
		})
	} else {
		c.JSON(http.StatusBadRequest, &model.ProduceErrorResponse{
			Error: err.Error(),
		})
	}
}

func (s *server) Run() error {
	return gracehttp.Serve(s.srv)
}

func (s *server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.engine.ServeHTTP(w, req)
}
