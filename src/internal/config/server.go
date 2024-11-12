package config

import (
	"fmt"
	"time"

	"github.com/creasty/defaults"
	"github.com/gin-contrib/cors"
)

type ServerConfig struct {
	Addr              string        `default:":8080"`
	ReadTimeout       time.Duration `yaml:"readTimeout"`
	ReadHeaderTimeout time.Duration `yaml:"readHeaderTimeout"`
	WriteTimeout      time.Duration `yaml:"writeTimeout"`
	IdleTimeout       time.Duration `yaml:"idleTimeout"`
	MaxHeaderBytes    int           `yaml:"maxHeaderBytes"`
	Cors              *CorsConfig   `yaml:"cors"`
}

func (o *ServerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type rawConfig ServerConfig
	cfg := &rawConfig{}
	if err := defaults.Set(cfg); err != nil {
		return fmt.Errorf("invalid config, failed to load server config: %w", err)
	}
	if err := unmarshal(cfg); err != nil {
		return fmt.Errorf("invalid config, failed to load server config: %w", err)
	}
	*o = ServerConfig(*cfg)
	return nil
}

type CorsConfig struct {
	AllowOrigins        []string      `yaml:"allowOrigins"`
	AllowMethods        []string      `yaml:"allowMethods"`
	AllowHeaders        []string      `yaml:"allowHeaders"`
	AllowPrivateNetwork bool          `yaml:"allowPrivateNetwork"`
	AllowCredentials    bool          `yaml:"allowCredentials"`
	ExposeHeaders       []string      `yaml:"exposeHeaders"`
	MaxAge              time.Duration `yaml:"maxAge"`
}

func (c *CorsConfig) GinConfig() cors.Config {
	ginConfig := cors.DefaultConfig()
	if c.AllowOrigins != nil {
		ginConfig.AllowOrigins = c.AllowOrigins
	}
	if c.AllowMethods != nil {
		ginConfig.AllowMethods = c.AllowMethods
	}
	if c.AllowHeaders != nil {
		ginConfig.AllowHeaders = c.AllowHeaders
	}
	ginConfig.AllowPrivateNetwork = c.AllowPrivateNetwork
	ginConfig.AllowCredentials = c.AllowCredentials
	if c.ExposeHeaders != nil {
		ginConfig.ExposeHeaders = c.ExposeHeaders
	}
	if c.MaxAge > 0 {
		ginConfig.MaxAge = c.MaxAge
	}
	return ginConfig
}
