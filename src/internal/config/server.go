package config

import (
	"fmt"
	"time"

	"github.com/creasty/defaults"
)

type ServerConfig struct {
	Addr              string        `default:":8080"`
	ReadTimeout       time.Duration `yaml:"readTimeout"`
	ReadHeaderTimeout time.Duration `yaml:"readHeaderTimeout"`
	WriteTimeout      time.Duration `yaml:"writeTimeout"`
	IdleTimeout       time.Duration `yaml:"idleTimeout"`
	MaxHeaderBytes    int           `yaml:"maxHeaderBytes"`
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
