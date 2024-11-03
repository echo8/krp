package config

import (
	"fmt"
	"time"

	"github.com/creasty/defaults"
)

type MetricsConfig struct {
	Enable MetricsEnableConfig
	Otel   OtelConfig
}

func (m MetricsConfig) Enabled() bool {
	return m.Enable.Endpoint || m.Enable.Host || m.Enable.Http || m.Enable.Producer || m.Enable.Runtime
}

type MetricsEnableConfig struct {
	All      bool
	Endpoint bool
	Host     bool
	Http     bool
	Producer bool
	Runtime  bool
}

type OtelConfig struct {
	Endpoint       string
	Tls            TlsConfig     `validate:"required_with=Endpoint"`
	ExportInterval time.Duration `yaml:"exportInterval" default:"5s"`
}

func (o *OtelConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type rawConfig OtelConfig
	cfg := &rawConfig{}
	if err := defaults.Set(cfg); err != nil {
		return fmt.Errorf("invalid config, failed to load metrics otel config: %w", err)
	}
	if err := unmarshal(cfg); err != nil {
		return fmt.Errorf("invalid config, failed to load metrics otel config: %w", err)
	}
	*o = OtelConfig(*cfg)
	return nil
}
