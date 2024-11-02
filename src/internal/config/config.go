package config

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/echo8/krp/internal/config/rdk"
	"github.com/echo8/krp/internal/config/sarama"
	"github.com/echo8/krp/internal/config/schemaregistry"
	"github.com/echo8/krp/internal/config/segment"
	"github.com/echo8/krp/internal/util"

	"github.com/creasty/defaults"
	"gopkg.in/yaml.v3"
)

type ProducerConfig interface {
	Load(v any) error
	SchemaRegistryCfg() *schemaregistry.Config
}

type ProducerConfigs map[ProducerId]ProducerConfig

func (c *ProducerConfigs) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var rawMap map[string]interface{}
	if err := unmarshal(&rawMap); err != nil {
		return err
	}
	*c = ProducerConfigs{}
	for k, v := range rawMap {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("invalid config, producer ids cannot be blank")
		}
		switch v := v.(type) {
		case map[string]interface{}:
			typ, ok := v["type"]
			if ok {
				var cfg ProducerConfig
				switch typ {
				case "kafka":
					cfg = &rdk.ProducerConfig{}
				case "sarama":
					cfg = &sarama.ProducerConfig{}
				case "segment":
					cfg = &segment.ProducerConfig{}
				default:
					return fmt.Errorf("invalid config, unknown producer type: %v", typ)
				}
				if err := cfg.Load(v); err != nil {
					return fmt.Errorf("invalid config, failed to load %v producer config: %w", typ, err)
				}
				(*c)[ProducerId(k)] = cfg
			} else {
				return fmt.Errorf("invalid config, producer type is missing for: %v", k)
			}
		default:
			return fmt.Errorf("invalid config, invalid producer config: %v", k)
		}
	}
	return nil
}

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
	Tls            TlsConfig
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

type ServerConfig struct {
	Addr      string
	Endpoints EndpointConfigs
	Producers ProducerConfigs
	Metrics   MetricsConfig
}

func (c *ServerConfig) validate() error {
	for _, cfg := range c.Endpoints {
		for _, route := range cfg.Routes {
			switch v := route.Producer.(type) {
			case ProducerId:
				if v.HasTemplate() {
					continue
				}
				_, ok := c.Producers[v]
				if !ok {
					return fmt.Errorf("invalid config, no producer id: %s", v)
				}
			case ProducerIdList:
				for _, pid := range v {
					if pid.HasTemplate() {
						continue
					}
					_, ok := c.Producers[pid]
					if !ok {
						return fmt.Errorf("invalid config, no producer id: %s", v)
					}
				}
			}
		}
	}
	return nil
}

func Load(configPath string) (*ServerConfig, error) {
	contents, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	return loadFromBytes(contents)
}

func loadFromBytes(contents []byte) (*ServerConfig, error) {
	expanded, err := expandEnvVars(contents)
	if err != nil {
		return nil, fmt.Errorf("failed to expand environment variables in config file: %w", err)
	}
	config := &ServerConfig{}
	if err := defaults.Set(config); err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(expanded, config); err != nil {
		return nil, err
	}
	if config.Metrics.Enable.All {
		config.Metrics.Enable.Endpoint = true
		config.Metrics.Enable.Host = true
		config.Metrics.Enable.Http = true
		config.Metrics.Enable.Producer = true
		config.Metrics.Enable.Runtime = true
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	return config, nil
}

func expandEnvVars(contents []byte) ([]byte, error) {
	cfgMap := make(map[string]any)
	if err := yaml.Unmarshal(contents, cfgMap); err != nil {
		return nil, err
	}
	expandEnvVarsInMap(cfgMap)
	return yaml.Marshal(cfgMap)
}

func expandEnvVarsInMap(mp map[string]any) {
	for k, v := range mp {
		switch v := v.(type) {
		case string:
			mp[k] = util.ExpandEnvVars(v)
		case map[string]any:
			expandEnvVarsInMap(v)
		case []string:
			for i := range v {
				v[i] = util.ExpandEnvVars(v[i])
			}
		case []any:
			for i := range v {
				switch iv := v[i].(type) {
				case map[string]any:
					expandEnvVarsInMap(iv)
				case string:
					v[i] = util.ExpandEnvVars(iv)
				default:
					slog.Warn(
						"skipped processing part of the config file because of unknown type.",
						"type", reflect.TypeOf(v))
				}
			}
		default:
			slog.Warn(
				"skipped processing part of the config file because of unknown type.",
				"type", reflect.TypeOf(v))
		}
	}
}
