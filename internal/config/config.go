package config

import (
	"echo8/kafka-rest-producer/internal/config/rdk"
	"echo8/kafka-rest-producer/internal/config/sarama"
	"echo8/kafka-rest-producer/internal/config/schemaregistry"
	"echo8/kafka-rest-producer/internal/config/segment"
	"echo8/kafka-rest-producer/internal/util"
	"fmt"
	"os"
	"strings"
	"time"

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
		if isMap(v) {
			pc := v.(map[string]interface{})
			typ, ok := pc["type"]
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
					return err
				}
				(*c)[ProducerId(k)] = cfg
			} else {
				return fmt.Errorf("invalid config, producer type is missing for: %v", k)
			}
		} else {
			return fmt.Errorf("invalid config, invalid producer config: %v", k)
		}
	}
	return nil
}

func isMap(v interface{}) bool {
	switch v.(type) {
	case map[string]interface{}:
		return true
	default:
		return false
	}
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
		return err
	}
	if err := unmarshal(cfg); err != nil {
		return err
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
				_, ok := c.Producers[v]
				if !ok {
					return fmt.Errorf("invalid config, no producer id: %s", v)
				}
			case ProducerIdList:
				for _, pid := range v {
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
		return nil, err
	}
	return loadFromBytes(contents)
}

func loadFromBytes(contents []byte) (*ServerConfig, error) {
	expanded, err := expandEnvVars(contents)
	if err != nil {
		return nil, err
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
		switch rv := v.(type) {
		case string:
			mp[k] = util.ExpandEnvVars(rv)
		case map[string]any:
			expandEnvVarsInMap(rv)
		case []string:
			for i := range rv {
				rv[i] = util.ExpandEnvVars(rv[i])
			}
		case []any:
			for i := range rv {
				switch iv := rv[i].(type) {
				case map[string]any:
					expandEnvVarsInMap(iv)
				case string:
					rv[i] = util.ExpandEnvVars(iv)
				default:
					fmt.Printf("Missed type: %T", v)
				}
			}
		default:
			fmt.Printf("Missed type: %T", v)
		}
	}
}
