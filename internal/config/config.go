package config

import (
	"echo8/kafka-rest-producer/internal/config/rdk"
	"echo8/kafka-rest-producer/internal/config/sarama"
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
}

type ProducerId string
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

type EndpointConfig struct {
	Endpoint  *Endpoint
	Topic     string
	Topics    []string
	Producer  ProducerId
	Producers []ProducerId
}

func (e EndpointConfig) HasTemplatedTopics() bool {
	if util.HasMsgVar(e.Topic) {
		return true
	}
	for _, t := range e.Topics {
		if util.HasMsgVar(t) {
			return true
		}
	}
	return false
}

func (e EndpointConfig) HasTemplatedProducers() bool {
	if util.HasMsgVar(string(e.Producer)) {
		return true
	}
	for _, p := range e.Producers {
		if util.HasMsgVar(string(p)) {
			return true
		}
	}
	return false
}

func (e EndpointConfig) HasTemplates() bool {
	return e.HasTemplatedTopics() || e.HasTemplatedProducers()
}

type Endpoint struct {
	Namespace string
	Id        string
}
type EndpointNamespace string
type EndpointId string
type NamespacedEndpointConfigs map[EndpointNamespace]map[EndpointId]EndpointConfig

const DefaultNamespace = EndpointNamespace("")

func (c *NamespacedEndpointConfigs) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var rawMap map[string]interface{}
	if err := unmarshal(&rawMap); err != nil {
		return err
	}
	*c = NamespacedEndpointConfigs{}
	for k, v := range rawMap {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("invalid config, endpoints/namespaces cannot be blank")
		}
		if isMap(v) {
			im := v.(map[string]interface{})
			defaultNs := false
			for ik, iv := range im {
				if strings.TrimSpace(ik) == "" {
					return fmt.Errorf("invalid config, endpoints/namespaces cannot be blank")
				}
				if isMap(iv) {
					_, ok := (*c)[EndpointNamespace(k)]
					if !ok {
						(*c)[EndpointNamespace(k)] = make(map[EndpointId]EndpointConfig)
					}
					cfg, err := parseEndpointConfig(iv)
					if err != nil {
						return err
					}
					cfg.Endpoint = &Endpoint{Namespace: k, Id: ik}
					(*c)[EndpointNamespace(k)][EndpointId(ik)] = *cfg
				} else {
					defaultNs = true
					break
				}
			}
			if defaultNs {
				_, ok := (*c)[DefaultNamespace]
				if !ok {
					(*c)[DefaultNamespace] = make(map[EndpointId]EndpointConfig)
				}
				cfg, err := parseEndpointConfig(v)
				if err != nil {
					return err
				}
				cfg.Endpoint = &Endpoint{Id: k}
				(*c)[DefaultNamespace][EndpointId(k)] = *cfg
			}
		} else {
			return fmt.Errorf("failed to parse config, expected map at: %s", k)
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

func parseEndpointConfig(v interface{}) (*EndpointConfig, error) {
	bytes, err := yaml.Marshal(v)
	if err != nil {
		return nil, err
	}
	cfg := &EndpointConfig{}
	err = yaml.Unmarshal(bytes, &cfg)
	return cfg, err
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
	Endpoints NamespacedEndpointConfigs
	Producers ProducerConfigs
	Metrics   MetricsConfig
}

func (c *ServerConfig) validate() error {
	for _, cfgs := range c.Endpoints {
		for _, cfg := range cfgs {
			_, ok := c.Producers[ProducerId(cfg.Producer)]
			if !ok {
				return fmt.Errorf("invalid config, no producer id: %s", cfg.Producer)
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
		}
	}
}
