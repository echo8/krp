package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/creasty/defaults"
	"gopkg.in/yaml.v3"
)

type ProducerConfig interface {
	iAmAProducerConfig()
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
				switch typ {
				case "kafka":
					cfg, err := parseRdKafkaProducerConfig(v)
					if err != nil {
						return err
					}
					(*c)[ProducerId(k)] = *cfg
				case "sarama":
					cfg, err := parseSaramaProducerConfig(v)
					if err != nil {
						return err
					}
					(*c)[ProducerId(k)] = *cfg
				case "segment":
					cfg, err := parseSegmentProducerConfig(v)
					if err != nil {
						return err
					}
					(*c)[ProducerId(k)] = *cfg
				default:
					return fmt.Errorf("invalid config, unknown producer type: %v", typ)
				}
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
	Topic    string
	Producer ProducerId
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

type ServerConfig struct {
	Addr      string
	Endpoints NamespacedEndpointConfigs
	Producers ProducerConfigs
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
	config := &ServerConfig{}
	if err := defaults.Set(config); err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(contents, config); err != nil {
		return nil, err
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	return config, nil
}
