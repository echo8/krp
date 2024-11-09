package config

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"github.com/echo8/krp/internal/util"

	"github.com/creasty/defaults"
	"github.com/go-playground/validator/v10"
	"github.com/go-playground/validator/v10/non-standard/validators"
	"gopkg.in/yaml.v3"
)

type AppConfig struct {
	Server    ServerConfig
	Endpoints EndpointConfigs `validate:"required_with=Producers,dive"`
	Producers ProducerConfigs `validate:"required_with=Endpoints,dive"`
	Metrics   MetricsConfig
}

func (c *AppConfig) validate() error {
	// validation using struct tags
	validate := validator.New(validator.WithRequiredStructEnabled())
	validate.RegisterValidation("notblank", validators.NotBlank)
	validate.RegisterValidation("notblankstrs", util.NotBlankStrs)
	validate.RegisterTagNameFunc(func(field reflect.StructField) string {
		return field.Tag.Get("yaml")
	})
	err := validate.Struct(c)
	if err != nil {
		return fmt.Errorf("invalid config: %w", handleValidationErrors(err))
	}
	// make sure producer ids used in endpoints actually exist
	for _, cfg := range c.Endpoints {
		for _, route := range cfg.Routes {
			switch v := route.Producer.(type) {
			case ProducerId:
				if v.HasTemplate() {
					continue
				}
				_, ok := c.Producers[v]
				if !ok {
					return fmt.Errorf(`invalid config, producer id "%s" does not exist`, v)
				}
			case ProducerIdList:
				for _, pid := range v {
					if pid.HasTemplate() {
						continue
					}
					_, ok := c.Producers[pid]
					if !ok {
						return fmt.Errorf(`invalid config, producer id "%s" does not exist`, pid)
					}
				}
			}
		}
	}
	// additional validation for metrics config
	// (because using defaults + struct validation tags is tricky)
	if c.Metrics.Enabled() {
		if !util.NotBlankStr(c.Metrics.Otel.Endpoint) {
			return fmt.Errorf("invalid config, otel endpoint must be specified when metrics are enabled")
		}
	}
	return nil
}

func handleValidationErrors(err error) error {
	var validationErrors validator.ValidationErrors
	if errors.As(err, &validationErrors) {
		for _, ve := range validationErrors {
			var errMsg string
			switch ve.Tag() {
			case "required":
				errMsg = fmt.Sprintf("%s: '%s' field is required",
					fieldPath(ve.Namespace(), ve.Field()), fieldName(ve.Field()))
			case "required_with":
				errMsg = fmt.Sprintf("%s: '%s' field must be specified when '%s' is present",
					fieldPath(ve.Namespace(), ve.Field()), fieldName(ve.Field()), fieldName(ve.Param()))
			case "required_without":
				errMsg = fmt.Sprintf("%s: '%s' OR '%s' field must be specified",
					fieldPath(ve.Namespace(), ve.Field()), fieldName(ve.Field()), fieldName(ve.Param()))
			case "notblank", "notblankstrs":
				if ve.Kind() == reflect.Slice {
					errMsg = fmt.Sprintf("%s: '%s' field must not contain a blank string",
						fieldPath(ve.Namespace(), ve.Field()), fieldName(ve.Field()))
				} else {
					errMsg = fmt.Sprintf("%s: '%s' field must not be blank",
						fieldPath(ve.Namespace(), ve.Field()), fieldName(ve.Field()))
				}
			default:
				slog.Warn("failed to parse validation error tag", "tag", ve.Tag())
			}
			if len(errMsg) > 0 {
				return errors.New(errMsg)
			}
		}
	}
	return err
}

func fieldPath(namespace, field string) string {
	return strings.TrimSuffix(namespace, "."+field)
}

func fieldName(field string) string {
	if field == "BootstrapServers" {
		return "bootstrap.servers"
	}
	return strings.ToLower(string(field[0])) + field[1:]
}

func Load(configPath string) (*AppConfig, error) {
	contents, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	return loadFromBytes(contents)
}

func loadFromBytes(contents []byte) (*AppConfig, error) {
	expanded, err := expandEnvVars(contents)
	if err != nil {
		return nil, fmt.Errorf("failed to expand environment variables in config file: %w", err)
	}
	config := &AppConfig{}
	if err := defaults.Set(config); err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(expanded))
	decoder.KnownFields(true)
	if err := decoder.Decode(config); err != nil {
		typeError, ok := err.(*yaml.TypeError)
		if ok {
			return nil, fmt.Errorf("invalid config: %w", typeError)
		}
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

func expandEnvVarsInMap(cfgMap map[string]any) {
	for k, v := range cfgMap {
		switch v := v.(type) {
		case string:
			cfgMap[k] = util.ExpandEnvVars(v)
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
				}
			}
		}
	}
}
