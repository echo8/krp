package config

import (
	"echo8/kafka-rest-producer/internal/config/rdk"
	"echo8/kafka-rest-producer/internal/util"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type MyConfig struct {
	Endpoints NamespacedEndpointConfigs
}

func TestConfig(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  ServerConfig
	}{
		{
			name: "only addr",
			input: `
			addr: ":8080"
			`,
			want: ServerConfig{
				Addr:    ":8080",
				Metrics: MetricsConfig{Otel: OtelConfig{ExportInterval: time.Duration(5 * time.Second)}},
			},
		},
		{
			name: "all",
			input: `
			addr: ":8080"
			endpoints:
				foo:
					topic: topic1
					producer: alpha
				bar:
					topic: topic2
					producer: beta
				baz:
					foo:
						topic: topic3
						producer: alpha
			producers:
				alpha:
					type: kafka
					clientConfig:
						bootstrap.servers: broker1
				beta:
					type: kafka
					clientConfig:
						bootstrap.servers: broker2
			`,
			want: ServerConfig{
				Addr: ":8080",
				Endpoints: NamespacedEndpointConfigs{
					DefaultNamespace: map[EndpointId]EndpointConfig{
						"foo": {Endpoint: &Endpoint{Id: "foo"}, Topic: "topic1", Producer: "alpha"},
						"bar": {Endpoint: &Endpoint{Id: "bar"}, Topic: "topic2", Producer: "beta"},
					},
					"baz": map[EndpointId]EndpointConfig{
						"foo": {Endpoint: &Endpoint{Namespace: "baz", Id: "foo"}, Topic: "topic3", Producer: "alpha"},
					},
				},
				Producers: ProducerConfigs{
					"alpha": &rdk.ProducerConfig{
						Type:            "kafka",
						AsyncBufferSize: 100000,
						ClientConfig:    &rdk.ClientConfig{BootstrapServers: util.Ptr("broker1")},
					},
					"beta": &rdk.ProducerConfig{
						Type:            "kafka",
						AsyncBufferSize: 100000,
						ClientConfig:    &rdk.ClientConfig{BootstrapServers: util.Ptr("broker2")},
					},
				},
				Metrics: MetricsConfig{Otel: OtelConfig{ExportInterval: time.Duration(5 * time.Second)}},
			},
		},
		{
			name: "with env vars",
			input: `
			addr: ":8080"
			endpoints:
				foo:
					topic: topic1-${env:MY_ENV_1}
					producer: alpha
				bar:
					topic: ${env:MY_ENV_2}
					producer: beta
				baz:
					foo:
						topic: topic3-${env:MY_ENV_1}-${env:DOES_NOT_EXIST|last}
						producer: alpha
			producers:
				alpha:
					type: kafka
					clientConfig:
						bootstrap.servers: ${env:MY_ENV_1}
				beta:
					type: kafka
					clientConfig:
						bootstrap.servers: broker2-${env:MY_ENV_2}
			`,
			want: ServerConfig{
				Addr: ":8080",
				Endpoints: NamespacedEndpointConfigs{
					DefaultNamespace: map[EndpointId]EndpointConfig{
						"foo": {Endpoint: &Endpoint{Id: "foo"}, Topic: "topic1-foo", Producer: "alpha"},
						"bar": {Endpoint: &Endpoint{Id: "bar"}, Topic: "bar", Producer: "beta"},
					},
					"baz": map[EndpointId]EndpointConfig{
						"foo": {Endpoint: &Endpoint{Namespace: "baz", Id: "foo"}, Topic: "topic3-foo-last", Producer: "alpha"},
					},
				},
				Producers: ProducerConfigs{
					"alpha": &rdk.ProducerConfig{
						Type:            "kafka",
						AsyncBufferSize: 100000,
						ClientConfig:    &rdk.ClientConfig{BootstrapServers: util.Ptr("foo")},
					},
					"beta": &rdk.ProducerConfig{
						Type:            "kafka",
						AsyncBufferSize: 100000,
						ClientConfig:    &rdk.ClientConfig{BootstrapServers: util.Ptr("broker2-bar")},
					},
				},
				Metrics: MetricsConfig{Otel: OtelConfig{ExportInterval: time.Duration(5 * time.Second)}},
			},
		},
	}

	os.Setenv("MY_ENV_1", "foo")
	os.Setenv("MY_ENV_2", "bar")
	defer os.Unsetenv("MY_ENV_1")
	defer os.Unsetenv("MY_ENV_2")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			noTabs := strings.ReplaceAll(tc.input, "\t", "  ")
			config, err := loadFromBytes([]byte(noTabs))
			require.Nil(t, err)
			require.Equal(t, &tc.want, config)
		})
	}
}

func TestConfigWithErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name: "missing producer",
			input: `
			addr: ":8080"
			endpoints:
				foo:
					topic: topic1
					producer: alpha
			`,
		},
		{
			name: "blank endpoint/namespace",
			input: `
			addr: ":8080"
			endpoints:
				"":
					topic: topic1
					producer: alpha
			`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			noTabs := strings.ReplaceAll(tc.input, "\t", "  ")
			_, err := loadFromBytes([]byte(noTabs))
			require.NotNil(t, err)
		})
	}
}

func TestRouteConfig(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  *RouteConfig
	}{
		{
			name: "single",
			input: `
			topic: foo
			producer: bar
			`,
			want: &RouteConfig{Topic: Topic("foo"), Producer: ProducerId("bar")},
		},
		{
			name: "list",
			input: `
			topic:
				- foo1
				- foo2
			producer:
				- bar1
				- bar2
			`,
			want: &RouteConfig{
				Topic:    TopicList([]Topic{"foo1", "foo2"}),
				Producer: ProducerIdList([]ProducerId{"bar1", "bar2"}),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &RouteConfig{}
			noTabs := strings.ReplaceAll(tc.input, "\t", "  ")
			err := yaml.Unmarshal([]byte(noTabs), cfg)
			require.Nil(t, err)
			require.Equal(t, tc.want, cfg)
		})
	}
}
