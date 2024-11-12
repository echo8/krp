package config

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/echo8/krp/internal/config/confluent"
	"github.com/echo8/krp/internal/config/schemaregistry"
	"github.com/echo8/krp/internal/util"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConfig(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  AppConfig
	}{
		{
			name: "only addr",
			input: `
			server:
				addr: ":8080"
			`,
			want: AppConfig{
				Server: ServerConfig{
					Addr: ":8080",
				},
				Metrics: MetricsConfig{Otel: OtelConfig{ExportInterval: time.Duration(5 * time.Second)}},
			},
		},
		{
			name: "all",
			input: `
			server:
				addr: ":8080"
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
				bar:
					routes:
						- topic: topic2
							producer: beta
				"baz/foo":
					routes:
						- topic: topic3
							producer: alpha
			producers:
				alpha:
					type: confluent
					clientConfig:
						bootstrap.servers: broker1
					schemaRegistry:
						url: schemaregistry1
						valueSchemaType: PROTOBUF
				beta:
					type: confluent
					clientConfig:
						bootstrap.servers: broker2
					schemaRegistry:
						url: schemaregistry1
						subjectNameStrategy: RECORD_NAME
						valueSchemaType: AVRO
			`,
			want: AppConfig{
				Server: ServerConfig{
					Addr: ":8080",
				},
				Endpoints: EndpointConfigs{
					EndpointPath("foo"): {
						Endpoint: &Endpoint{Path: EndpointPath("foo")},
						Routes: []*RouteConfig{
							{Topic: Topic("topic1"), Producer: ProducerId("alpha")},
						},
					},
					EndpointPath("bar"): {
						Endpoint: &Endpoint{Path: EndpointPath("bar")},
						Routes: []*RouteConfig{
							{Topic: Topic("topic2"), Producer: ProducerId("beta")},
						},
					},
					EndpointPath("baz/foo"): {
						Endpoint: &Endpoint{Path: EndpointPath("baz/foo")},
						Routes: []*RouteConfig{
							{Topic: Topic("topic3"), Producer: ProducerId("alpha")},
						},
					},
				},
				Producers: ProducerConfigs{
					"alpha": &confluent.ProducerConfig{
						Type:            "confluent",
						AsyncBufferSize: 100000,
						ClientConfig:    &confluent.ClientConfig{BootstrapServers: util.Ptr("broker1")},
						SchemaRegistry: &schemaregistry.Config{
							Url:                 "schemaregistry1",
							SubjectNameStrategy: schemaregistry.TopicName,
							KeySchemaType:       schemaregistry.None,
							ValueSchemaType:     schemaregistry.Protobuf,
						},
					},
					"beta": &confluent.ProducerConfig{
						Type:            "confluent",
						AsyncBufferSize: 100000,
						ClientConfig:    &confluent.ClientConfig{BootstrapServers: util.Ptr("broker2")},
						SchemaRegistry: &schemaregistry.Config{
							Url:                 "schemaregistry1",
							SubjectNameStrategy: schemaregistry.RecordName,
							KeySchemaType:       schemaregistry.None,
							ValueSchemaType:     schemaregistry.Avro,
						},
					},
				},
				Metrics: MetricsConfig{Otel: OtelConfig{ExportInterval: time.Duration(5 * time.Second)}},
			},
		},
		{
			name: "with env vars",
			input: `
			server:
				addr: ":8080"
			endpoints:
				foo:
					routes:
						- topic: topic1-${env:MY_ENV_1}
							producer: alpha
				bar:
					routes:
						- topic: ${env:MY_ENV_2}
							producer: beta
				"baz/foo":
					routes:
						- topic: topic3-${env:MY_ENV_1}-${env:DOES_NOT_EXIST|last}
							producer: alpha
			producers:
				alpha:
					type: confluent
					clientConfig:
						bootstrap.servers: ${env:MY_ENV_1}
				beta:
					type: confluent
					clientConfig:
						bootstrap.servers: broker2-${env:MY_ENV_2}
			`,
			want: AppConfig{
				Server: ServerConfig{
					Addr: ":8080",
				},
				Endpoints: EndpointConfigs{
					EndpointPath("foo"): {
						Endpoint: &Endpoint{Path: EndpointPath("foo")},
						Routes: []*RouteConfig{
							{Topic: Topic("topic1-foo"), Producer: ProducerId("alpha")},
						},
					},
					EndpointPath("bar"): {
						Endpoint: &Endpoint{Path: EndpointPath("bar")},
						Routes: []*RouteConfig{
							{Topic: Topic("bar"), Producer: ProducerId("beta")},
						},
					},
					EndpointPath("baz/foo"): {
						Endpoint: &Endpoint{Path: EndpointPath("baz/foo")},
						Routes: []*RouteConfig{
							{Topic: Topic("topic3-foo-last"), Producer: ProducerId("alpha")},
						},
					},
				},
				Producers: ProducerConfigs{
					"alpha": &confluent.ProducerConfig{
						Type:            "confluent",
						AsyncBufferSize: 100000,
						ClientConfig:    &confluent.ClientConfig{BootstrapServers: util.Ptr("foo")},
					},
					"beta": &confluent.ProducerConfig{
						Type:            "confluent",
						AsyncBufferSize: 100000,
						ClientConfig:    &confluent.ClientConfig{BootstrapServers: util.Ptr("broker2-bar")},
					},
				},
				Metrics: MetricsConfig{Otel: OtelConfig{ExportInterval: time.Duration(5 * time.Second)}},
			},
		},
		{
			name: "default producer type",
			input: `
			server:
				addr: ":8080"
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					clientConfig:
						bootstrap.servers: broker1
			`,
			want: AppConfig{
				Server: ServerConfig{
					Addr: ":8080",
				},
				Endpoints: EndpointConfigs{
					EndpointPath("foo"): {
						Endpoint: &Endpoint{Path: EndpointPath("foo")},
						Routes: []*RouteConfig{
							{Topic: Topic("topic1"), Producer: ProducerId("alpha")},
						},
					},
				},
				Producers: ProducerConfigs{
					"alpha": &confluent.ProducerConfig{
						Type:            "confluent",
						AsyncBufferSize: 100000,
						ClientConfig:    &confluent.ClientConfig{BootstrapServers: util.Ptr("broker1")},
					},
				},
				Metrics: MetricsConfig{Otel: OtelConfig{ExportInterval: time.Duration(5 * time.Second)}},
			},
		},
		{
			name: "cors config",
			input: `
			server:
				addr: ":8080"
				cors:
					allowOrigins:
						- http://example.com
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					clientConfig:
						bootstrap.servers: broker1
			`,
			want: AppConfig{
				Server: ServerConfig{
					Addr: ":8080",
					Cors: &CorsConfig{
						AllowOrigins: []string{"http://example.com"},
					},
				},
				Endpoints: EndpointConfigs{
					EndpointPath("foo"): {
						Endpoint: &Endpoint{Path: EndpointPath("foo")},
						Routes: []*RouteConfig{
							{Topic: Topic("topic1"), Producer: ProducerId("alpha")},
						},
					},
				},
				Producers: ProducerConfigs{
					"alpha": &confluent.ProducerConfig{
						Type:            "confluent",
						AsyncBufferSize: 100000,
						ClientConfig:    &confluent.ClientConfig{BootstrapServers: util.Ptr("broker1")},
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
			require.NoError(t, err)
			require.Equal(t, &tc.want, config)
		})
	}
}

func TestConfigWithErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name: "missing producers section",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			`,
			want: "invalid config: AppConfig: 'producers' field must be specified when 'endpoints' is present",
		},
		{
			name: "missing routes",
			input: `
			endpoints:
				foo:
			`,
			want: "invalid config: AppConfig.Endpoints[foo]: 'routes' field is required",
		},
		{
			name: "empty routes",
			input: `
			endpoints:
				foo:
					routes:
			`,
			want: "invalid config: AppConfig.Endpoints[foo]: 'routes' field is required",
		},
		{
			name: "missing producer field",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
			`,
			want: "invalid config: AppConfig.Endpoints[foo].Routes[0]: 'producer' field is required",
		},
		{
			name: "missing topic field",
			input: `
			endpoints:
				foo:
					routes:
						- producer: alpha
			`,
			want: "invalid config: AppConfig.Endpoints[foo].Routes[0]: 'topic' field is required",
		},
		{
			name: "blank matcher",
			input: `
			endpoints:
				foo:
					routes:
						- match: " "
							topic: topic1
							producer: alpha
			`,
			want: "invalid config: AppConfig.Endpoints[foo].Routes[0]: 'match' field must not be blank",
		},
		{
			name: "blank topic",
			input: `
			endpoints:
				foo:
					routes:
						- topic: " "
							producer: alpha
			`,
			want: "invalid config: AppConfig.Endpoints[foo].Routes[0]: 'topic' field must not be blank",
		},
		{
			name: "blank topic list",
			input: `
			endpoints:
				foo:
					routes:
						- topic:
							- " "
							- topic1
							producer: alpha
			`,
			want: "invalid config: AppConfig.Endpoints[foo].Routes[0]: 'topic' field must not contain a blank string",
		},
		{
			name: "blank producer",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: " "
			producers:
				alpha:
					type: confluent
					clientConfig:
						bootstrap.servers: broker1
			`,
			want: `invalid config, producer id " " does not exist`,
		},
		{
			name: "blank producer list",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer:
							- " "
							- alpha
			producers:
				alpha:
					type: confluent
					clientConfig:
						bootstrap.servers: broker1
			`,
			want: `invalid config, producer id " " does not exist`,
		},
		{
			name: "missing endpoints section",
			input: `
			producers:
				alpha:
					type: confluent
					clientConfig:
						bootstrap.servers: broker1
			`,
			want: "invalid config: AppConfig: 'endpoints' field must be specified when 'producers' is present",
		},
		{
			name: "blank producer id",
			input: `
			producers:
				" ":
					type: confluent
					clientConfig:
						bootstrap.servers: broker1
			`,
			want: "invalid config, producer ids cannot be blank",
		},
		{
			name: "invalid producer type",
			input: `
			producers:
				alpha:
					type: foo
					clientConfig:
						bootstrap.servers: broker1
			`,
			want: "invalid config, unknown producer type: foo",
		},
		{
			name: "missing confluent client config",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: confluent
			`,
			want: "invalid config: AppConfig.Producers[alpha]: 'clientConfig' field is required",
		},
		{
			name: "missing sarama client config",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: sarama
			`,
			want: "invalid config: AppConfig.Producers[alpha]: 'clientConfig' field is required",
		},
		{
			name: "missing segment client config",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: segment
			`,
			want: "invalid config: AppConfig.Producers[alpha]: 'clientConfig' field is required",
		},
		{
			name: "missing confluent bootstrap servers",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: confluent
					clientConfig:
						client.id: foo
			`,
			want: "invalid config: AppConfig.Producers[alpha].clientConfig: 'metadata.broker.list' OR 'bootstrap.servers' field must be specified",
		},
		{
			name: "missing sarama bootstrap servers",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: sarama
					clientConfig:
						client.id: foo
			`,
			want: "invalid config: AppConfig.Producers[alpha].clientConfig: 'bootstrap.servers' field is required",
		},
		{
			name: "missing segment bootstrap servers",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: segment
					clientConfig:
						transport.client.id: foo
			`,
			want: "invalid config: AppConfig.Producers[alpha].clientConfig: 'bootstrap.servers' field is required",
		},
		{
			name: "missing schema registry url",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: confluent
					clientConfig:
						bootstrap.servers: localhost
					schemaRegistry:
						basicAuthUsername: foo
			`,
			want: "invalid config: AppConfig.Producers[alpha].schemaRegistry: 'url' field is required",
		},
		{
			name: "missing otel config",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: confluent
					clientConfig:
						bootstrap.servers: localhost
			metrics:
				enable:
					all: true
			`,
			want: "invalid config, otel endpoint must be specified when metrics are enabled",
		},
		{
			name: "missing otel tls config",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: confluent
					clientConfig:
						bootstrap.servers: localhost
			metrics:
				enable:
					all: true
				otel:
					endpoint: localhost
			`,
			want: "invalid config: AppConfig.Metrics.Otel: 'tls' field must be specified when 'endpoint' is present",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			noTabs := strings.ReplaceAll(tc.input, "\t", "  ")
			_, err := loadFromBytes([]byte(noTabs))
			require.Error(t, err)
			require.Equal(t, tc.want, err.Error())
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
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg)
		})
	}
}
