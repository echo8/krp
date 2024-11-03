package config

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/echo8/krp/internal/config/rdk"
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
			addr: ":8080"
			`,
			want: AppConfig{
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
					type: kafka
					clientConfig:
						bootstrap.servers: broker1
					schemaRegistry:
						url: schemaregistry1
						valueSchemaType: PROTOBUF
				beta:
					type: kafka
					clientConfig:
						bootstrap.servers: broker2
					schemaRegistry:
						url: schemaregistry1
						subjectNameStrategy: RECORD_NAME
						valueSchemaType: AVRO
			`,
			want: AppConfig{
				Addr: ":8080",
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
					"alpha": &rdk.ProducerConfig{
						Type:            "kafka",
						AsyncBufferSize: 100000,
						ClientConfig:    &rdk.ClientConfig{BootstrapServers: util.Ptr("broker1")},
						SchemaRegistry: &schemaregistry.Config{
							Url:                 "schemaregistry1",
							SubjectNameStrategy: schemaregistry.TopicName,
							KeySchemaType:       schemaregistry.None,
							ValueSchemaType:     schemaregistry.Protobuf,
						},
					},
					"beta": &rdk.ProducerConfig{
						Type:            "kafka",
						AsyncBufferSize: 100000,
						ClientConfig:    &rdk.ClientConfig{BootstrapServers: util.Ptr("broker2")},
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
					type: kafka
					clientConfig:
						bootstrap.servers: ${env:MY_ENV_1}
				beta:
					type: kafka
					clientConfig:
						bootstrap.servers: broker2-${env:MY_ENV_2}
			`,
			want: AppConfig{
				Addr: ":8080",
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
			require.NoError(t, err)
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
					routes:
						- topic: topic1
							producer: alpha
			`,
		},
		{
			name: "missing producer field",
			input: `
			addr: ":8080"
			endpoints:
				foo:
					routes:
						- topic: topic1
			`,
		},
		{
			name: "blank endpoint/namespace",
			input: `
			addr: ":8080"
			endpoints:
				"":
					routes:
						- topic: topic1
							producer: alpha
			`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			noTabs := strings.ReplaceAll(tc.input, "\t", "  ")
			_, err := loadFromBytes([]byte(noTabs))
			require.Error(t, err)
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
