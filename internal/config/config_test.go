package config

import (
	"koko/kafka-rest-producer/internal/util"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
			want: ServerConfig{Addr: ":8080"},
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
					client-config:
						bootstrap.servers: broker1
				beta:
					type: kafka
					client-config:
						bootstrap.servers: broker2
			`,
			want: ServerConfig{
				Addr: ":8080",
				Endpoints: NamespacedEndpointConfigs{
					DefaultNamespace: map[EndpointId]EndpointConfig{
						"foo": {Topic: "topic1", Producer: "alpha"},
						"bar": {Topic: "topic2", Producer: "beta"},
					},
					"baz": map[EndpointId]EndpointConfig{
						"foo": {Topic: "topic3", Producer: "alpha"},
					},
				},
				Producers: ProducerConfigs{
					"alpha": RdKafkaProducerConfig{
						Type: "kafka", 
						AsyncBufferSize: 100000, 
						ClientConfig: RdKafkaClientConfig{BootstrapServers: util.Ptr("broker1")},
					},
					"beta":  RdKafkaProducerConfig{
						Type: "kafka", 
						AsyncBufferSize: 100000, 
						ClientConfig: RdKafkaClientConfig{BootstrapServers: util.Ptr("broker2")},
					},
				},
			},
		},
	}

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
