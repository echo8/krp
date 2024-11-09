package e2e

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/echo8/krp/model"
	"github.com/echo8/krp/tests/internal/testutil"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

func TestSchemas(t *testing.T) {
	ctx := context.Background()

	avroBase64 := "AAZmb28ABmJhcgIABmJhegI="
	avroBytes, err := base64.StdEncoding.DecodeString(avroBase64)
	require.NoError(t, err)
	avroPayload, err := (&serde.BaseSerializer{}).WriteBytes(1, avroBytes)
	require.NoError(t, err)

	pbBase64 := "CgNmb28SA2JhciIFCgNiYXo="
	pbBytes, err := base64.StdEncoding.DecodeString(pbBase64)
	require.NoError(t, err)
	pbPayload, err := (&serde.BaseSerializer{}).WriteBytes(1, append([]byte{0}, pbBytes...))
	require.NoError(t, err)

	json := `{"fieldFour":{"subFieldOne":"baz"},"fieldOne":"foo","fieldTwo":"bar"}`
	jsonBytes := []byte(json)
	jsonPayload, err := (&serde.BaseSerializer{}).WriteBytes(1, jsonBytes)
	require.NoError(t, err)
	jsonBase64 := base64.StdEncoding.EncodeToString(jsonBytes)

	testcases := []struct {
		name         string
		inputCfg     string
		inputSubject string
		inputSchema  schemaregistry.SchemaInfo
		inputReq     model.ProduceRequest
		want         []byte
	}{
		{
			name: "avro schema",
			inputCfg: `
			valueSchemaType: AVRO
			`,
			inputSubject: "topic1-value",
			inputSchema: schemaregistry.SchemaInfo{
				Schema: `{"namespace": "example.avro",
 "type": "record",
 "name": "MyMessage",
 "fields": [
  {"name": "FieldOne", "type": ["string", "null"]},
  {"name": "FieldTwo", "type": ["string", "null"]},
  {"name": "FieldThree", "type": ["long", "null"]},
  {"name": "FieldFour", "type": {
    "type": "record",
		"name": "MySubMessage",
		"fields": [
			{"name": "SubFieldOne", "type": ["string", "null"]},
  		{"name": "SubFieldTwo", "type": ["string", "null"]}
		]	
   }
	}
 ]
}`,
			},
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value: &model.ProduceData{
							Bytes: testutil.Ptr(avroBase64),
						},
					},
				},
			},
			want: avroPayload,
		},
		{
			name: "protobuf schema",
			inputCfg: `
			valueSchemaType: PROTOBUF
			deterministicProtoSerializer: true
			`,
			inputSubject: "topic1-value",
			inputSchema: schemaregistry.SchemaInfo{
				SchemaType: "PROTOBUF",
				Schema: `syntax = "proto3";

message MyMessage {
  string fieldOne = 1;
  string fieldTwo = 2;
  int64 fieldThree = 3;
  MySubMessage fieldFour = 4;
}

message MySubMessage {
  string subFieldOne = 1;
  string subFieldTwo = 2;
}
`,
			},
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value: &model.ProduceData{
							Bytes: testutil.Ptr(pbBase64),
						},
					},
				},
			},
			want: pbPayload,
		},
		{
			name: "json schema",
			inputCfg: `
			valueSchemaType: JSON_SCHEMA
			`,
			inputSubject: "topic1-value",
			inputSchema: schemaregistry.SchemaInfo{
				SchemaType: "JSON",
				Schema: `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "test schema",
  "type": "object",
  "properties": {
    "fieldOne": {
      "type": "string"
    },
    "fieldTwo": {
      "type": "string"
    },
    "fieldThree": {
      "type": "number"
    },
    "fieldFour": {
      "type": "object",
      "properties": {
        "subFieldOne": {
          "type": "string"
        },
        "subFieldTwo": {
          "type": "string"
        }
      }
    }
  }
}`,
			},
			inputReq: model.ProduceRequest{
				Messages: []model.ProduceMessage{
					{
						Value: &model.ProduceData{
							Bytes: testutil.Ptr(jsonBase64),
						},
					},
				},
			},
			want: jsonPayload,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			network, err := network.New(ctx)
			require.NoError(t, err)
			defer network.Remove(ctx)

			broker, err := testutil.NewKafkaContainer(ctx, "broker", "9094", network.Name)
			require.NoError(t, err)
			defer broker.Terminate(ctx)

			sr, err := testutil.NewSchemaRegistryContainer(ctx, network.Name)
			require.NoError(t, err)
			defer sr.Terminate(ctx)

			krp, err := testutil.NewKrpContainer(ctx, network.Name, `
endpoints:
  first:
    routes:
      - topic: topic1
        producer: confluent
producers:
  confluent:
    type: confluent
    clientConfig:
      bootstrap.servers: broker:9092
    schemaRegistry:
      url: "http://schema-registry:8081"
`+testutil.FormatCfg(tc.inputCfg))
			require.NoError(t, err)
			defer krp.Terminate(ctx)

			srClient := newSchemaRegistryClient(ctx, t, sr)
			defer srClient.Close()

			_, err = srClient.Register(tc.inputSubject, tc.inputSchema, false)
			require.NoError(t, err)

			testutil.ProduceSync(ctx, t, krp, "/first", tc.inputReq)

			consumer := testutil.NewConsumer(ctx, t, "topic1", "9094")
			defer consumer.Close()
			received := testutil.GetReceived(t, consumer, tc.inputReq.Messages, testutil.WithVerifyReceived(false))
			require.Equal(t, 1, len(received))
			require.Equal(t, tc.want, received[0].Value)
		})
	}
}

func newSchemaRegistryClient(ctx context.Context, t *testing.T, sr testcontainers.Container) schemaregistry.Client {
	srMappedPort, err := sr.MappedPort(ctx, "8081/tcp")
	require.NoError(t, err)
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(fmt.Sprintf("http://localhost:%s", srMappedPort.Port())))
	require.NoError(t, err)
	return srClient
}
