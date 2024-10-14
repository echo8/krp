package serializer

import (
	srconfig "echo8/kafka-rest-producer/internal/config/schemaregistry"
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/util"
	"encoding/base64"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/stretchr/testify/require"
)

func TestJsonSchemaSerializer(t *testing.T) {
	json := `{"fieldFour":{"subFieldOne":"baz"},"fieldOne":"foo","fieldTwo":"bar"}`
	jsonBytes := []byte(json)
	payload, err := (&serde.BaseSerializer{}).WriteBytes(1, jsonBytes)
	require.NoError(t, err)
	jsonBase64 := base64.StdEncoding.EncodeToString(jsonBytes)
	schema := `{
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
}`

	testcases := []struct {
		name           string
		inputTopic     string
		inputMessage   *model.ProduceMessage
		cfg            *srconfig.Config
		schemaRegistry schemaregistry.Client
		forKey         bool
		want           []byte
	}{
		{
			name:       "json value with defaults",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes: util.Ptr(jsonBase64),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.JsonSchema,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-value",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema:     schema,
						SchemaType: "JSON",
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "json value with record name",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:            util.Ptr(jsonBase64),
					SchemaRecordName: util.Ptr("MyMessage"),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.RecordName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.JsonSchema,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "MyMessage",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema:     schema,
						SchemaType: "JSON",
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "json value with topic record name",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:            util.Ptr(jsonBase64),
					SchemaRecordName: util.Ptr("MyMessage"),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicRecordName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.JsonSchema,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-MyMessage",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema:     schema,
						SchemaType: "JSON",
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "json value with schema id",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:    util.Ptr(jsonBase64),
					SchemaId: util.Ptr(1),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.JsonSchema,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-value",
				expectedId:      1,
				getBySubjectAndID: schemaregistry.SchemaInfo{
					Schema:     schema,
					SchemaType: "JSON",
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "json value with schema metadata",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:          util.Ptr(jsonBase64),
					SchemaMetadata: map[string]string{"foo": "bar"},
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.JsonSchema,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject:  "testTopic-value",
				expectedMetadata: map[string]string{"foo": "bar"},
				getLatestWithMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema:     schema,
						SchemaType: "JSON",
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "json key with defaults",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Key: &model.ProduceData{
					Bytes: util.Ptr(jsonBase64),
				},
				Value: &model.ProduceData{
					Bytes: util.Ptr(jsonBase64),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.JsonSchema,
				ValueSchemaType:     srconfig.JsonSchema,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-key",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema:     schema,
						SchemaType: "JSONPROTOBUF",
					},
				},
			},
			forKey: true,
			want:   payload,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewSerializer(tc.cfg, tc.schemaRegistry, tc.forKey)
			require.NoError(t, err)
			payload, err := s.Serialize(tc.inputTopic, tc.inputMessage)
			require.NoError(t, err)
			require.Equal(t, tc.want, payload)
		})
	}
}
