package serializer

import (
	"encoding/base64"
	"testing"

	srconfig "github.com/echo8/krp/internal/config/schemaregistry"
	"github.com/echo8/krp/internal/model"
	"github.com/echo8/krp/internal/util"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/stretchr/testify/require"
)

func TestAvroSerializer(t *testing.T) {
	msgBase64 := "AAZmb28ABmJhcgIABmJhegI="
	msgBytes, err := base64.StdEncoding.DecodeString(msgBase64)
	require.NoError(t, err)
	payload, err := (&serde.BaseSerializer{}).WriteBytes(1, msgBytes)
	require.NoError(t, err)
	schema := `{"namespace": "example.avro",
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
			name:       "avro value with defaults",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes: util.Ptr(msgBase64),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.Avro,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-value",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema: schema,
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "avro value with record name",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:            util.Ptr(msgBase64),
					SchemaRecordName: util.Ptr("MyMessage"),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.RecordName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.Avro,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "MyMessage",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema: schema,
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "avro value with topic record name",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:            util.Ptr(msgBase64),
					SchemaRecordName: util.Ptr("MyMessage"),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicRecordName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.Avro,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-MyMessage",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema: schema,
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "avro value with schema id",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:    util.Ptr(msgBase64),
					SchemaId: util.Ptr(1),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.Avro,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-value",
				expectedId:      1,
				getBySubjectAndID: schemaregistry.SchemaInfo{
					Schema: schema,
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "avro value with schema metadata",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:          util.Ptr(msgBase64),
					SchemaMetadata: map[string]string{"foo": "bar"},
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.Avro,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject:  "testTopic-value",
				expectedMetadata: map[string]string{"foo": "bar"},
				getLatestWithMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema: schema,
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "avro key with defaults",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Key: &model.ProduceData{
					Bytes: util.Ptr(msgBase64),
				},
				Value: &model.ProduceData{
					Bytes: util.Ptr(msgBase64),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.Avro,
				ValueSchemaType:     srconfig.Avro,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-key",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema: schema,
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
