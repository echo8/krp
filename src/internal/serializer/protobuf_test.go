package serializer

import (
	"encoding/base64"
	"testing"

	srconfig "github.com/echo8/krp/internal/config/schemaregistry"
	"github.com/echo8/krp/internal/util"
	"github.com/echo8/krp/model"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/stretchr/testify/require"
)

func TestProtobufSerializer(t *testing.T) {
	pbBytes, err := base64.StdEncoding.DecodeString("CgNmb28SA2JhciIFCgNiYXo=")
	require.NoError(t, err)
	payload, err := (&serde.BaseSerializer{}).WriteBytes(1, append([]byte{0}, pbBytes...))
	require.NoError(t, err)
	schema := `syntax = "proto3";

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
`

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
			name:       "protobuf value with defaults",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes: util.Ptr("CgNmb28SA2JhciIFCgNiYXo="),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.Protobuf,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-value",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema:     schema,
						SchemaType: "PROTOBUF",
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "protobuf value with record name",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:            util.Ptr("CgNmb28SA2JhciIFCgNiYXo="),
					SchemaRecordName: util.Ptr("MyMessage"),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.RecordName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.Protobuf,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "MyMessage",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema:     schema,
						SchemaType: "PROTOBUF",
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "protobuf value with topic record name",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:            util.Ptr("CgNmb28SA2JhciIFCgNiYXo="),
					SchemaRecordName: util.Ptr("MyMessage"),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicRecordName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.Protobuf,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-MyMessage",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema:     schema,
						SchemaType: "PROTOBUF",
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "protobuf value with schema id",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:    util.Ptr("CgNmb28SA2JhciIFCgNiYXo="),
					SchemaId: util.Ptr(1),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.Protobuf,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-value",
				expectedId:      1,
				getBySubjectAndID: schemaregistry.SchemaInfo{
					Schema:     schema,
					SchemaType: "PROTOBUF",
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "protobuf value with schema metadata",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Value: &model.ProduceData{
					Bytes:          util.Ptr("CgNmb28SA2JhciIFCgNiYXo="),
					SchemaMetadata: map[string]string{"foo": "bar"},
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.None,
				ValueSchemaType:     srconfig.Protobuf,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject:  "testTopic-value",
				expectedMetadata: map[string]string{"foo": "bar"},
				getLatestWithMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema:     schema,
						SchemaType: "PROTOBUF",
					},
				},
			},
			forKey: false,
			want:   payload,
		},
		{
			name:       "protobuf key with defaults",
			inputTopic: "testTopic",
			inputMessage: &model.ProduceMessage{
				Key: &model.ProduceData{
					Bytes: util.Ptr("CgNmb28SA2JhciIFCgNiYXo="),
				},
				Value: &model.ProduceData{
					Bytes: util.Ptr("CgNmb28SA2JhciIFCgNiYXo="),
				},
			},
			cfg: &srconfig.Config{
				Url:                 "localhost",
				SubjectNameStrategy: srconfig.TopicName,
				KeySchemaType:       srconfig.Protobuf,
				ValueSchemaType:     srconfig.Protobuf,
			},
			schemaRegistry: &mockSchemaRegistry{
				expectedSubject: "testTopic-key",
				getLatestSchemaMetadata: schemaregistry.SchemaMetadata{
					SchemaInfo: schemaregistry.SchemaInfo{
						Schema:     schema,
						SchemaType: "PROTOBUF",
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
			pbs := s.(*protobufSerializer)
			pbs.deterministic = true
			payload, err := pbs.Serialize(tc.inputTopic, tc.inputMessage)
			require.NoError(t, err)
			require.Equal(t, tc.want, payload)
		})
	}
}
