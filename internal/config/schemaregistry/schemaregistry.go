package schemaregistry

import (
	"fmt"

	srclient "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

type Config struct {
	Url                 string
	SubjectNameStrategy SubjectNameStrategy `yaml:"subjectNameStrategy"`
	KeySchemaType       SchemaType          `yaml:"keySchemaType"`
	ValueSchemaType     SchemaType          `yaml:"valueSchemaType"`
	AutoRegisterSchemas bool                `yaml:"autoRegisterSchemas"`
	NormalizeSchemas    bool                `yaml:"normalizeSchemas"`
	ValidateJsonSchema  bool                `yaml:"validateJsonSchema"`
}

func (s *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	cfg := &Config{
		SubjectNameStrategy: TopicName,
		KeySchemaType:       None,
		ValueSchemaType:     None,
	}
	type plain Config
	if err := unmarshal((*plain)(cfg)); err != nil {
		return err
	}
	*s = *cfg
	return nil
}

func (s *Config) ToClient() (srclient.Client, error) {
	return nil, nil
}

type SubjectNameStrategy string

const (
	TopicName       SubjectNameStrategy = "TOPIC_NAME"
	RecordName      SubjectNameStrategy = "RECORD_NAME"
	TopicRecordName SubjectNameStrategy = "TOPIC_RECORD_NAME"
)

func (s SubjectNameStrategy) String() string {
	return string(s)
}

func (s *SubjectNameStrategy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}
	switch val {
	case TopicName.String():
		*s = TopicName
	case RecordName.String():
		*s = RecordName
	case TopicRecordName.String():
		*s = TopicRecordName
	default:
		return fmt.Errorf("invalid subject name strategy: %v", val)
	}
	return nil
}

type SchemaType string

const (
	Avro       SchemaType = "AVRO"
	JsonSchema SchemaType = "JSON_SCHEMA"
	Protobuf   SchemaType = "PROTOBUF"
	None       SchemaType = "NONE"
)

func (s SchemaType) String() string {
	return string(s)
}

func (s *SchemaType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}
	switch val {
	case Avro.String():
		*s = Avro
	case JsonSchema.String():
		*s = JsonSchema
	case Protobuf.String():
		*s = Protobuf
	case None.String():
		*s = None
	default:
		return fmt.Errorf("invalid schema type: %v", val)
	}
	return nil
}
