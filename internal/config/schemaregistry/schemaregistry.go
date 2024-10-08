package schemaregistry

import (
	"fmt"
)

type Config struct {
	Url                 string
	SubjectNameStrategy SubjectNameStrategy `yaml:"subjectNameStrategy"`
	SchemaIdStrategy    SchemaIdStrategy    `yaml:"schemaIdStrategy"`
	KeySchemaType       SchemaType          `yaml:"keySchemaType"`
	ValueSchemaType     SchemaType          `yaml:"valueSchemaType"`
}

func (s *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	cfg := &Config{
		SubjectNameStrategy: TopicName,
		SchemaIdStrategy:    UseLatestVersion,
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

type SchemaIdStrategy string

const (
	AutoRegister          SchemaIdStrategy = "AUTO_REGISTER"
	UseSchemaId           SchemaIdStrategy = "USE_SCHEMA_ID"
	UseLatestWithMetadata SchemaIdStrategy = "USE_LATEST_WITH_METADATA"
	UseLatestVersion      SchemaIdStrategy = "USE_LATEST_VERSION"
)

func (s SchemaIdStrategy) String() string {
	return string(s)
}

func (s *SchemaIdStrategy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}
	switch val {
	case AutoRegister.String():
		*s = AutoRegister
	case UseSchemaId.String():
		*s = UseSchemaId
	case UseLatestWithMetadata.String():
		*s = UseLatestWithMetadata
	case UseLatestVersion.String():
		*s = UseLatestVersion
	default:
		return fmt.Errorf("invalid schema id strategy: %v", val)
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
