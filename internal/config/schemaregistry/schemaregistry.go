package schemaregistry

import "fmt"

type Config struct {
	Url                 string
	SubjectNameStrategy SubjectNameStrategy `yaml:"subjectNameStrategy"`
	SchemaIdStrategy    SchemaIdStrategy    `yaml:"schemaIdStrategy"`
}

const (
	TopicName SubjectNameStrategy = iota + 1
	RecordName
	TopicRecordName
)

type SubjectNameStrategy uint8

func (s SubjectNameStrategy) String() string {
	switch s {
	case TopicName:
		return "TOPIC_NAME"
	case RecordName:
		return "RECORD_NAME"
	case TopicRecordName:
		return "TOPIC_RECORD_NAME"
	default:
		return "UNKNOWN"
	}
}

func (s *SubjectNameStrategy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}
	switch val {
	case "TOPIC_NAME":
		*s = TopicName
	case "RECORD_NAME":
		*s = RecordName
	case "TOPIC_RECORD_NAME":
		*s = TopicRecordName
	default:
		return fmt.Errorf("invalid subject name strategy: %v", val)
	}
	return nil
}

const (
	AutoRegister SchemaIdStrategy = iota + 1
	UseSchemaId
	UseLatestWithMetadata
	UseLatestVersion
)

type SchemaIdStrategy uint8

func (s SchemaIdStrategy) String() string {
	switch s {
	case AutoRegister:
		return "AUTO_REGISTER"
	case UseSchemaId:
		return "USE_SCHEMA_ID"
	case UseLatestWithMetadata:
		return "USE_LATEST_WITH_METADATA"
	case UseLatestVersion:
		return "USE_LATEST_VERSION"
	default:
		return "UNKNOWN"
	}
}

func (s *SchemaIdStrategy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}
	switch val {
	case "AUTO_REGISTER":
		*s = AutoRegister
	case "USE_SCHEMA_ID":
		*s = UseSchemaId
	case "USE_LATEST_WITH_METADATA":
		*s = UseLatestWithMetadata
	case "USE_LATEST_VERSION":
		*s = UseLatestVersion
	default:
		return fmt.Errorf("invalid schema id strategy: %v", val)
	}
	return nil
}
