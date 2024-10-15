package serializer

import (
	"errors"
	"fmt"

	srconfig "github.com/echo8/krp/internal/config/schemaregistry"
	"github.com/echo8/krp/model"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

type Serializer interface {
	Serialize(topic string, message *model.ProduceMessage) ([]byte, error)
}

func NewSerializer(cfg *srconfig.Config, client schemaregistry.Client, forKey bool) (Serializer, error) {
	var serdeType serde.Type
	if forKey {
		serdeType = serde.KeySerde
	} else {
		serdeType = serde.ValueSerde
	}
	if cfg != nil {
		// using schema registry
		serConfig := &serde.SerializerConfig{}
		var subjectNameStrategy serde.SubjectNameStrategyFunc
		switch cfg.SubjectNameStrategy {
		case srconfig.TopicName:
			subjectNameStrategy = serde.TopicNameStrategy
		case srconfig.RecordName:
			subjectNameStrategy = RecordNameStrategy
		case srconfig.TopicRecordName:
			subjectNameStrategy = TopicRecordNameStrategy
		default:
			return nil, fmt.Errorf("invalid subject name strategy: %v", cfg.SubjectNameStrategy)
		}
		var schemaType srconfig.SchemaType
		var serdeTypeStr string
		if forKey {
			schemaType = cfg.KeySchemaType
			serdeTypeStr = "key"
		} else {
			schemaType = cfg.ValueSchemaType
			serdeTypeStr = "value"
		}
		switch schemaType {
		case srconfig.Avro:
			s, err := newAvroSerializer(client, serdeType, subjectNameStrategy, serConfig)
			if err != nil {
				return nil, err
			}
			return s, nil
		case srconfig.JsonSchema:
			s, err := newJsonSchemaSerializer(client, serdeType, subjectNameStrategy, serConfig, cfg.ValidateJsonSchema)
			if err != nil {
				return nil, err
			}
			return s, nil
		case srconfig.Protobuf:
			s, err := newProtobufSerializer(client, serdeType, subjectNameStrategy, serConfig)
			if err != nil {
				return nil, err
			}
			return s, nil
		case srconfig.None:
			return &defaultSerializer{serdeType}, nil
		default:
			return nil, fmt.Errorf("invalid %v schema type: %v", serdeTypeStr, schemaType)
		}
	} else {
		return &defaultSerializer{serdeType}, nil
	}
}

type defaultSerializer struct {
	serdeType serde.Type
}

func (s *defaultSerializer) Serialize(topic string, message *model.ProduceMessage) ([]byte, error) {
	data := getData(message, s.serdeType)
	if data == nil {
		return nil, nil
	}
	if data.String != nil {
		return []byte(*data.String), nil
	} else if data.Bytes != nil {
		return data.GetBytes(), nil
	}
	return nil, nil
}

func getData(message *model.ProduceMessage, serdeType serde.Type) *model.ProduceData {
	if serdeType == serde.ValueSerde {
		return message.Value
	} else {
		return message.Key
	}
}

func updateConfAndInfo(cfg *serde.SerializerConfig, schemaInfo *schemaregistry.SchemaInfo, data *model.ProduceData) {
	if data.SchemaId != nil {
		cfg.UseSchemaID = *data.SchemaId
	} else {
		cfg.UseSchemaID = -1
		if data.SchemaMetadata != nil {
			cfg.UseLatestWithMetadata = data.SchemaMetadata
		} else {
			cfg.UseLatestVersion = true
		}
	}
	if data.SchemaRecordName != nil {
		if schemaInfo.Metadata == nil {
			schemaInfo.Metadata = &schemaregistry.Metadata{
				Properties: make(map[string]string, 1),
			}
		}
		schemaInfo.Metadata.Properties["recordName"] = *data.SchemaRecordName
	}
}

func RecordNameStrategy(topic string, serdeType serde.Type, schema schemaregistry.SchemaInfo) (string, error) {
	recordName, ok := schema.Metadata.Properties["recordName"]
	if !ok {
		return "", fmt.Errorf("failed to find record name, a record name must be specified when using %v or %v subject name strategy, %w", srconfig.RecordName, srconfig.TopicRecordName, ErrSerialization)
	}
	return recordName, nil
}

func TopicRecordNameStrategy(topic string, serdeType serde.Type, schema schemaregistry.SchemaInfo) (string, error) {
	recordName, err := RecordNameStrategy(topic, serdeType, schema)
	if err != nil {
		return "", err
	}
	return topic + "-" + recordName, nil
}

var ErrSerialization = errors.New("")
