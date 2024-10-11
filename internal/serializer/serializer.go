package serializer

import (
	"echo8/kafka-rest-producer/internal/model"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

type Serializer interface {
	Serialize(topic string, message *model.ProduceMessage) ([]byte, error)
}

type defaultSerializer struct {
	serdeType serde.Type
}

func (s *defaultSerializer) Serialize(topic string, message *model.ProduceMessage) ([]byte, error) {
	data := getData(message, s.serdeType)
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

func updateConf(cfg *serde.SerializerConfig, data *model.ProduceData) {
	if data.SchemaId != nil {
		cfg.UseSchemaID = *data.SchemaId
	} else if data.SchemaMetadata != nil {
		cfg.UseLatestWithMetadata = data.SchemaMetadata
	} else {
		cfg.UseLatestVersion = true
	}
}
