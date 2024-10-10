package schema

import (
	"echo8/kafka-rest-producer/internal/model"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/cache"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/hamba/avro/v2"
)

type avroSerializer struct {
	serde.BaseSerializer
	schemaToTypeCache     cache.Cache
	schemaToTypeCacheLock sync.RWMutex
}

func (s *avroSerializer) Serialize(topic string, message model.ProduceMessage) ([]byte, error) {
	schemaInfo := &schemaregistry.SchemaInfo{}
	id, err := s.GetID(topic, nil, schemaInfo)
	if err != nil {
		return nil, err
	}
	avroSchema, err := s.toType(s.Client, *schemaInfo)
	if err != nil {
		return nil, err
	}
	var msg any
	msg = map[string]any{}
	err = avro.Unmarshal(avroSchema, []byte(""), msg)
	if err != nil {
		return nil, err
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, *schemaInfo)
	if err != nil {
		return nil, err
	}
	msg, err = s.ExecuteRules(subject, topic, schemaregistry.Write, nil, schemaInfo, msg)
	if err != nil {
		return nil, err
	}
	msgBytes, err := avro.Marshal(avroSchema, msg)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, msgBytes)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (s *avroSerializer) toType(client schemaregistry.Client, schema schemaregistry.SchemaInfo) (avro.Schema, error) {
	s.schemaToTypeCacheLock.RLock()
	value, ok := s.schemaToTypeCache.Get(schema.Schema)
	s.schemaToTypeCacheLock.RUnlock()
	if ok {
		avroType := value.(avro.Schema)
		return avroType, nil
	}
	avroType, err := resolveAvroReferences(client, schema)
	if err != nil {
		return nil, err
	}
	s.schemaToTypeCacheLock.Lock()
	s.schemaToTypeCache.Put(schema.Schema, avroType)
	s.schemaToTypeCacheLock.Unlock()
	return avroType, nil
}

func resolveAvroReferences(c schemaregistry.Client, schema schemaregistry.SchemaInfo) (avro.Schema, error) {
	for _, ref := range schema.References {
		metadata, err := c.GetSchemaMetadataIncludeDeleted(ref.Subject, ref.Version, true)
		if err != nil {
			return nil, err
		}
		info := metadata.SchemaInfo
		_, err = resolveAvroReferences(c, info)
		if err != nil {
			return nil, err
		}

	}
	sType, err := avro.Parse(schema.Schema)
	if err != nil {
		return nil, err
	}
	return sType, nil
}
