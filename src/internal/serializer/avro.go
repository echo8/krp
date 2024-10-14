package serializer

import (
	"fmt"
	"sync"

	"github.com/echo8/krp/internal/model"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/cache"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/hamba/avro/v2"
)

// Serialization logic here is mostly adapted from Kafka's Go client code:
// https://github.com/confluentinc/confluent-kafka-go/blob/master/schemaregistry/serde/avrov2/avro.go
// That code is copyright Confluent Inc. and licensed Apache 2.0

type avroSerializer struct {
	serde.BaseSerializer
	schemaToTypeCache     cache.Cache
	schemaToTypeCacheLock sync.RWMutex
	resolver              *avro.TypeResolver
}

func newAvroSerializer(client schemaregistry.Client, serdeType serde.Type,
	subjectNameStrategy serde.SubjectNameStrategyFunc, conf *serde.SerializerConfig) (Serializer, error) {
	schemaToTypeCache, err := cache.NewLRUCache(1000)
	if err != nil {
		return nil, err
	}
	s := &avroSerializer{
		schemaToTypeCache: schemaToTypeCache,
		resolver:          avro.NewTypeResolver(),
	}
	err = s.ConfigureSerializer(client, serdeType, conf)
	if err != nil {
		return nil, err
	}
	s.SubjectNameStrategy = subjectNameStrategy
	return s, nil
}

func (s *avroSerializer) Serialize(topic string, message *model.ProduceMessage) ([]byte, error) {
	data := getData(message, s.SerdeType)
	if data == nil {
		return nil, nil
	}
	schemaInfo := &schemaregistry.SchemaInfo{}
	updateConfAndInfo(s.Conf, schemaInfo, data)
	id, err := s.GetID(topic, nil, schemaInfo)
	if err != nil {
		return nil, err
	}
	avroSchema, err := s.toType(s.Client, *schemaInfo)
	if err != nil {
		return nil, err
	}
	dataBytes := data.GetBytes()
	if dataBytes == nil {
		return nil, fmt.Errorf("no bytes found, data must be sent as bytes when using schema registry, %w", ErrSerialization)
	}
	msg := map[string]any{}
	err = avro.Unmarshal(avroSchema, dataBytes, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bytes to avro message: %v, %w", err.Error(), ErrSerialization)
	}
	msgBytes, err := avro.Marshal(avroSchema, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse avro message to bytes: %v, %w", err.Error(), ErrSerialization)
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
