package serializer

import (
	"echo8/kafka-rest-producer/internal/model"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/cache"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	jsonschema2 "github.com/santhosh-tekuri/jsonschema/v5"
)

// Serialization logic here is mostly adapted from Kafka's Go client code:
// https://github.com/confluentinc/confluent-kafka-go/blob/master/schemaregistry/serde/jsonschema/json_schema.go
// That code is copyright Confluent Inc. and licensed Apache 2.0

const (
	defaultBaseURL = "mem://input/"
)

type jsonSchemaSerializer struct {
	serde.BaseSerializer
	validate              bool
	schemaToTypeCache     cache.Cache
	schemaToTypeCacheLock sync.RWMutex
}

func (s *jsonSchemaSerializer) Serialize(topic string, message *model.ProduceMessage) ([]byte, error) {
	data := getData(message, s.SerdeType)
	schemaInfo := &schemaregistry.SchemaInfo{}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, *schemaInfo)
	if err != nil {
		return nil, err
	}
	updateConf(s.Conf, data)
	id, err := s.GetID(topic, nil, schemaInfo)
	if err != nil {
		return nil, err
	}
	dataBytes := data.GetBytes()
	if dataBytes == nil {
		return nil, fmt.Errorf("produce data must be sent as bytes when using schema registry")
	}
	var obj any
	err = json.Unmarshal(dataBytes, &obj)
	if err != nil {
		return nil, err
	}
	obj, err = s.ExecuteRules(subject, topic, schemaregistry.Write, nil, schemaInfo, obj)
	if err != nil {
		return nil, err
	}
	if s.validate {
		jschema, err := s.toJSONSchema(s.Client, *schemaInfo)
		if err != nil {
			return nil, err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return nil, err
		}
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, raw)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (s *jsonSchemaSerializer) toJSONSchema(c schemaregistry.Client, schema schemaregistry.SchemaInfo) (*jsonschema2.Schema, error) {
	s.schemaToTypeCacheLock.RLock()
	value, ok := s.schemaToTypeCache.Get(schema.Schema)
	s.schemaToTypeCacheLock.RUnlock()
	if ok {
		jsonType := value.(*jsonschema2.Schema)
		return jsonType, nil
	}
	deps := make(map[string]string)
	err := serde.ResolveReferences(c, schema, deps)
	if err != nil {
		return nil, err
	}
	compiler := jsonschema2.NewCompiler()
	compiler.RegisterExtension("confluent:tags", tagsMeta, tagsCompiler{})
	compiler.LoadURL = func(url string) (io.ReadCloser, error) {
		url = strings.TrimPrefix(url, defaultBaseURL)
		return io.NopCloser(strings.NewReader(deps[url])), nil
	}
	if err := compiler.AddResource(defaultBaseURL, strings.NewReader(schema.Schema)); err != nil {
		return nil, err
	}
	jsonType, err := compiler.Compile(defaultBaseURL)
	if err != nil {
		return nil, err
	}
	s.schemaToTypeCacheLock.Lock()
	s.schemaToTypeCache.Put(schema.Schema, jsonType)
	s.schemaToTypeCacheLock.Unlock()
	return jsonType, nil
}

var tagsMeta = jsonschema2.MustCompileString("tags.json", `{
	"properties" : {
		"confluent:tags": {
			"type": "array",
            "items": { "type": "string" }
		}
	}
}`)

type tagsCompiler struct{}

func (tagsCompiler) Compile(ctx jsonschema2.CompilerContext, m map[string]interface{}) (jsonschema2.ExtSchema, error) {
	if prop, ok := m["confluent:tags"]; ok {
		val, ok2 := prop.([]interface{})
		if ok2 {
			tags := make([]string, len(val))
			for i, v := range val {
				tags[i] = fmt.Sprint(v)
			}
			return tagsSchema(tags), nil
		}
	}
	return nil, nil
}

type tagsSchema []string

func (s tagsSchema) Validate(ctx jsonschema2.ValidationContext, v interface{}) error {
	return nil
}
