package serializer

import (
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/echo8/krp/internal/model"
	"github.com/echo8/krp/internal/util"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/stretchr/testify/require"
)

func TestDefaultSerializer(t *testing.T) {
	testcases := []struct {
		name         string
		inputMessage *model.ProduceMessage
		forKey       bool
		want         []byte
	}{
		{
			name:         "blank value",
			inputMessage: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("")}},
			forKey:       false,
			want:         []byte(""),
		},
		{
			name:         "null value data",
			inputMessage: &model.ProduceMessage{Value: &model.ProduceData{}},
			forKey:       false,
			want:         nil,
		},
		{
			name:         "null value",
			inputMessage: &model.ProduceMessage{},
			forKey:       false,
			want:         nil,
		},
		{
			name:         "string value",
			inputMessage: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("foo")}},
			forKey:       false,
			want:         []byte("foo"),
		},
		{
			name:         "byte value",
			inputMessage: &model.ProduceMessage{Value: &model.ProduceData{Bytes: util.Ptr(base64.StdEncoding.EncodeToString([]byte("foo")))}},
			forKey:       false,
			want:         []byte("foo"),
		},
		{
			name:         "blank key",
			inputMessage: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("")}},
			forKey:       true,
			want:         []byte(""),
		},
		{
			name:         "null key data",
			inputMessage: &model.ProduceMessage{Key: &model.ProduceData{}},
			forKey:       true,
			want:         nil,
		},
		{
			name:         "null key",
			inputMessage: &model.ProduceMessage{},
			forKey:       true,
			want:         nil,
		},
		{
			name:         "string key",
			inputMessage: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}},
			forKey:       true,
			want:         []byte("foo"),
		},
		{
			name:         "byte key",
			inputMessage: &model.ProduceMessage{Key: &model.ProduceData{Bytes: util.Ptr(base64.StdEncoding.EncodeToString([]byte("foo")))}},
			forKey:       true,
			want:         []byte("foo"),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewSerializer(nil, nil, tc.forKey)
			require.NoError(t, err)
			payload, err := s.Serialize("", tc.inputMessage)
			require.NoError(t, err)
			require.Equal(t, tc.want, payload)
		})
	}
}

type mockSchemaRegistry struct {
	expectedSubject         string
	expectedId              int
	expectedMetadata        map[string]string
	getLatestSchemaMetadata schemaregistry.SchemaMetadata
	getBySubjectAndID       schemaregistry.SchemaInfo
	getLatestWithMetadata   schemaregistry.SchemaMetadata
}

func (s *mockSchemaRegistry) GetAllContexts() ([]string, error) {
	return nil, nil
}
func (s *mockSchemaRegistry) Config() *schemaregistry.Config {
	return nil
}
func (s *mockSchemaRegistry) Register(subject string, schema schemaregistry.SchemaInfo, normalize bool) (id int, err error) {
	return -1, nil
}
func (s *mockSchemaRegistry) RegisterFullResponse(subject string, schema schemaregistry.SchemaInfo, normalize bool) (result schemaregistry.SchemaMetadata, err error) {
	return schemaregistry.SchemaMetadata{}, nil
}
func (s *mockSchemaRegistry) GetBySubjectAndID(subject string, id int) (schema schemaregistry.SchemaInfo, err error) {
	if subject == s.expectedSubject && id == s.expectedId {
		return s.getBySubjectAndID, nil
	}
	return schemaregistry.SchemaInfo{}, nil
}
func (s *mockSchemaRegistry) GetSubjectsAndVersionsByID(id int) (subjectAndVersion []schemaregistry.SubjectAndVersion, err error) {
	return nil, nil
}
func (s *mockSchemaRegistry) GetID(subject string, schema schemaregistry.SchemaInfo, normalize bool) (id int, err error) {
	if subject == s.expectedSubject &&
		schema.Schema == s.getLatestSchemaMetadata.SchemaInfo.Schema &&
		schema.SchemaType == s.getLatestSchemaMetadata.SchemaInfo.SchemaType {
		return 1, nil
	}
	if subject == s.expectedSubject &&
		schema.Schema == s.getBySubjectAndID.Schema &&
		schema.SchemaType == s.getBySubjectAndID.SchemaType {
		return 1, nil
	}
	if subject == s.expectedSubject &&
		schema.Schema == s.getLatestWithMetadata.SchemaInfo.Schema &&
		schema.SchemaType == s.getLatestWithMetadata.SchemaInfo.SchemaType {
		return 1, nil
	}
	return -1, nil
}
func (s *mockSchemaRegistry) GetLatestSchemaMetadata(subject string) (schemaregistry.SchemaMetadata, error) {
	if subject == s.expectedSubject {
		return s.getLatestSchemaMetadata, nil
	}
	return schemaregistry.SchemaMetadata{}, nil
}
func (s *mockSchemaRegistry) GetSchemaMetadata(subject string, version int) (schemaregistry.SchemaMetadata, error) {
	return schemaregistry.SchemaMetadata{}, nil
}
func (s *mockSchemaRegistry) GetSchemaMetadataIncludeDeleted(subject string, version int, deleted bool) (schemaregistry.SchemaMetadata, error) {
	return schemaregistry.SchemaMetadata{}, nil
}
func (s *mockSchemaRegistry) GetLatestWithMetadata(subject string, metadata map[string]string, deleted bool) (schemaregistry.SchemaMetadata, error) {
	if subject == s.expectedSubject && reflect.DeepEqual(metadata, s.expectedMetadata) {
		return s.getLatestWithMetadata, nil
	}
	return schemaregistry.SchemaMetadata{}, nil
}
func (s *mockSchemaRegistry) GetAllVersions(subject string) ([]int, error) {
	return nil, nil
}
func (s *mockSchemaRegistry) GetVersion(subject string, schema schemaregistry.SchemaInfo, normalize bool) (version int, err error) {
	return -1, nil
}
func (s *mockSchemaRegistry) GetAllSubjects() ([]string, error) {
	return nil, nil
}
func (s *mockSchemaRegistry) DeleteSubject(subject string, permanent bool) ([]int, error) {
	return nil, nil
}
func (s *mockSchemaRegistry) DeleteSubjectVersion(subject string, version int, permanent bool) (deletes int, err error) {
	return -1, nil
}
func (s *mockSchemaRegistry) TestSubjectCompatibility(subject string, schema schemaregistry.SchemaInfo) (compatible bool, err error) {
	return false, nil
}
func (s *mockSchemaRegistry) TestCompatibility(subject string, version int, schema schemaregistry.SchemaInfo) (compatible bool, err error) {
	return false, nil
}
func (s *mockSchemaRegistry) GetCompatibility(subject string) (compatibility schemaregistry.Compatibility, err error) {
	return schemaregistry.None, nil
}
func (s *mockSchemaRegistry) UpdateCompatibility(subject string, update schemaregistry.Compatibility) (compatibility schemaregistry.Compatibility, err error) {
	return schemaregistry.None, nil
}
func (s *mockSchemaRegistry) GetDefaultCompatibility() (compatibility schemaregistry.Compatibility, err error) {
	return schemaregistry.None, nil
}
func (s *mockSchemaRegistry) UpdateDefaultCompatibility(update schemaregistry.Compatibility) (compatibility schemaregistry.Compatibility, err error) {
	return schemaregistry.None, nil
}
func (s *mockSchemaRegistry) GetConfig(subject string, defaultToGlobal bool) (result schemaregistry.ServerConfig, err error) {
	return schemaregistry.ServerConfig{}, nil
}
func (s *mockSchemaRegistry) UpdateConfig(subject string, update schemaregistry.ServerConfig) (result schemaregistry.ServerConfig, err error) {
	return schemaregistry.ServerConfig{}, nil
}
func (s *mockSchemaRegistry) GetDefaultConfig() (result schemaregistry.ServerConfig, err error) {
	return schemaregistry.ServerConfig{}, nil
}
func (s *mockSchemaRegistry) UpdateDefaultConfig(update schemaregistry.ServerConfig) (result schemaregistry.ServerConfig, err error) {
	return schemaregistry.ServerConfig{}, nil
}
func (s *mockSchemaRegistry) Close() error {
	return nil
}
