package serializer

import (
	"echo8/kafka-rest-producer/internal/model"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/cache"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/genproto/googleapis/type/calendarperiod"
	"google.golang.org/genproto/googleapis/type/color"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/datetime"
	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/genproto/googleapis/type/fraction"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/genproto/googleapis/type/money"
	"google.golang.org/genproto/googleapis/type/month"
	"google.golang.org/genproto/googleapis/type/postaladdress"
	"google.golang.org/genproto/googleapis/type/quaternion"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/apipb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/sourcecontextpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/typepb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Serialization logic here is mostly adapted from Kafka's Go client code:
// https://github.com/confluentinc/confluent-kafka-go/blob/master/schemaregistry/serde/protobuf/protobuf.go
// That code is copyright Confluent Inc. and licensed Apache 2.0

var builtInDeps = make(map[string]string)

func init() {
	builtins := map[string]protoreflect.FileDescriptor{
		"confluent/meta.proto":                 confluent.File_confluent_meta_proto,
		"confluent/type/decimal.proto":         types.File_confluent_types_decimal_proto,
		"google/type/calendar_period.proto":    calendarperiod.File_google_type_calendar_period_proto,
		"google/type/color.proto":              color.File_google_type_color_proto,
		"google/type/date.proto":               date.File_google_type_date_proto,
		"google/type/datetime.proto":           datetime.File_google_type_datetime_proto,
		"google/type/dayofweek.proto":          dayofweek.File_google_type_dayofweek_proto,
		"google/type/expr.proto":               expr.File_google_type_expr_proto,
		"google/type/fraction.proto":           fraction.File_google_type_fraction_proto,
		"google/type/latlng.proto":             latlng.File_google_type_latlng_proto,
		"google/type/money.proto":              money.File_google_type_money_proto,
		"google/type/month.proto":              month.File_google_type_month_proto,
		"google/type/postal_address.proto":     postaladdress.File_google_type_postal_address_proto,
		"google/type/quaternion.proto":         quaternion.File_google_type_quaternion_proto,
		"google/type/timeofday.proto":          timeofday.File_google_type_timeofday_proto,
		"google/protobuf/any.proto":            anypb.File_google_protobuf_any_proto,
		"google/protobuf/api.proto":            apipb.File_google_protobuf_api_proto,
		"google/protobuf/descriptor.proto":     descriptorpb.File_google_protobuf_descriptor_proto,
		"google/protobuf/duration.proto":       durationpb.File_google_protobuf_duration_proto,
		"google/protobuf/empty.proto":          emptypb.File_google_protobuf_empty_proto,
		"google/protobuf/field_mask.proto":     fieldmaskpb.File_google_protobuf_field_mask_proto,
		"google/protobuf/source_context.proto": sourcecontextpb.File_google_protobuf_source_context_proto,
		"google/protobuf/struct.proto":         structpb.File_google_protobuf_struct_proto,
		"google/protobuf/timestamp.proto":      timestamppb.File_google_protobuf_timestamp_proto,
		"google/protobuf/type.proto":           typepb.File_google_protobuf_type_proto,
		"google/protobuf/wrappers.proto":       wrapperspb.File_google_protobuf_wrappers_proto,
	}
	var fds []*descriptorpb.FileDescriptorProto
	for _, value := range builtins {
		fd := protodesc.ToFileDescriptorProto(value)
		fds = append(fds, fd)
	}
	fdMap, err := desc.CreateFileDescriptors(fds)
	if err != nil {
		log.Fatalf("Could not create fds")
	}
	printer := protoprint.Printer{OmitComments: protoprint.CommentsAll}
	for key, value := range fdMap {
		var writer strings.Builder
		err = printer.PrintProtoFile(value, &writer)
		if err != nil {
			log.Fatalf("Could not print %s", key)
		}
		builtInDeps[key] = writer.String()
	}
}

type protobufSerializer struct {
	serde.BaseSerializer
	schemaToDescCache     cache.Cache
	schemaToDescCacheLock sync.RWMutex
}

func newProtobufSerializer(client schemaregistry.Client, serdeType serde.Type,
	subjectNameStrategy serde.SubjectNameStrategyFunc, conf *serde.SerializerConfig) (Serializer, error) {
	schemaToDescCache, err := cache.NewLRUCache(1000)
	if err != nil {
		return nil, err
	}
	s := &protobufSerializer{
		schemaToDescCache: schemaToDescCache,
	}
	err = s.ConfigureSerializer(client, serdeType, conf)
	if err != nil {
		return nil, err
	}
	s.SubjectNameStrategy = subjectNameStrategy
	return s, nil
}

func (s *protobufSerializer) Serialize(topic string, message *model.ProduceMessage) ([]byte, error) {
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
	fileDesc, err := s.toFileDesc(s.Client, *schemaInfo)
	if err != nil {
		return nil, err
	}
	if len(fileDesc.GetMessageTypes()) == 0 {
		return nil, fmt.Errorf("failed to find first top-level protobuf message")
	}
	pbMsgType := fileDesc.GetMessageTypes()[0].AsDescriptorProto().ProtoReflect()
	pbMsg := pbMsgType.New().Interface()
	dataBytes := data.GetBytes()
	if dataBytes == nil {
		return nil, fmt.Errorf("no bytes found, data must be sent as bytes when using schema registry, %w", ErrSerialization)
	}
	err = proto.Unmarshal(dataBytes, pbMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse data: %v, %w", err.Error(), ErrSerialization)
	}
	msgIndexBytes := toMessageIndexBytes(pbMsg.ProtoReflect().Descriptor())
	msgBytes, err := proto.Marshal(pbMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse data: %v, %w", err.Error(), ErrSerialization)
	}
	payload, err := s.WriteBytes(id, append(msgIndexBytes, msgBytes...))
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (s *protobufSerializer) toFileDesc(client schemaregistry.Client, info schemaregistry.SchemaInfo) (*desc.FileDescriptor, error) {
	s.schemaToDescCacheLock.RLock()
	value, ok := s.schemaToDescCache.Get(info.Schema)
	s.schemaToDescCacheLock.RUnlock()
	if ok {
		return value.(*desc.FileDescriptor), nil
	}
	fd, err := parseFileDesc(client, info)
	if err != nil {
		return nil, err
	}
	s.schemaToDescCacheLock.Lock()
	s.schemaToDescCache.Put(info.Schema, fd)
	s.schemaToDescCacheLock.Unlock()
	return fd, nil
}

func parseFileDesc(client schemaregistry.Client, info schemaregistry.SchemaInfo) (*desc.FileDescriptor, error) {
	deps := make(map[string]string)
	err := serde.ResolveReferences(client, info, deps)
	if err != nil {
		return nil, err
	}
	parser := protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			var schema string
			if filename == "." {
				schema = info.Schema
			} else {
				schema = deps[filename]
			}
			if schema == "" {
				schema = builtInDeps[filename]
			}
			return io.NopCloser(strings.NewReader(schema)), nil
		},
	}

	fileDescriptors, err := parser.ParseFiles(".")
	if err != nil {
		return nil, err
	}

	if len(fileDescriptors) != 1 {
		return nil, fmt.Errorf("could not resolve schema")
	}
	fd := fileDescriptors[0]
	return fd, nil
}

func toMessageIndexBytes(descriptor protoreflect.Descriptor) []byte {
	if descriptor.Index() == 0 {
		switch descriptor.Parent().(type) {
		case protoreflect.FileDescriptor:
			// This is an optimization for the first message in the schema
			return []byte{0}
		}
	}
	msgIndexes := toMessageIndexes(descriptor, 0)
	buf := make([]byte, (1+len(msgIndexes))*binary.MaxVarintLen64)
	length := binary.PutVarint(buf, int64(len(msgIndexes)))

	for _, element := range msgIndexes {
		length += binary.PutVarint(buf[length:], int64(element))
	}
	return buf[0:length]
}

// Adapted from ideasculptor, see https://github.com/riferrei/srclient/issues/17
func toMessageIndexes(descriptor protoreflect.Descriptor, count int) []int {
	index := descriptor.Index()
	switch v := descriptor.Parent().(type) {
	case protoreflect.FileDescriptor:
		// parent is FileDescriptor, we reached the top of the stack, so we are
		// done. Allocate an array large enough to hold count+1 entries and
		// populate first value with index
		msgIndexes := make([]int, count+1)
		msgIndexes[0] = index
		return msgIndexes[0:1]
	default:
		// parent is another MessageDescriptor.  We were nested so get that
		// descriptor's indexes and append the index of this one
		msgIndexes := toMessageIndexes(v, count+1)
		return append(msgIndexes, index)
	}
}
