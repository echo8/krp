package serializer

import (
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/util"
	"encoding/base64"
	"testing"

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
			name: "blank value",
			inputMessage: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("")}},
			forKey: false,
			want: []byte(""),
		},
		{
			name: "null value data",
			inputMessage: &model.ProduceMessage{Value: &model.ProduceData{}},
			forKey: false,
			want: nil,
		},
		{
			name: "null value",
			inputMessage: &model.ProduceMessage{},
			forKey: false,
			want: nil,
		},
		{
			name: "string value",
			inputMessage: &model.ProduceMessage{Value: &model.ProduceData{String: util.Ptr("foo")}},
			forKey: false,
			want: []byte("foo"),
		},
		{
			name: "byte value",
			inputMessage: &model.ProduceMessage{Value: &model.ProduceData{Bytes: util.Ptr(base64.StdEncoding.EncodeToString([]byte("foo")))}},
			forKey: false,
			want: []byte("foo"),
		},
		{
			name: "blank key",
			inputMessage: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("")}},
			forKey: true,
			want: []byte(""),
		},
		{
			name: "null key data",
			inputMessage: &model.ProduceMessage{Key: &model.ProduceData{}},
			forKey: true,
			want: nil,
		},
		{
			name: "null key",
			inputMessage: &model.ProduceMessage{},
			forKey: true,
			want: nil,
		},
		{
			name: "string key",
			inputMessage: &model.ProduceMessage{Key: &model.ProduceData{String: util.Ptr("foo")}},
			forKey: true,
			want: []byte("foo"),
		},
		{
			name: "byte key",
			inputMessage: &model.ProduceMessage{Key: &model.ProduceData{Bytes: util.Ptr(base64.StdEncoding.EncodeToString([]byte("foo")))}},
			forKey: true,
			want: []byte("foo"),
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
