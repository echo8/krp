package router

import (
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/util"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMatcher(t *testing.T) {
	testTimestamp := time.Date(2000, 2, 1, 12, 13, 14, 0, time.UTC)
	tests := []struct {
		name         string
		inputMatch   string
		inputMsg     model.ProduceMessage
		inputHttpReq http.Request
		want         bool
	}{
		{
			name:         "all match",
			inputMatch:   "",
			inputMsg:     model.ProduceMessage{},
			inputHttpReq: http.Request{},
			want:         true,
		},
		{
			name:         "match on message key",
			inputMatch:   "message.key == 'foo'",
			inputMsg:     model.ProduceMessage{Key: util.Ptr("foo")},
			inputHttpReq: http.Request{},
			want:         true,
		},
		{
			name:         "match on message value",
			inputMatch:   "message.value == 'bar'",
			inputMsg:     model.ProduceMessage{Value: util.Ptr("bar")},
			inputHttpReq: http.Request{},
			want:         true,
		},
		{
			name:         "match on message header",
			inputMatch:   "message.headers['foo'] == 'bar'",
			inputMsg:     model.ProduceMessage{Headers: map[string]string{"foo": "bar"}},
			inputHttpReq: http.Request{},
			want:         true,
		},
		{
			name:         "match on http header",
			inputMatch:   "httpHeader('Foo') == 'bar'",
			inputMsg:     model.ProduceMessage{},
			inputHttpReq: http.Request{Header: map[string][]string{"Foo": {"bar"}}},
			want:         true,
		},
		{
			name:         "match on message timestamp",
			inputMatch:   "message.timestamp.String() == '2000-02-01 12:13:14 +0000 UTC'",
			inputMsg:     model.ProduceMessage{Timestamp: &testTimestamp},
			inputHttpReq: http.Request{},
			want:         true,
		},
		{
			name:         "match failure on message key",
			inputMatch:   "message.key == 'foo'",
			inputMsg:     model.ProduceMessage{Key: util.Ptr("bar")},
			inputHttpReq: http.Request{},
			want:         false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matcher, err := newRouteMatcher(tc.inputMatch)
			require.NoError(t, err)
			actual := matcher.Matches(&tc.inputMsg, &tc.inputHttpReq)
			require.Equal(t, tc.want, actual)
		})
	}
}
