package util

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpandEnvVars(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "just env var",
			input: "${env:MY_ENV_1}",
			want:  "foo",
		},
		{
			name:  "env var as substring",
			input: "left-${env:MY_ENV_1}-right",
			want:  "left-foo-right",
		},
		{
			name:  "multiple env vars",
			input: "${env:MY_ENV_1}-${env:MY_ENV_2}",
			want:  "foo-bar",
		},
		{
			name:  "default val",
			input: "${env:MY_ENV_2|foo}-${env:DOES_NOT_EXIST|baz}",
			want:  "bar-baz",
		},
		{
			name:  "no env var",
			input: "foobar",
			want:  "foobar",
		},
		{
			name:  "no var name",
			input: "foo-${env:}",
			want:  "foo-${env:}",
		},
		{
			name:  "blank default",
			input: "foo-${env:DOES_NOT_EXIST|}",
			want:  "foo-",
		},
		{
			name:  "no default",
			input: "foo-${env:DOES_NOT_EXIST}",
			want:  "foo-",
		},
	}

	os.Setenv("MY_ENV_1", "foo")
	os.Setenv("MY_ENV_2", "bar")
	defer os.Unsetenv("MY_ENV_1")
	defer os.Unsetenv("MY_ENV_2")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ExpandEnvVars(tc.input))
		})
	}
}

func TestHasMsgVar(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "just msg var",
			input: "${msg:key}",
			want:  true,
		},
		{
			name:  "msg var as substring",
			input: "foo_${msg:headers.my_header}_bar",
			want:  true,
		},
		{
			name:  "no msg var",
			input: "foo",
			want:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, HasMsgVar(tc.input))
		})
	}
}

type ProduceMessage struct {
	Key     *string
	Headers map[string]string
}

func TestConvertToMsgTmpl(t *testing.T) {
	tests := []struct {
		name  string
		input string
		msg   *ProduceMessage
		want  string
	}{
		{
			name:  "just msg key var",
			input: "${msg:key}",
			msg:   &ProduceMessage{Key: Ptr("foo")},
			want:  "foo",
		},
		{
			name:  "msg key var as substring",
			input: "left-${msg:key}-right",
			msg:   &ProduceMessage{Key: Ptr("foo")},
			want:  "left-foo-right",
		},
		{
			name:  "escape template brackets",
			input: "${msg:key}-{{.Key}}-{{}}",
			msg:   &ProduceMessage{Key: Ptr("foo")},
			want:  "foo-{{.Key}}-{{}}",
		},
		{
			name:  "just msg header var",
			input: "${msg:header.foo}",
			msg:   &ProduceMessage{Headers: map[string]string{"foo": "bar"}},
			want:  "bar",
		},
		{
			name:  "msg header var as substring",
			input: "left-${msg:header.foo}-right",
			msg:   &ProduceMessage{Headers: map[string]string{"foo": "bar"}},
			want:  "left-bar-right",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mt, err := ConvertToMsgTmpl(tc.input)
			if err != nil {
				t.Fatal("Got error when converting to template:", err)
			}
			out := new(bytes.Buffer)
			err = mt.Execute(out, tc.msg)
			if err != nil {
				t.Fatal("Got error when executing template:", err)
			}
			require.Equal(t, tc.want, out.String())
		})
	}
}
