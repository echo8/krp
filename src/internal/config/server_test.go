package config

import (
	"testing"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/stretchr/testify/require"
)

func TestCorsGinConfig(t *testing.T) {
	testcases := []struct {
		name  string
		input *CorsConfig
		want  func() cors.Config
	}{
		{
			name:  "default",
			input: &CorsConfig{},
			want: func() cors.Config {
				return cors.DefaultConfig()
			},
		},
		{
			name: "allow origins",
			input: &CorsConfig{
				AllowOrigins: []string{"http://example.com"},
			},
			want: func() cors.Config {
				cfg := cors.DefaultConfig()
				cfg.AllowOrigins = []string{"http://example.com"}
				return cfg
			},
		},
		{
			name: "allow methods",
			input: &CorsConfig{
				AllowMethods: []string{"GET"},
			},
			want: func() cors.Config {
				cfg := cors.DefaultConfig()
				cfg.AllowMethods = []string{"GET"}
				return cfg
			},
		},
		{
			name: "allow headers",
			input: &CorsConfig{
				AllowHeaders: []string{"X-Foo"},
			},
			want: func() cors.Config {
				cfg := cors.DefaultConfig()
				cfg.AllowHeaders = []string{"X-Foo"}
				return cfg
			},
		},
		{
			name: "expose headers",
			input: &CorsConfig{
				ExposeHeaders: []string{"X-Foo"},
			},
			want: func() cors.Config {
				cfg := cors.DefaultConfig()
				cfg.ExposeHeaders = []string{"X-Foo"}
				return cfg
			},
		},
		{
			name: "allow private network",
			input: &CorsConfig{
				AllowPrivateNetwork: true,
			},
			want: func() cors.Config {
				cfg := cors.DefaultConfig()
				cfg.AllowPrivateNetwork = true
				return cfg
			},
		},
		{
			name: "allow credentials",
			input: &CorsConfig{
				AllowCredentials: true,
			},
			want: func() cors.Config {
				cfg := cors.DefaultConfig()
				cfg.AllowCredentials = true
				return cfg
			},
		},
		{
			name: "max age",
			input: &CorsConfig{
				MaxAge: 10 * time.Second,
			},
			want: func() cors.Config {
				cfg := cors.DefaultConfig()
				cfg.MaxAge = 10 * time.Second
				return cfg
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want(), tc.input.GinConfig())
		})
	}
}
