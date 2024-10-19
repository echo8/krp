package integrationtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/echo8/krp/model"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

type produceOptionals struct {
	headers map[string]string
}

type ProduceOption interface {
	apply(*produceOptionals)
}

type produceOptionFunc func(*produceOptionals)

func (f produceOptionFunc) apply(e *produceOptionals) {
	f(e)
}

func ProduceSync(ctx context.Context, t *testing.T, krp testcontainers.Container,
	path string, req model.ProduceRequest, options ...ProduceOption) {
	resBytes, statusCode := send(ctx, t, krp, path, req, options...)

	require.Equal(t, http.StatusOK, statusCode)

	var res model.ProduceResponse
	err := json.Unmarshal(resBytes, &res)
	require.NoError(t, err)

	expected := make([]model.ProduceResult, 0, len(req.Messages))
	for range req.Messages {
		expected = append(expected, model.ProduceResult{Success: true})
	}
	require.ElementsMatch(t, expected, res.Results)
}

func ProduceAsync(ctx context.Context, t *testing.T, krp testcontainers.Container,
	path string, req model.ProduceRequest, options ...ProduceOption) {
	resBytes, statusCode := send(ctx, t, krp, path, req, options...)
	require.Equal(t, http.StatusNoContent, statusCode)
	require.Equal(t, []byte(""), resBytes)
}

func ProduceError(ctx context.Context, t *testing.T, krp testcontainers.Container,
	path string, req any) (model.ProduceErrorResponse, int) {
	resBytes, statusCode := send(ctx, t, krp, path, req)

	var res model.ProduceErrorResponse
	err := json.Unmarshal(resBytes, &res)
	require.NoError(t, err)

	return res, statusCode
}

func send(ctx context.Context, t *testing.T, krp testcontainers.Container,
	path string, req any, options ...ProduceOption) ([]byte, int) {
	optionals := &produceOptionals{}
	for _, op := range options {
		op.apply(optionals)
	}

	mp, err := krp.MappedPort(ctx, "8080/tcp")
	require.NoError(t, err)

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	httpReq, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%s%s", mp.Port(), path), bytes.NewReader(reqBytes))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")
	for k, v := range optionals.headers {
		httpReq.Header.Set(k, v)
	}

	client := http.Client{Timeout: 2 * time.Second}
	httpRes, err := client.Do(httpReq)
	require.NoError(t, err)

	defer httpRes.Body.Close()
	resBytes, err := io.ReadAll(httpRes.Body)
	require.NoError(t, err)

	return resBytes, httpRes.StatusCode
}

func WithHeaders(headers map[string]string) ProduceOption {
	return produceOptionFunc(func(po *produceOptionals) {
		po.headers = headers
	})
}
