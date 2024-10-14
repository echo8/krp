package metric

import (
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/echo8/krp/internal/util"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	otm "go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func newHttpMeters() (*httpMeters, error) {
	hm := &httpMeters{}
	if err := util.CreateMeters(hm); err != nil {
		return nil, err
	}
	return hm, nil
}

type httpMeters struct {
	RequestSize   otm.Int64Histogram   `name:"krp.http.request.size" description:"Measures the size of HTTP requests." unit:"By"`
	ResponseSize  otm.Int64Histogram   `name:"krp.http.response.size" description:"Measures the size of HTTP responses." unit:"By"`
	ServerLatency otm.Float64Histogram `name:"krp.http.latency" description:"Measures the duration of inbound HTTP requests." unit:"ms"`
}

func GinMiddleware() gin.HandlerFunc {
	m, err := newHttpMeters()
	if err != nil {
		slog.Error("Failed to create http meters. Http metrics will be disabled.", "error", err)
		return func(c *gin.Context) {
			c.Next()
		}
	}
	return func(c *gin.Context) {
		requestStartTime := time.Now()

		c.Next()

		reqSize := calcReqSize(c.Request)
		respSize := int64(c.Writer.Size())
		if respSize < 0 {
			respSize = 0
		}
		attributes := metricAttributes(c.Request, c.FullPath(), c.Writer.Status())
		opts := otm.WithAttributeSet(attribute.NewSet(attributes...))
		elapsedTime := float64(time.Since(requestStartTime)) / float64(time.Millisecond)

		ctx := c.Request.Context()
		m.RequestSize.Record(ctx, reqSize, opts)
		m.ResponseSize.Record(ctx, respSize, opts)
		m.ServerLatency.Record(ctx, elapsedTime, opts)
	}
}

func calcReqSize(r *http.Request) int64 {
	size := 0
	if r.URL != nil {
		size = len(r.URL.String())
	}

	size += len(r.Method)
	size += len(r.Proto)

	for name, values := range r.Header {
		size += len(name)
		for _, value := range values {
			size += len(value)
		}
	}
	size += len(r.Host)

	if r.ContentLength != -1 {
		size += int(r.ContentLength)
	}
	return int64(size)
}

func metricAttributes(req *http.Request, route string, statusCode int) []attribute.KeyValue {
	n := 3
	host, p := splitHostPort(req.Host)
	hostPort := requiredHTTPPort(req.TLS != nil, p)
	if hostPort > 0 {
		n++
	}
	protoName, protoVersion := netProtocol(req.Proto)
	if protoName != "" {
		n++
	}
	if protoVersion != "" {
		n++
	}

	if statusCode > 0 {
		n++
	}

	if route != "" {
		n++
	}

	attributes := make([]attribute.KeyValue, 0, n)
	attributes = append(attributes,
		methodMetric(req.Method),
		scheme(req.TLS != nil),
		semconv.ServerAddress(host))

	if hostPort > 0 {
		attributes = append(attributes, semconv.ServerPort(hostPort))
	}
	if protoName != "" {
		attributes = append(attributes, semconv.NetworkProtocolName(protoName))
	}
	if protoVersion != "" {
		attributes = append(attributes, semconv.NetworkProtocolVersion(protoVersion))
	}

	if statusCode > 0 {
		attributes = append(attributes, semconv.HTTPResponseStatusCode(statusCode))
	}

	if route != "" {
		attributes = append(attributes, semconv.HTTPRoute(route))
	}
	return attributes
}

func methodMetric(method string) attribute.KeyValue {
	method = strings.ToUpper(method)
	switch method {
	case http.MethodConnect, http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPatch, http.MethodPost, http.MethodPut, http.MethodTrace:
	default:
		method = "_OTHER"
	}
	return semconv.HTTPRequestMethodKey.String(method)
}

func scheme(https bool) attribute.KeyValue {
	if https {
		return semconv.URLScheme("https")
	}
	return semconv.URLScheme("http")
}

func splitHostPort(hostport string) (host string, port int) {
	port = -1

	if strings.HasPrefix(hostport, "[") {
		addrEnd := strings.LastIndex(hostport, "]")
		if addrEnd < 0 {
			return
		}
		if i := strings.LastIndex(hostport[addrEnd:], ":"); i < 0 {
			host = hostport[1:addrEnd]
			return
		}
	} else {
		if i := strings.LastIndex(hostport, ":"); i < 0 {
			host = hostport
			return
		}
	}

	host, pStr, err := net.SplitHostPort(hostport)
	if err != nil {
		return
	}

	p, err := strconv.ParseUint(pStr, 10, 16)
	if err != nil {
		return
	}
	return host, int(p)
}

func requiredHTTPPort(https bool, port int) int {
	if https {
		if port > 0 && port != 443 {
			return port
		}
	} else {
		if port > 0 && port != 80 {
			return port
		}
	}
	return -1
}

func netProtocol(proto string) (name string, version string) {
	name, version, _ = strings.Cut(proto, "/")
	name = strings.ToLower(name)
	return name, version
}
