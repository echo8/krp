---
sidebar_position: 1
---

# Configuration

KRP is configured using a single YAML file.

## Top-level elements

| Name      | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `server` | [Server Config](#server-config) | no | HTTP server settings. |
| `endpoints` | [Endpoints](#endpoints) | yes | REST endpoints and routing rules. |
| `producers` | [Producers](#producers) | yes | Kafka producer settings. |
| `metrics` | [Metrics Config](#metrics-config) | no | Observability metrics settings. |

## Server Config

| Name      | Type | Required | Description | Default |
| --------- | ---- | -------- | ----------- | ------- |
| `addr` | string | no | TCP address for the HTTP server to listen on. | `:8080` |
| `readTimeout` | [duration string](https://pkg.go.dev/time#ParseDuration) | no | The maximum duration for reading the entire request, including the body. | |
| `readHeaderTimeout` | [duration string](https://pkg.go.dev/time#ParseDuration) | no | The maximum duration for reading the request headers. | |
| `writeTimeout` | [duration string](https://pkg.go.dev/time#ParseDuration) | no | The maximum duration before timing out writes of the response. | |
| `idleTimeout` | [duration string](https://pkg.go.dev/time#ParseDuration) | no | The maximum amount of time to wait for the next request when keep-alives are enabled. | |
| `maxHeaderBytes` | int | no | The maximum number of bytes the server will read parsing the request header's keys and values, including the request line. | |
| `cors` | [CORS Config](#cors-config) | no | Cross-Origin Resource Sharing settings. | |

### CORS Config

| Name      | Type | Required | Description | Default |
| --------- | ---- | -------- | ----------- | ------- |
| `allowOrigins` | list of strings | no | List of origins a cross-domain request can be executed from. If the special `*` value is present in the list, all origins will be allowed. | |
| `allowMethods` | list of strings | no | List of methods the client is allowed to use with cross-domain requests. | `[GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]` |
| `allowHeaders` | list of strings | no | List of non simple headers the client is allowed to use with cross-domain requests. | `[Origin, Content-Length, Content-Type]` |
| `allowPrivateNetwork` | bool | no | Whether the response should include the allow private network header. | `false` |
| `allowCredentials` | bool | no | Whether the request can include user credentials like cookies, HTTP authentication or client side SSL certificates. | `false` |
| `exposeHeaders` | list of strings | no | List of headers that are safe to expose to the API of a CORS API specification. | |
| `maxAge` | [duration string](https://pkg.go.dev/time#ParseDuration) | no | How long (with second-precision) the results of a preflight request can be cached. | `12h` |

## Endpoints

A map between HTTP REST endpoint paths and configs.

#### Sample

```yaml
endpoints:
  /foo/messages:
    routes:
      - topic: test-topic1
        producer: alpha
  /bar/messages:
    async: true
    routes:
      - topic: test-topic2
        producer: beta
```

### Endpoint Path

An HTTP URL path. The leading slash is optional and a blank path is valid.

### Endpoint Config

| Name      | Type | Required | Description | Default |
| --------- | ---- | -------- | ----------- | ------- |
| `async` | bool | no | If `true` messages sent to this endpoint will be written asynchronously to Kafka, meaning the server will not wait for a response from Kafka before completing the HTTP request. | `false` |
| `routes` | list of [Route Configs](#route-config) | yes | Routes (i.e. Kafka (producer, topic) pairs) to write messages to. All routes will always be evaluated for each incoming message. | |

### Route Config

| Name      | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `match` | string | no | An [Expr](https://expr-lang.org/docs/language-definition) expression that decides if the incoming message matches this route. If this field is not specified then all incoming messages will match this route. See [Route Matchers](./route-matchers.md) for more info. |
| `topic` | string or list of strings | yes | Kafka topic(s) to write messages to. [Templating](./templating.md) is allowed. |
| `producer` | string or list of strings | yes | ID(s) of Kafka producer(s) used to write messages. [Templating](./templating.md) is allowed. |

## Producers

A map between Kafka producer IDs and configs.

#### Sample

```yaml
producers:
  alpha:
    clientConfig:
      bootstrap.servers: localhost:9092
      linger.ms: 10
      compression.type: snappy
  beta:
    clientConfig:
      bootstrap.servers: localhost:9092
    schemaRegistry:
      url: http://schema-registry:8081
      valueSchemaType: PROTOBUF
```

### Producer ID

A non-blank string. Used to reference this producer throughout the config.

### Producer Config

| Name      | Type | Required | Description | Default |
| --------- | ---- | -------- | ----------- | ------- |
| `type` | string | no | The Kafka client implementation to use. | `confluent` |
| `asyncBufferSize` | int | no | Size of internal buffer used when messages are asynchronously written to Kafka. | `100000` |
| `clientConfig` | [Client Config](#client-config) | yes | Kafka client-specific settings. | |
| `schemaRegistry` | [Schema Registry Config](#schema-registry-config) | no | Schema Registry settings. Currently only the [Confluent Schema Registry](https://github.com/confluentinc/schema-registry) is supported.  | |

### Client Config

Kafka client-specific configuration settings. Below is a list of the currently supported settings. See [librdkafka's docs](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) for further info on these settings.

#### Required

_One_ of the following settings must be specified:

- `metadata.broker.list`
- `bootstrap.servers`

#### Optional

- `client.id`
- `message.max.bytes`
- `message.copy.max.bytes`
- `receive.message.max.bytes`
- `max.in.flight.requests.per.connection`
- `max.in.flight`
- `topic.metadata.refresh.interval.ms`
- `metadata.max.age.ms`
- `topic.metadata.refresh.fast.interval.ms`
- `topic.metadata.refresh.sparse`
- `topic.metadata.propagation.max.ms`
- `topic.blacklist`
- `debug`
- `socket.timeout.ms`
- `socket.send.buffer.bytes`
- `socket.receive.buffer.bytes`
- `socket.keepalive.enable`
- `socket.nagle.disable`
- `socket.max.fails`
- `broker.address.ttl`
- `broker.address.family`
- `socket.connection.setup.timeout.ms`
- `connections.max.idle.ms`
- `reconnect.backoff.ms`
- `reconnect.backoff.max.ms`
- `statistics.interval.ms`
- `enabled_events`
- `log_level`
- `log.queue`
- `log.thread.name`
- `enable.random.seed`
- `log.connection.close`
- `internal.termination.signal`
- `api.version.request`
- `api.version.request.timeout.ms`
- `api.version.fallback.ms`
- `broker.version.fallback`
- `allow.auto.create.topics`
- `security.protocol`
- `ssl.cipher.suites`
- `ssl.curves.list`
- `ssl.sigalgs.list`
- `ssl.key.location`
- `ssl.key.password`
- `ssl.key.pem`
- `ssl.certificate.location`
- `ssl.certificate.pem`
- `ssl.ca.location`
- `ssl.ca.pem`
- `ssl.ca.certificate.stores`
- `ssl.crl.location`
- `ssl.keystore.location`
- `ssl.keystore.password`
- `ssl.providers`
- `ssl.engine.id`
- `enable.ssl.certificate.verification`
- `ssl.endpoint.identification.algorithm`
- `sasl.mechanisms`
- `sasl.mechanism`
- `sasl.kerberos.service.name`
- `sasl.kerberos.principal`
- `sasl.kerberos.kinit.cmd`
- `sasl.kerberos.keytab`
- `sasl.kerberos.min.time.before.relogin`
- `sasl.username`
- `sasl.password`
- `sasl.oauthbearer.config`
- `enable.sasl.oauthbearer.unsecure.jwt`
- `sasl.oauthbearer.method`
- `sasl.oauthbearer.client.id`
- `sasl.oauthbearer.client.secret`
- `sasl.oauthbearer.scope`
- `sasl.oauthbearer.extensions`
- `sasl.oauthbearer.token.endpoint.url`
- `plugin.library.paths`
- `client.rack`
- `queue.buffering.max.messages`
- `queue.buffering.max.kbytes`
- `queue.buffering.max.ms`
- `linger.ms`
- `message.send.max.retries`
- `retries`
- `retry.backoff.ms`
- `retry.backoff.max.ms`
- `queue.buffering.backpressure.threshold`
- `compression.codec`
- `compression.type`
- `batch.num.messages`
- `batch.size`
- `delivery.report.only.error`
- `sticky.partitioning.linger.ms`
- `client.dns.lookup`
- `request.required.acks`
- `acks`
- `request.timeout.ms`
- `message.timeout.ms`
- `delivery.timeout.ms`
- `partitioner`
- `compression.level`

### Schema Registry Config

| Name      | Type | Required | Description | Default |
| --------- | ---- | -------- | ----------- | ------- |
| `url` | string | yes | The URL of the schema registry. | |
| `basicAuthUsername` | string | no | Username for basic authorization. | |
| `basicAuthPassword` | string | no | Password for basic authorization. | |
| `bearerAuthToken` | string | no | Token for bearer authentication. | |
| `bearerAuthLogicalCluster` | string | no | The target SR logical cluster id for bearer authentication. It is required for Confluent Cloud Schema Registry. | |
| `bearerAuthIdentityPoolId` | string | no | The identity pool ID for bearer authentication. It is required for Confluent Cloud Schema Registry. | |
| `subjectNameStrategy` | [Subject Name Strategy](#subject=-name-strategy) | no | The method for determining the schema's subject name. | `TOPIC_NAME` |
| `keySchemaType` | [Schema Type](#schema-type) | no | The schema type used for message keys. | `NONE` |
| `valueSchemaType` | [Schema Type](#schema-type) | no | The schema type used for message values. | `NONE` |
| `validateJsonSchema` | bool | no | Whether to validate data against the schema when using the `JSON_SCHEMA` type. | `false` |
| `deterministicProtoSerializer` | bool | no | Whether to force deterministic output of Protobufs. Usually only used for testing. | `false` |

### Subject Name Strategy

| Value | Description |
| ----- | ----------- |
| `TOPIC_NAME` | Subject name derived as: `<topicName>-(key\|value)`. |
| `RECORD_NAME` | The value of the `schemaRecordName` field in the HTTP request is used as the subject name. |
| `TOPIC_RECORD_NAME` | A combination of the two strategies above. Subject name derived as: `<topicName>-<schemaRecordName>`. |

Refer to the following links for more info about subject name strategies:

- [Understanding Schema Subjects](https://developer.confluent.io/courses/schema-registry/schema-subjects/)
- [Serializers and Deserializers Overview: Subject name strategy](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#subject-name-strategy)

### Schema Type

| Value | Description |
| ----- | ----------- |
| `AVRO` | [Apache Avro](https://avro.apache.org/) format. |
| `JSON_SCHEMA` | [JSON Schema](https://json-schema.org/) format. |
| `PROTOBUF` | [Protocol Buffers](https://protobuf.dev/) format. |
| `NONE` | No serde/validation logic is performed. |

## Metrics Config

| Name      | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `enable` | [Enable Config](#enable-config) | no | Enable flags for metric types. |
| `otel` | [OpenTelemetry Config](#opentelemetry-config) | no | OpenTelemetry settings. |

### Enable Config

| Name      | Type | Required | Description | Default |
| --------- | ---- | -------- | ----------- | ------- |
| `all` | bool | no | Enable all metrics. If this is `true` then all other fields are ignored. | `false` |
| `endpoint` | bool | no | Enable endpoint metrics. | `false` |
| `host` | bool | no | Enable host metrics. | `false` |
| `http` | bool | no | Enable HTTP server metrics. | `false` |
| `producer` | bool | no | Enable Kafka producer-specific metrics. | `false` |
| `runtime` | bool | no | Enable Go runtime metrics. | `false` |

### OpenTelemetry Config

| Name      | Type | Required | Description | Default |
| --------- | ---- | -------- | ----------- | ------- |
| `endpoint` | string | yes | Endpoint address of the OpenTelemetry collector. | |
| `tls` | [TLS Config](#tls-config) | yes | TLS settings for the connection to the collector. | |
| `exportInterval` | [duration string](https://pkg.go.dev/time#ParseDuration) | no | How often to export metrics to the collector. | `5s` |

### TLS Config

| Name      | Type | Required | Description | Default |
| --------- | ---- | -------- | ----------- | ------- |
| `insecure` | bool | no | Whether to enable client transport security for the exporter's connection. | `false` |
| `caFile` | string | no | Path to a file containing the certificate authority cert. | |
| `caPem` | string | no | Alternative to `caFile`, provide the cert as a PEM-encoded string. | |
| `includeSystemCaCertsPool` | bool | no | Whether to load the system certificate authorities pool alongside the certificate authority. | `false` |
| `certFile` | string | no | Path to a file containing the TLS cert to use for TLS required connections. | |
| `certPem` | string | no | Alternative to `certFile`, provide the cert as a PEM-encoded string. | |
| `keyFile` | string | no | Path to a file containing the TLS key to use for TLS required connections. | |
| `keyPem` | string | no | Alternative to `keyFile`, provide the cert as a PEM-encoded string. | |
| `minVersion` | string | no | Minimum acceptable TLS version. | |
| `maxVersion` | string | no | Maximum acceptable TLS version. | |
| `cipherSuites` | list of strings | no | List of cipher suites to use. | |
| `reloadInterval` | [duration string](https://pkg.go.dev/time#ParseDuration) | no | Duration after which the certificate will be reloaded. If not set, it will never be reloaded. | |
| `insecureSkipVerify` | bool | no | Whether to skip verifying the certificate or not. | `false` |
| `serverNameOverride` | string | no | Override the virtual host name of authority (e.g. :authority header field) in requests (typically used for testing). | |
