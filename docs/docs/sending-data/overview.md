---
sidebar_position: 1
---

# Sending Data

Data is sent to KRP by using an HTTP POST request. The HTTP URL path should match one of the endpoint paths that you specify in your [config](../config/overview.md).

For example, given the following config:

```yaml
endpoints:
  /foo/messages:
    routes:
      - topic: test-topic1
        producer: alpha
  /bar/messages:
    routes:
      - topic: test-topic2
        producer: alpha
producers:
  alpha:
    clientConfig:
      bootstrap.servers: localhost:9092
```

You can send data to the `/foo/messages` endpoint like this (assuming KRP is running locally):

```bash
curl -X POST \
  -H 'Content-Type: application/json' \
  http://localhost:8080/foo/messages -d '{"messages":[{"value":{"string":"hello1"}}]}'
```

And the `/bar/messages` endpoint like this:

```bash
curl -X POST \
  -H 'Content-Type: application/json' \
  http://localhost:8080/bar/messages -d '{"messages":[{"value":{"string":"hello2"}}]}'
```

## Request Model

All endpoints support the same request model.

### Top-level fields

| Name | Type | Required | Description |
| ---- | ---- | -------- | ----------- |
| `messages` | list of [Message](#message)s | yes | List of Kafka messages to send. |

### Message

| Name | Type | Required | Description |
| ---- | ---- | -------- | ----------- |
| `key` | [Data](#data) | no | Kafka message key. |
| `value` | [Data](#data) | yes | Kafka message value. |
| `headers` | [Headers](#headers) | no | Kafka message headers. |
| `timestamp` | string | no | Kafka message timestamp. Must be specified in [RFC3339Nano format](https://pkg.go.dev/time#pkg-constants). |

### Data

| Name | Type | Required | Description |
| ---- | ---- | -------- | ----------- |
| `string` | string | yes if `bytes` is not specified | String data. |
| `bytes` | string | yes if `string` is not specified | Byte data specified as a base64-encoded string. |
| `schemaRecordName` | string | no | The record name of this data's schema (used in some [subject name strategies](../config/overview.md#subject-name-strategy)). |
| `schemaId` | int | no | The ID of this data's schema in the schema registry. |
| `schemaMetadata` | [Schema Metadata](#schema-metadata) | no | Metadata to filter by when looking up the latest version of a schema. |

### Headers

A JSON object with string values. Each key/value in the object will be converted to a Kafka header key/value.

### Schema Metadata

A JSON object with string values.

## Response

The format of a request's response depends on if the endpoint is [asynchronous or not](../config/overview.md#endpoint-config).

### Synchronous Response

For synchronous endpoints, the response is an HTTP 200 containing success results for each message sent.

#### Top-level fields

| Name | Type | Description |
| ---- | ---- | ----------- |
| `results` | list of [Result](#result)s | List of results for each sent message. Will always be in the same order as the messages were sent. |

#### Result

| Name | Type | Description |
| ---- | ---- | ----------- |
| `success` | bool | `true` if the message was successfully delivered to **all** routes. |

### Asynchronous Response

For asynchronous endpoints, the response is an HTTP 204.

### Errors

Validation errors that occur during processing of the request body or data schema will be returned as an HTTP 400 with a description of the error in the body. All other errors will result in an HTTP 500 with a generic error message in the body being returned.
