---
sidebar_position: 1
---

# Metrics

KRP can be [configured](../config/overview.md#metrics-config) to export the following metrics.

## Endpoint

| Name | Type | Description |
| ---- | ---- | ----------- |
| `krp.endpoint.request.size` | histogram | Measures the size of [request objects](../sending-data/overview.md#request-model) attributed by endpoint path. |
| `krp.endpoint.message.size` | histogram | Measures the size of [message objects](../sending-data/overview.md#message) attributed by endpoint path. |
| `krp.endpoint.message.produced` | count | Count of messages produced to Kafka attributed by endpoint path and success result. |
| `krp.endpoint.message.unmatched` | count | Count of messages that were received but did not match any of an endpoint's routes attributed by endpoint path. |

## Host

| Name | Type | Description |
| ---- | ---- | ----------- |
| `process.cpu.time` | counter | Accumulated CPU time spent by this process attributed by state (User, System, ...). |
| `system.cpu.time` | counter | Accumulated CPU time spent by this host attributed by state (User, System, Other, Idle). |
| `system.memory.usage` | gauge | Memory usage of this process attributed by memory state (Used, Available). |
| `system.memory.utilization` | gauge | Memory utilization of this process attributed by memory state (Used, Available). |
| `system.network.io` | counter | Bytes transferred attributed by direction (Transmit, Receive). |

## HTTP Server

| Name | Type | Description |
| ---- | ---- | ----------- |
| `krp.http.request.size` | histogram | Measures the size of HTTP requests. |
| `krp.http.response.size` | histogram | Measures the size of HTTP responses. |
| `krp.http.latency` | histogram | Measures the duration of inbound HTTP requests. |

## Kafka Producer

Kafka client-specific metrics. See [librdkafka's docs](https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md) for details.

## Go Runtime

| Name | Type | Description |
| ---- | ---- | ----------- |
| `go.memory.used` | up/down counter | Memory used by the Go runtime. |
| `go.memory.limit` | up/down counter | Go runtime memory limit configured by the user, if a limit exists. |
| `go.memory.allocated` | counter | Memory allocated to the heap by the application. |
| `go.memory.allocations` | counter | Count of allocations to the heap by the application. |
| `go.memory.gc.goal` | up/down counter | Heap size target for the end of the GC cycle. |
| `go.goroutine.count` | up/down counter | Count of live goroutines. |
| `go.processor.limit` | up/down counter | The number of OS threads that can execute user-level Go code simultaneously. |
| `go.config.gogc` | up/down counter | Heap size target percentage configured by the user, otherwise 100. |
