---
sidebar_position: 2
---

# Route Matchers

For each route under an endpoint, KRP must determine if each incoming message matches it (and thus be sent down that route) or not. It does that using route matchers. A route matcher is defined using the [Expr](https://expr-lang.org/docs/language-definition) expression language and is specified in the `match` field under each route.

## No Route Matcher

In the simplest case, a route matcher is not defined:

```yaml
routes:
  - topic: test-topic1
    producer: alpha
```

This route will match **all** incoming messages.

## Matching Multiple Routes

Incoming messages can match multiple routes. In the following example, all incoming messages will be sent to both `test-topic1` and `test-topic2`:

```yaml
routes:
  - topic: test-topic1
    producer: alpha
  - topic: test-topic2
    producer: alpha
```

## Matching Messages

All fields in a message's [request model](../sending-data/overview.md#message) are available to the matcher.

#### Example: Key (String)

```yaml
routes:
  - match: "message.key.string == 'foo'"
    topic: test-topic1
    producer: alpha
```

#### Example: Value (String)

```yaml
routes:
  - match: "message.value.string == 'bar'"
    topic: test-topic1
    producer: alpha
```

#### Example: Header

```yaml
routes:
  - match: "message.headers['foo'] == 'bar'"
    topic: test-topic1
    producer: alpha
```

## Matching HTTP Headers

HTTP request headers are also available to the matcher. Header keys must be specified in [canonical format](https://pkg.go.dev/net/http#CanonicalHeaderKey).

#### Example: HTTP Header

```yaml
routes:
  - match: "httpHeader('Foo') == 'bar'"
    topic: test-topic1
    producer: alpha
```
