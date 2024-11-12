---
sidebar_position: 3
---

# Templating

Some fields support template variables which are expanded for each incoming message.

## Message Variables

#### Example: Key (String)

```yaml
routes:
  - topic: test-topic1-${msg:key}
    producer: alpha
```

:::warning

In this case make sure you use some kind of random Kafka partioner, otherwise all of your messages will be written to the same partition.

:::

#### Example: Header

```yaml
routes:
  - topic: test-topic1-${msg:header.foo}
    producer: alpha
```
