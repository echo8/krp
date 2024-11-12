---
sidebar_position: 4
---

# Environment Variables

Environment variable expansion is supported in all string fields in the config.

#### Example

```yaml
producers:
  alpha:
    clientConfig:
      bootstrap.servers: ${env:KAFKA_BOOTSTRAP}
      sasl.username: ${env:KAFKA_USERNAME}
      sasl.password: ${env:KAFKA_PASSWORD}
```
