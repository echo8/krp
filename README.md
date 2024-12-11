# KRP

A REST service that makes writing data to Apache Kafka easy.

## Documentation

Detailed documentation can be found at https://www.echo8.dev/krp

## Example

Copy the following into a file called `config.yaml`:

```yaml
endpoints:
  /messages:
    routes:
      - topic: test-topic1
        producer: alpha
producers:
  alpha:
    clientConfig:
      bootstrap.servers: localhost:9092
```

Run KRP like this:

```bash
krp -config path/to/config.yaml
```

Finally send some data to Kafka:

```bash
curl -X POST -H 'Content-Type: application/json' http://localhost:8080/messages -d '
{"messages": [{"value": {"string": "hello world"}}]}'
```
