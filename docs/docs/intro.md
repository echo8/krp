---
sidebar_position: 1
---

# Intro

Welcome to KRP's documentation! KRP is a REST service that makes writing data to [Apache Kafka](https://kafka.apache.org/) easy! This site will teach you how to install, configure and run KRP.

## Quickstart

Follow the steps below to see a quick demo of KRP!

### Prerequisites

#### Docker

You will need to have [Docker](https://www.docker.com/) installed to run the containers used in this quickstart.

#### Kafka

A local deployment of Kafka can be setup by running:

```bash
docker run -d \
  -p 9092:9092 \
  --name broker \
  apache/kafka:latest
```

A topic named `test-topic1` will also be needed to write data to. You can create it by running the following:

```bash
docker exec \
  --workdir /opt/kafka/bin/ \
  -it broker \
  sh kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic test-topic1
```

### Configuration

Next we will create a config file for KRP. Copy the following into a file called `config.yaml`:

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

### Running KRP

Now run KRP with the config you just created:

```bash
docker run -d \
  -p 8080:8080 \
  -v /path/to/config.yaml:/opt/app/config.yaml \
  --name krp \
  ghcr.io/echo8/krp:latest
```

### Sending data

Finally send some data to Kafka with KRP:

```bash
curl -X POST \
  -H 'Content-Type: application/json' \
  http://localhost:8080/messages -d '
{
  "messages": [
    {
      "value": {
        "string": "hello world"
      }
    }
  ]
}'
```

You can verify that your data was delivered to Kafka by first making sure that the above `curl` command receives the following response:

```json
{"results":[{"success":true}]}
```

and then actually reading the data from Kafka by running:

```bash
docker exec \
  --workdir /opt/kafka/bin/ \
  -it broker \
  sh kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic1 \
  --from-beginning
```
