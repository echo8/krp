package integrationtest

import (
	"context"
	"encoding/base64"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/echo8/krp/model"
	"github.com/stretchr/testify/require"
)

func NewConsumer(ctx context.Context, t *testing.T, topic, port string) *kafka.Consumer {
	cfg := kafka.ConfigMap{
		"bootstrap.servers":        fmt.Sprint("localhost:", port),
		"group.id":                 "it-consumer",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
		"enable.auto.commit":       false,
	}
	consumer, err := kafka.NewConsumer(&cfg)
	require.NoError(t, err)
	err = consumer.Subscribe(topic, nil)
	require.NoError(t, err)
	return consumer
}

func CheckReceived(t *testing.T, consumer *kafka.Consumer, sent []model.ProduceMessage) {
	if len(sent) == 0 {
		kafkaMsg, err := consumer.ReadMessage(10 * time.Second)
		if err != nil && err.(kafka.Error).IsTimeout() {
			// success, didn't receive any message as expected
			return
		} else if err != nil && strings.HasPrefix(err.(kafka.Error).Error(), "Subscribed topic not available") {
			// success, topic hasn't been created yet because no messages have been sent (as expected)
			return
		}
		require.NoError(t, err, "got error when reading message from kafka")
		_, err = consumer.StoreMessage(kafkaMsg)
		require.NoError(t, err)
		_, err = consumer.Commit()
		require.NoError(t, err)
		t.Fatal("received a message when we were expecting to receive zero",
			kafkaMsg, "key:", string(kafkaMsg.Key), "value:", string(kafkaMsg.Value),
			"headers:", kafkaMsg.Headers)
	}
	received := make([]kafka.Message, 0, len(sent))
	for range sent {
		kafkaMsg, err := consumer.ReadMessage(10 * time.Second)
		if err != nil && err.(kafka.Error).IsTimeout() {
			t.Fatal("expected number of messages were not received before the timeout")
		}
		require.NoError(t, err, "got error when reading message from kafka")
		received = append(received, *kafkaMsg)
		_, err = consumer.StoreMessage(kafkaMsg)
		require.NoError(t, err)
	}
	_, err := consumer.Commit()
	require.NoError(t, err)
	for _, receivedMsg := range received {
		foundPos := -1
		for i, sentMsg := range sent {
			if sameMsg(t, sentMsg, receivedMsg) {
				foundPos = i
				break
			}
		}
		if foundPos > -1 {
			sent = slices.Delete(sent, foundPos, foundPos+1)
		}
	}
	if len(sent) > 0 {
		t.Fatal("failed to find sent messages. sent:", sent, "recieved:", received)
	}
}

func sameMsg(t *testing.T, sent model.ProduceMessage, received kafka.Message) bool {
	if sent.Key != nil && received.Key != nil {
		if sent.Key.String != nil {
			if string(received.Key) != *sent.Key.String {
				return false
			}
		} else if sent.Key.Bytes != nil {
			keyBytes, err := base64.StdEncoding.DecodeString(*sent.Key.Bytes)
			require.NoError(t, err)
			if slices.Compare(received.Key, keyBytes) != 0 {
				return false
			}
		}
	} else if (sent.Key != nil && received.Key == nil) ||
		(sent.Key == nil && received.Key != nil) {
		return false
	}
	if sent.Value != nil && received.Value != nil {
		if sent.Value.String != nil {
			if string(received.Value) != *sent.Value.String {
				return false
			}
		} else if sent.Value.Bytes != nil {
			valueBytes, err := base64.StdEncoding.DecodeString(*sent.Value.Bytes)
			require.NoError(t, err)
			if slices.Compare(received.Value, valueBytes) != 0 {
				return false
			}
		}
	} else if (sent.Value != nil && received.Value == nil) ||
		(sent.Value == nil && received.Value != nil) {
		return false
	}
	if sent.Headers != nil && received.Headers != nil {
		receivedHeaders := make(map[string]string, len(received.Headers))
		for _, h := range received.Headers {
			receivedHeaders[h.Key] = string(h.Value)
		}
		if !reflect.DeepEqual(sent.Headers, receivedHeaders) {
			return false
		}
	} else if (sent.Headers != nil && received.Headers == nil) ||
		(sent.Headers == nil && received.Headers != nil) {
		return false
	}
	if sent.Timestamp != nil {
		if sent.Timestamp.Compare(received.Timestamp) != 0 {
			return false
		}
	}
	return true
}
