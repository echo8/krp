package e2e

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type StdoutLogConsumer struct{}

func (lc *StdoutLogConsumer) Accept(l testcontainers.Log) {
	fmt.Print(string(l.Content))
}

func NewKrpContainer(ctx context.Context, network string, cfg string, files ...testcontainers.ContainerFile) (testcontainers.Container, error) {
	g := StdoutLogConsumer{}
	rootDir := ProjectRootDir()
	var buildArgs map[string]*string
	if len(cfg) > 0 {
		tmpCfg, err := os.CreateTemp(rootDir, "it-cfg*.yaml")
		if err != nil {
			return nil, err
		}
		defer os.Remove(tmpCfg.Name())
		_, err = io.WriteString(tmpCfg, cfg)
		if err != nil {
			return nil, err
		}
		err = tmpCfg.Close()
		if err != nil {
			return nil, err
		}
		buildArgs = make(map[string]*string, 1)
		_, fn := filepath.Split(tmpCfg.Name())
		buildArgs["config_path"] = Ptr(fmt.Sprint("./", fn))
	}
	req := testcontainers.ContainerRequest{
		Name: "krp-it",
		FromDockerfile: testcontainers.FromDockerfile{
			Context:       rootDir,
			Dockerfile:    "local/Dockerfile",
			PrintBuildLog: true,
			Repo:          "krp",
			Tag:           "test",
			BuildArgs:     buildArgs,
		},
		Networks:     []string{network},
		ExposedPorts: []string{"8080/tcp"},
		Cmd: []string{
			"/bin/bash",
			"-c",
			`echo "Running with config:" &&
cat /opt/app/config.yaml &&
/opt/app/krp /opt/app/config.yaml`,
		},
		Files: files,
		WaitingFor: wait.ForHTTP("http://localhost:8080/healthcheck").WithStatusCodeMatcher(func(status int) bool {
			return status == 204
		}),
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(10 * time.Second)},
			Consumers: []testcontainers.LogConsumer{&g},
		},
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func NewKafkaContainer(ctx context.Context, name, port, network string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Name:  fmt.Sprintf("kafka-broker-%s-it", name),
		Image: "apache/kafka:3.8.0",
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                fmt.Sprintf("PLAINTEXT://%s:9092,CONTROLLER://localhost:9093,PLAINTEXT_HOST://0.0.0.0:%s", name, port),
			"KAFKA_ADVERTISED_LISTENERS":                     fmt.Sprintf("PLAINTEXT://%s:9092,PLAINTEXT_HOST://localhost:%s", name, port),
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_NUM_PARTITIONS":                           "3",
		},
		ExposedPorts: []string{fmt.Sprintf("%s:%s/tcp", port, port)},
		Networks:     []string{network},
		NetworkAliases: map[string][]string{
			network: {name},
		},
		WaitingFor: wait.ForLog("Kafka Server started"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func NewKafkaSSLContainer(ctx context.Context, name, port, network string) (testcontainers.Container, error) {
	g := StdoutLogConsumer{}
	req := testcontainers.ContainerRequest{
		Name:  fmt.Sprintf("kafka-broker-ssl-%s-it", name),
		Image: "apache/kafka:3.8.0",
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                fmt.Sprintf("SSL://%s:9092,CONTROLLER://localhost:9093,PLAINTEXT://0.0.0.0:%s", name, port),
			"KAFKA_ADVERTISED_LISTENERS":                     fmt.Sprintf("SSL://%s:9092,PLAINTEXT://localhost:%s", name, port),
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_NUM_PARTITIONS":                           "3",
			"KAFKA_SSL_CLIENT_AUTH":                          "required",
			"KAFKA_SSL_KEYSTORE_FILENAME":                    "kafka.server.keystore.jks",
			"KAFKA_SSL_KEYSTORE_CREDENTIALS":                 "kafka_keystore_creds",
			"KAFKA_SSL_KEY_CREDENTIALS":                      "kafka_sslkey_creds",
			"KAFKA_SSL_TRUSTSTORE_FILENAME":                  "kafka.server.truststore.jks",
			"KAFKA_SSL_TRUSTSTORE_CREDENTIALS":               "kafka_truststore_creds",
		},
		User: "root",
		Cmd: []string{
			"/bin/bash",
			"-c",
			`apk update &&

apk add openssl &&

cd /etc/kafka/secrets &&

keytool -keystore kafka.server.keystore.jks -alias localhost -keyalg RSA -validity 1 -genkey -storepass test1234 -keypass test1234 -dname "CN=broker, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown" -ext SAN=DNS:broker &&

openssl req -new -x509 -keyout ca-key -out ca-cert -days 1 -nodes -subj "/C=NA/ST=Unknown/L=Unknown/O=Unknown/CN=broker" -addext "subjectAltName = DNS:broker" &&

keytool -keystore kafka.client.truststore.jks -alias CARoot -importcert -file ca-cert -noprompt -storepass test1234 -keypass test1234 -dname "CN=broker, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown" &&

keytool -keystore kafka.server.truststore.jks -alias CARoot -importcert -file ca-cert -noprompt -storepass test1234 -keypass test1234 -dname "CN=broker, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown" &&

keytool -keystore kafka.server.keystore.jks -alias localhost -certreq -file cert-file -storepass test1234 -keypass test1234 &&

openssl x509 -req -extfile <(printf "subjectAltName=DNS:broker") -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 1 -CAcreateserial -passin pass:test1234 &&

keytool -keystore kafka.server.keystore.jks -alias CARoot -importcert -file ca-cert -noprompt -storepass test1234 -keypass test1234 &&

keytool -keystore kafka.server.keystore.jks -alias localhost -importcert -file cert-signed -storepass test1234 -keypass test1234 &&

keytool -keystore kafka.client.keystore.jks -alias localhost -keyalg RSA -validity 1 -genkey -storepass test1234 -keypass test1234 -dname "CN=broker, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown" -ext SAN=DNS:broker &&

keytool -keystore kafka.client.keystore.jks -alias localhost -certreq -file client-cert-file -storepass test1234 -keypass test1234 &&

openssl x509 -req -extfile <(printf "subjectAltName=DNS:broker") -CA ca-cert -CAkey ca-key -in client-cert-file -out client-cert-signed -days 1 -CAcreateserial -passin pass:test1234 &&

keytool -keystore kafka.client.keystore.jks -alias CARoot -importcert -file ca-cert -noprompt -storepass test1234 -keypass test1234 &&

keytool -keystore kafka.client.keystore.jks -alias localhost -importcert -file client-cert-signed -storepass test1234 -keypass test1234 &&

openssl genrsa -des3 -passout "pass:test1234" -out client.key 2048 &&

openssl req -passin "pass:test1234" -passout "pass:test1234" -key client.key -new -out client.req -subj "/C=NA/ST=Unknown/L=Unknown/O=Unknown/CN=broker" -addext "subjectAltName = DNS:broker" &&

openssl x509 -req -extfile <(printf "subjectAltName=DNS:broker") -passin "pass:test1234" -in client.req -CA ca-cert -CAkey ca-key -CAcreateserial -out client.pem -days 1 &&

echo "test1234" > kafka_keystore_creds &&

echo "test1234" > kafka_sslkey_creds &&

echo "test1234" > kafka_truststore_creds &&

/etc/kafka/docker/run`,
		},
		ExposedPorts: []string{fmt.Sprintf("%s:%s/tcp", port, port)},
		Networks:     []string{network},
		NetworkAliases: map[string][]string{
			network: {name},
		},
		WaitingFor: wait.ForLog("Kafka Server started"),
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(10 * time.Second)},
			Consumers: []testcontainers.LogConsumer{&g},
		},
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func NewKafkaSASLPlainContainer(ctx context.Context, name, port, network string) (testcontainers.Container, error) {
	g := StdoutLogConsumer{}
	req := testcontainers.ContainerRequest{
		Name:  fmt.Sprintf("kafka-broker-sasl-plain-%s-it", name),
		Image: "apache/kafka:3.8.0",
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                fmt.Sprintf("SASL_PLAINTEXT://%s:9092,CONTROLLER://localhost:9093,PLAINTEXT_HOST://0.0.0.0:%s", name, port),
			"KAFKA_ADVERTISED_LISTENERS":                     fmt.Sprintf("SASL_PLAINTEXT://%s:9092,PLAINTEXT_HOST://localhost:%s", name, port),
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_NUM_PARTITIONS":                           "3",
			"KAFKA_SECURITY_INTER_BROKER_PROTOCOL":           "SASL_PLAINTEXT",
			"KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL":     "PLAIN",
			"KAFKA_SASL_ENABLED_MECHANISMS":                  "PLAIN",
			"KAFKA_OPTS":                                     "-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_jaas.conf",
		},
		Cmd: []string{
			"/bin/bash",
			"-c",
			`cd /etc/kafka/secrets &&

echo 'KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="admin"
    password="admin-secret"
    user_test="test1234";
};' > kafka_jaas.conf

/etc/kafka/docker/run`},
		ExposedPorts: []string{fmt.Sprintf("%s:%s/tcp", port, port)},
		Networks:     []string{network},
		NetworkAliases: map[string][]string{
			network: {name},
		},
		WaitingFor: wait.ForLog("Kafka Server started"),
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(10 * time.Second)},
			Consumers: []testcontainers.LogConsumer{&g},
		},
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func CopyFromContainer(ctx context.Context, t *testing.T, container testcontainers.Container, path, name string) *os.File {
	containerFile, err := container.CopyFileFromContainer(ctx, path)
	require.NoError(t, err)
	rootDir := ProjectRootDir()
	tmpFile, err := os.CreateTemp(rootDir, name)
	require.NoError(t, err)
	_, err = io.Copy(tmpFile, containerFile)
	require.NoError(t, err)
	err = containerFile.Close()
	require.NoError(t, err)
	err = tmpFile.Close()
	require.NoError(t, err)
	return tmpFile
}
