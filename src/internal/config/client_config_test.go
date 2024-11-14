package config

import (
	"strings"
	"testing"
	"time"

	confluentcfg "github.com/echo8/krp/internal/config/confluent"
	"github.com/echo8/krp/internal/config/franz"
	saramacfg "github.com/echo8/krp/internal/config/sarama"
	segmentcfg "github.com/echo8/krp/internal/config/segment"
	"github.com/rcrowley/go-metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/IBM/sarama"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	segment "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type franzTestOpt struct {
	optFunc any
	opt     kgo.Opt
}

func TestClientConfig(t *testing.T) {
	testcases := []struct {
		name  string
		input string
		want  any
	}{
		{
			name: "confluent client config",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: confluent
					clientConfig:
						client.id: foo
						metadata.broker.list: broker1
						bootstrap.servers: broker2
						message.max.bytes: 2000
						message.copy.max.bytes: 2000
						receive.message.max.bytes: 2000
						max.in.flight.requests.per.connection: 1
						max.in.flight: 1
						topic.metadata.refresh.interval.ms: 1
						metadata.max.age.ms: 1
						topic.metadata.refresh.fast.interval.ms: 1
						topic.metadata.refresh.sparse: true
						topic.metadata.propagation.max.ms: 1
						topic.blacklist: foo
						debug: foo
						socket.timeout.ms: 10
						socket.send.buffer.bytes: 1
						socket.receive.buffer.bytes: 1
						socket.keepalive.enable: true
						socket.nagle.disable: true
						socket.max.fails: 1
						broker.address.ttl: 1
						broker.address.family: any
						socket.connection.setup.timeout.ms: 1000
						connections.max.idle.ms: 1
						reconnect.backoff.ms: 1
						reconnect.backoff.max.ms: 1
						statistics.interval.ms: 1
						enabled_events: 1
						log_level: 1
						log.queue: true
						log.thread.name: true
						enable.random.seed: true
						log.connection.close: true
						internal.termination.signal: 1
						api.version.request: true
						api.version.request.timeout.ms: 1
						api.version.fallback.ms: 1
						broker.version.fallback: foo
						allow.auto.create.topics: true
						security.protocol: foo
						ssl.cipher.suites: foo
						ssl.curves.list: foo
						ssl.sigalgs.list: foo
						ssl.key.location: foo
						ssl.key.password: foo
						ssl.key.pem: foo
						ssl.certificate.location: foo
						ssl.certificate.pem: foo
						ssl.ca.location: foo
						ssl.ca.pem: foo
						ssl.ca.certificate.stores: foo
						ssl.crl.location: foo
						ssl.keystore.location: foo
						ssl.keystore.password: foo
						ssl.providers: foo
						ssl.engine.id: foo
						enable.ssl.certificate.verification: true
						ssl.endpoint.identification.algorithm: foo
						sasl.mechanisms: foo
						sasl.mechanism: foo
						sasl.kerberos.service.name: foo
						sasl.kerberos.principal: foo
						sasl.kerberos.kinit.cmd: foo
						sasl.kerberos.keytab: foo
						sasl.kerberos.min.time.before.relogin: 1
						sasl.username: foo
						sasl.password: foo
						sasl.oauthbearer.config: foo
						enable.sasl.oauthbearer.unsecure.jwt: true
						sasl.oauthbearer.method: foo
						sasl.oauthbearer.client.id: foo
						sasl.oauthbearer.client.secret: foo
						sasl.oauthbearer.scope: foo
						sasl.oauthbearer.extensions: foo
						sasl.oauthbearer.token.endpoint.url: foo
						plugin.library.paths: foo
						client.rack: foo
						queue.buffering.max.messages: 1
						queue.buffering.max.kbytes: 1
						queue.buffering.max.ms: 1
						linger.ms: 1
						message.send.max.retries: 1
						retries: 1
						retry.backoff.ms: 1
						retry.backoff.max.ms: 1
						queue.buffering.backpressure.threshold: 1
						compression.codec: gzip
						compression.type: gzip
						batch.num.messages: 1
						batch.size: 1
						delivery.report.only.error: true
						sticky.partitioning.linger.ms: 1
						client.dns.lookup: foo
						request.required.acks: all
						acks: all
						request.timeout.ms: 1
						message.timeout.ms: 1
						delivery.timeout.ms: 1
						partitioner: random
						compression.level: 1
			`,
			want: &kafka.ConfigMap{
				"client.id":                               "foo",
				"metadata.broker.list":                    "broker1",
				"bootstrap.servers":                       "broker2",
				"message.max.bytes":                       2000,
				"message.copy.max.bytes":                  2000,
				"receive.message.max.bytes":               2000,
				"max.in.flight.requests.per.connection":   1,
				"max.in.flight":                           1,
				"topic.metadata.refresh.interval.ms":      1,
				"metadata.max.age.ms":                     1,
				"topic.metadata.refresh.fast.interval.ms": 1,
				"topic.metadata.refresh.sparse":           true,
				"topic.metadata.propagation.max.ms":       1,
				"topic.blacklist":                         "foo",
				"debug":                                   "foo",
				"socket.timeout.ms":                       10,
				"socket.send.buffer.bytes":                1,
				"socket.receive.buffer.bytes":             1,
				"socket.keepalive.enable":                 true,
				"socket.nagle.disable":                    true,
				"socket.max.fails":                        1,
				"broker.address.ttl":                      1,
				"broker.address.family":                   "any",
				"socket.connection.setup.timeout.ms":      1000,
				"connections.max.idle.ms":                 1,
				"reconnect.backoff.ms":                    1,
				"reconnect.backoff.max.ms":                1,
				"statistics.interval.ms":                  1,
				"enabled_events":                          1,
				"log_level":                               1,
				"log.queue":                               true,
				"log.thread.name":                         true,
				"enable.random.seed":                      true,
				"log.connection.close":                    true,
				"internal.termination.signal":             1,
				"api.version.request":                     true,
				"api.version.request.timeout.ms":          1,
				"api.version.fallback.ms":                 1,
				"broker.version.fallback":                 "foo",
				"allow.auto.create.topics":                true,
				"security.protocol":                       "foo",
				"ssl.cipher.suites":                       "foo",
				"ssl.curves.list":                         "foo",
				"ssl.sigalgs.list":                        "foo",
				"ssl.key.location":                        "foo",
				"ssl.key.password":                        "foo",
				"ssl.key.pem":                             "foo",
				"ssl.certificate.location":                "foo",
				"ssl.certificate.pem":                     "foo",
				"ssl.ca.location":                         "foo",
				"ssl.ca.pem":                              "foo",
				"ssl.ca.certificate.stores":               "foo",
				"ssl.crl.location":                        "foo",
				"ssl.keystore.location":                   "foo",
				"ssl.keystore.password":                   "foo",
				"ssl.providers":                           "foo",
				"ssl.engine.id":                           "foo",
				"enable.ssl.certificate.verification":     true,
				"ssl.endpoint.identification.algorithm":   "foo",
				"sasl.mechanisms":                         "foo",
				"sasl.mechanism":                          "foo",
				"sasl.kerberos.service.name":              "foo",
				"sasl.kerberos.principal":                 "foo",
				"sasl.kerberos.kinit.cmd":                 "foo",
				"sasl.kerberos.keytab":                    "foo",
				"sasl.kerberos.min.time.before.relogin":   1,
				"sasl.username":                           "foo",
				"sasl.password":                           "foo",
				"sasl.oauthbearer.config":                 "foo",
				"enable.sasl.oauthbearer.unsecure.jwt":    true,
				"sasl.oauthbearer.method":                 "foo",
				"sasl.oauthbearer.client.id":              "foo",
				"sasl.oauthbearer.client.secret":          "foo",
				"sasl.oauthbearer.scope":                  "foo",
				"sasl.oauthbearer.extensions":             "foo",
				"sasl.oauthbearer.token.endpoint.url":     "foo",
				"plugin.library.paths":                    "foo",
				"client.rack":                             "foo",
				"queue.buffering.max.messages":            1,
				"queue.buffering.max.kbytes":              1,
				"queue.buffering.max.ms":                  1,
				"linger.ms":                               1,
				"message.send.max.retries":                1,
				"retries":                                 1,
				"retry.backoff.ms":                        1,
				"retry.backoff.max.ms":                    1,
				"queue.buffering.backpressure.threshold":  1,
				"compression.codec":                       "gzip",
				"compression.type":                        "gzip",
				"batch.num.messages":                      1,
				"batch.size":                              1,
				"delivery.report.only.error":              true,
				"sticky.partitioning.linger.ms":           1,
				"client.dns.lookup":                       "foo",
				"request.required.acks":                   "all",
				"acks":                                    "all",
				"request.timeout.ms":                      1,
				"message.timeout.ms":                      1,
				"delivery.timeout.ms":                     1,
				"partitioner":                             "random",
				"compression.level":                       1,
			},
		},
		{
			name: "sarama client config",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: sarama
					clientConfig:
						bootstrap.servers: broker1
						net.max.open.requests: 1
						net.dial.timeout: 1s
						net.read.timeout: 1s
						net.write.timeout: 1s
						net.resolve.canonical.bootstrap.servers: true
						net.sasl.enable: true
						net.sasl.mechanism: PLAIN
						net.sasl.version: 1
						net.sasl.handshake: true
						net.sasl.auth.identity: foo
						net.sasl.user: foo
						net.sasl.password: foo
						net.sasl.scram.authz.id: foo
						net.sasl.gss.api.auth.type: 1
						net.sasl.gss.api.key.tab.path: foo
						net.sasl.gss.api.ccache.path: foo
						net.sasl.gss.api.kerberos.config.path: foo
						net.sasl.gss.api.service.name: foo
						net.sasl.gss.api.username: foo
						net.sasl.gss.api.password: foo
						net.sasl.gss.api.realm: foo
						net.sasl.gss.api.disable.pafxfast: true
						net.keep.alive: 1s
						metadata.retry.max: 1
						metadata.retry.backoff: 1s
						metadata.refresh.frequency: 1s
						metadata.full: true
						metadata.timeout: 1s
						metadata.allow.auto.topic.creation: true
						producer.max.message.bytes: 1
						producer.required.acks: 1
						producer.timeout: 1s
						producer.compression: gzip
						producer.compression.level: 1
						producer.flush.bytes: 1
						producer.flush.messages: 1
						producer.flush.frequency: 1s
						producer.flush.max.messages: 1
						producer.retry.max: 1
						producer.retry.backoff: 1s
						client.id: foo
						rack.id: foo
						channel.buffer.size: 1
						api.version.request: true
						version: 2.1.0
					`,
			want: getSaramaConfig(),
		},
		{
			name: "segment client config",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: segment
					clientConfig:
						bootstrap.servers: broker1
						balancer: hash_crc
						max.attempts: 1
						write.backoff.min: 1s
						write.backoff.max: 2s
						batch.size: 1
						batch.bytes: 1
						batch.timeout: 1s
						read.timeout: 1s
						write.timeout: 1s
						required.acks: 1
						compression: gzip
						allow.auto.topic.creation: true
			`,
			want: &segment.Writer{
				Addr:                   segment.TCP("broker1"),
				Balancer:               &segment.CRC32Balancer{},
				MaxAttempts:            1,
				WriteBackoffMin:        1 * time.Second,
				WriteBackoffMax:        2 * time.Second,
				BatchSize:              1,
				BatchBytes:             1,
				BatchTimeout:           1 * time.Second,
				ReadTimeout:            1 * time.Second,
				WriteTimeout:           1 * time.Second,
				RequiredAcks:           segment.RequireOne,
				Compression:            segment.Gzip,
				AllowAutoTopicCreation: true,
			},
		},
		{
			name: "franz client config",
			input: `
			endpoints:
				foo:
					routes:
						- topic: topic1
							producer: alpha
			producers:
				alpha:
					type: franz
					clientConfig:
						bootstrap.servers: broker1
						topic.allow.auto.creation: true
						broker.read.bytes.max: 52428801
						broker.write.bytes.max: 1000015
						client.id: foo
						conn.idle.timeout: 1m
						topic.consider.missing.deleted.after: 10s
						dial.timeout: 2m
						versions.max: 2.1.0
						versions.min: 2.0.0
						metadata.age.max: 10m
						metadata.age.min: 5m
						request.retries: 10
						request.timeout.overhead: 3m
						request.retry.timeout: 4m
						software.name: test
						software.version: 0.1.0
						idempotent.write.disable: true
						buffered.bytes.max: 100000
						buffered.records.max: 1000
						producer.request.inflight.per.broker.max: 5
						producer.request.timeout: 6m
						producer.batch.bytes.max: 150000
						producer.linger: 5s
						record.delivery.timeout: 30s
						record.retries: 6
						topic.unknown.retries: 7
			`,
			want: []franzTestOpt{
				{
					optFunc: kgo.SeedBrokers,
					opt:     kgo.SeedBrokers("broker1"),
				},
				{
					optFunc: kgo.AllowAutoTopicCreation,
					opt:     kgo.AllowAutoTopicCreation(),
				},
				{
					optFunc: kgo.BrokerMaxReadBytes,
					opt:     kgo.BrokerMaxReadBytes(52428801),
				},
				{
					optFunc: kgo.BrokerMaxWriteBytes,
					opt:     kgo.BrokerMaxWriteBytes(1000015),
				},
				{
					optFunc: kgo.ClientID,
					opt:     kgo.ClientID("foo"),
				},
				{
					optFunc: kgo.ConnIdleTimeout,
					opt:     kgo.ConnIdleTimeout(1 * time.Minute),
				},
				{
					optFunc: kgo.ConsiderMissingTopicDeletedAfter,
					opt:     kgo.ConsiderMissingTopicDeletedAfter(10 * time.Second),
				},
				{
					optFunc: kgo.DialTimeout,
					opt:     kgo.DialTimeout(2 * time.Minute),
				},
				{
					optFunc: kgo.MaxVersions,
					opt:     kgo.MaxVersions(kversion.FromString("2.1.0")),
				},
				{
					optFunc: kgo.MinVersions,
					opt:     kgo.MinVersions(kversion.FromString("2.0.0")),
				},
				{
					optFunc: kgo.MetadataMaxAge,
					opt:     kgo.MetadataMaxAge(10 * time.Minute),
				},
				{
					optFunc: kgo.MetadataMinAge,
					opt:     kgo.MetadataMinAge(5 * time.Minute),
				},
				{
					optFunc: kgo.RequestRetries,
					opt:     kgo.RequestRetries(10),
				},
				{
					optFunc: kgo.RequestTimeoutOverhead,
					opt:     kgo.RequestTimeoutOverhead(3 * time.Minute),
				},
				{
					optFunc: kgo.RetryTimeout,
					opt:     kgo.RetryTimeout(4 * time.Minute),
				},
				{
					optFunc: kgo.SoftwareNameAndVersion,
					opt:     kgo.SoftwareNameAndVersion("test", "0.1.0"),
				},
				{
					optFunc: kgo.DisableIdempotentWrite,
					opt:     kgo.DisableIdempotentWrite(),
				},
				{
					optFunc: kgo.MaxBufferedBytes,
					opt:     kgo.MaxBufferedBytes(100000),
				},
				{
					optFunc: kgo.MaxBufferedRecords,
					opt:     kgo.MaxBufferedRecords(1000),
				},
				{
					optFunc: kgo.MaxProduceRequestsInflightPerBroker,
					opt:     kgo.MaxProduceRequestsInflightPerBroker(5),
				},
				{
					optFunc: kgo.ProduceRequestTimeout,
					opt:     kgo.ProduceRequestTimeout(6 * time.Minute),
				},
				{
					optFunc: kgo.ProducerBatchMaxBytes,
					opt:     kgo.ProducerBatchMaxBytes(150000),
				},
				{
					optFunc: kgo.ProducerLinger,
					opt:     kgo.ProducerLinger(5 * time.Second),
				},
				{
					optFunc: kgo.RecordDeliveryTimeout,
					opt:     kgo.RecordDeliveryTimeout(30 * time.Second),
				},
				{
					optFunc: kgo.RecordRetries,
					opt:     kgo.RecordRetries(6),
				},
				{
					optFunc: kgo.UnknownTopicRetries,
					opt:     kgo.UnknownTopicRetries(7),
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			noTabs := strings.ReplaceAll(tc.input, "\t", "  ")
			config, err := loadFromBytes([]byte(noTabs))
			require.NoError(t, err)
			for _, cfg := range config.Producers {
				switch cfg := cfg.(type) {
				case *confluentcfg.ProducerConfig:
					confluentConfig := cfg.ClientConfig.ToConfigMap()
					require.Equal(t, tc.want, confluentConfig)
				case *saramacfg.ProducerConfig:
					saramaConfig, err := cfg.ClientConfig.ToConfig(false)
					require.NoError(t, err)
					require.Equal(t, tc.want, saramaConfig)
				case *segmentcfg.ProducerConfig:
					segmentWriter, err := cfg.ClientConfig.ToWriter()
					require.NoError(t, err)
					require.Equal(t, tc.want, segmentWriter)
				case *franz.ProducerConfig:
					opts, err := cfg.ClientConfig.ToOpts()
					require.NoError(t, err)
					want := tc.want.([]franzTestOpt)
					wantOpts := make([]kgo.Opt, 0, len(want))
					for _, testOpt := range want {
						wantOpts = append(wantOpts, testOpt.opt)
					}
					client, err := kgo.NewClient(opts...)
					require.NoError(t, err)
					wantClient, err := kgo.NewClient(wantOpts...)
					require.NoError(t, err)
					for _, testOpt := range want {
						require.Equal(t, wantClient.OptValue(testOpt.optFunc), client.OptValue(testOpt.optFunc))
					}
				}
			}
		})
	}
}

func getSaramaConfig() *sarama.Config {
	saramaConfig := &sarama.Config{}
	saramaConfig.Net.MaxOpenRequests = 1
	saramaConfig.Net.DialTimeout = 1 * time.Second
	saramaConfig.Net.ReadTimeout = 1 * time.Second
	saramaConfig.Net.WriteTimeout = 1 * time.Second
	saramaConfig.Net.ResolveCanonicalBootstrapServers = true
	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	saramaConfig.Net.SASL.AuthIdentity = "foo"
	saramaConfig.Net.SASL.Handshake = true
	saramaConfig.Net.SASL.Version = 1
	saramaConfig.Net.SASL.User = "foo"
	saramaConfig.Net.SASL.Password = "foo"
	saramaConfig.Net.SASL.SCRAMAuthzID = "foo"
	saramaConfig.Net.SASL.GSSAPI = sarama.GSSAPIConfig{
		AuthType:           1,
		KeyTabPath:         "foo",
		CCachePath:         "foo",
		KerberosConfigPath: "foo",
		ServiceName:        "foo",
		Username:           "foo",
		Password:           "foo",
		Realm:              "foo",
		DisablePAFXFAST:    true,
	}
	saramaConfig.Net.KeepAlive = 1 * time.Second
	saramaConfig.Metadata.Retry.Max = 1
	saramaConfig.Metadata.Retry.Backoff = 1 * time.Second
	saramaConfig.Metadata.RefreshFrequency = 1 * time.Second
	saramaConfig.Metadata.Full = true
	saramaConfig.Metadata.Timeout = 1 * time.Second
	saramaConfig.Metadata.AllowAutoTopicCreation = true
	saramaConfig.Producer.MaxMessageBytes = 1
	saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	saramaConfig.Producer.Timeout = 1 * time.Second
	saramaConfig.Producer.Compression = sarama.CompressionGZIP
	saramaConfig.Producer.CompressionLevel = 1
	saramaConfig.Producer.Flush.Bytes = 1
	saramaConfig.Producer.Flush.MaxMessages = 1
	saramaConfig.Producer.Flush.Messages = 1
	saramaConfig.Producer.Flush.Frequency = 1 * time.Second
	saramaConfig.Producer.Retry.Max = 1
	saramaConfig.Producer.Retry.Backoff = 1 * time.Second
	saramaConfig.ClientID = "foo"
	saramaConfig.RackID = "foo"
	saramaConfig.ChannelBufferSize = 1
	saramaConfig.ApiVersionsRequest = true
	saramaConfig.Version = sarama.DefaultVersion
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.MetricRegistry = metrics.NewRegistry()
	return saramaConfig
}
