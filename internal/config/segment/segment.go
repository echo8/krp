package segment

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"gopkg.in/yaml.v3"
)

type ProducerConfig struct {
	Type                 string
	ClientConfig         *ClientConfig `yaml:"clientConfig"`
	MetricsFlushDuration time.Duration `yaml:"metricsFlushDuration"`
}

func (c *ProducerConfig) Load(v any) error {
	bytes, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	cfg := &ProducerConfig{}
	type plain ProducerConfig
	if err := yaml.Unmarshal(bytes, (*plain)(cfg)); err != nil {
		return err
	}
	*c = *cfg
	return nil
}

type ClientConfig struct {
	Addr                         *string        `yaml:"bootstrap.servers"`
	Balancer                     *string        `yaml:"balancer"`
	MaxAttempts                  *int           `yaml:"max.attempts"`
	WriteBackoffMin              *time.Duration `yaml:"write.backoff.min"`
	WriteBackoffMax              *time.Duration `yaml:"write.backoff.max"`
	BatchSize                    *int           `yaml:"batch.size"`
	BatchBytes                   *int64         `yaml:"batch.bytes"`
	BatchTimeout                 *time.Duration `yaml:"batch.timeout"`
	ReadTimeout                  *time.Duration `yaml:"read.timeout"`
	WriteTimeout                 *time.Duration `yaml:"write.timeout"`
	RequiredAcks                 *string        `yaml:"required.acks"`
	Compression                  *string        `yaml:"compression"`
	AllowAutoTopicCreation       *bool          `yaml:"allow.auto.topic.creation"`
	TransportDialerTimeout       *time.Duration `yaml:"transport.dialer.timeout"`
	TransportDialerDeadline      *time.Time     `yaml:"transport.dialer.deadline"`
	TransportDialerLocalAddr     *string        `yaml:"transport.dialer.local.addr"`
	TransportDialerDualStack     *bool          `yaml:"transport.dialer.dual.stack"`
	TransportDialerFallbackDelay *time.Duration `yaml:"transport.dialer.fallback.delay"`
	TransportDialerKeepAlive     *time.Duration `yaml:"transport.dialer.keep.alive"`
	TransportSaslMechanism       *string        `yaml:"transport.sasl.mechanism"` // plain or scram
	TransportSaslUsername        *string        `yaml:"transport.sasl.username"`
	TransportSaslPassword        *string        `yaml:"transport.sasl.password"`
	TransportTlsEnable           *bool          `yaml:"transport.tls.enable"`
	TransportTlsSkipVerify       *bool          `yaml:"transport.tls.skip.verify"`
	TransportTlsCertFile         *string        `yaml:"transport.tls.cert.file"`
	TransportTlsKeyFile          *string        `yaml:"transport.tls.key.file"`
	TransportTlsCaFile           *string        `yaml:"transport.tls.ca.file"`
	TransportClientID            *string        `yaml:"transport.client.id"`
	TransportIdleTimeout         *time.Duration `yaml:"transport.idle.timeout"`
	TransportMetadataTTL         *time.Duration `yaml:"transport.metadata.ttl"`
}

func (clientConfig *ClientConfig) ToWriter() (*kafka.Writer, error) {
	writer := &kafka.Writer{}
	if clientConfig.Addr != nil {
		writer.Addr = kafka.TCP(strings.Split(*clientConfig.Addr, ",")...)
	}
	if clientConfig.Balancer != nil {
		switch *clientConfig.Balancer {
		case "hash_crc":
			writer.Balancer = &kafka.CRC32Balancer{}
		case "hash":
			writer.Balancer = &kafka.Hash{}
		case "murmur2":
			writer.Balancer = &kafka.Murmur2Balancer{}
		case "hash_reference":
			writer.Balancer = &kafka.ReferenceHash{}
		case "round_robin":
			writer.Balancer = &kafka.RoundRobin{}
		case "least_bytes":
			writer.Balancer = &kafka.LeastBytes{}
		default:
			return nil, fmt.Errorf("invalid config, unknown segment balancer: %s", *clientConfig.Balancer)
		}
	}
	if clientConfig.MaxAttempts != nil {
		writer.MaxAttempts = *clientConfig.MaxAttempts
	}
	if clientConfig.WriteBackoffMin != nil {
		writer.WriteBackoffMin = *clientConfig.WriteBackoffMin
	}
	if clientConfig.WriteBackoffMax != nil {
		writer.WriteBackoffMax = *clientConfig.WriteBackoffMax
	}
	if clientConfig.BatchSize != nil {
		writer.BatchSize = *clientConfig.BatchSize
	}
	if clientConfig.BatchBytes != nil {
		writer.BatchBytes = *clientConfig.BatchBytes
	}
	if clientConfig.BatchTimeout != nil {
		writer.BatchTimeout = *clientConfig.BatchTimeout
	}
	if clientConfig.ReadTimeout != nil {
		writer.ReadTimeout = *clientConfig.ReadTimeout
	}
	if clientConfig.WriteTimeout != nil {
		writer.WriteTimeout = *clientConfig.WriteTimeout
	}
	if clientConfig.RequiredAcks != nil {
		if *clientConfig.RequiredAcks == "all" {
			writer.RequiredAcks = kafka.RequireAll
		} else {
			i, err := strconv.Atoi(*clientConfig.RequiredAcks)
			if err != nil {
				return nil, err
			}
			writer.RequiredAcks = kafka.RequiredAcks(int16(i))
		}
	}
	if clientConfig.Compression != nil {
		switch *clientConfig.Compression {
		case "none":
		case "gzip":
			writer.Compression = kafka.Gzip
		case "snappy":
			writer.Compression = kafka.Snappy
		case "lz4":
			writer.Compression = kafka.Lz4
		case "zstd":
			writer.Compression = kafka.Zstd
		default:
			return nil, fmt.Errorf("invalid config, unknown segment compression codec: %s", *clientConfig.Compression)
		}
	}
	if clientConfig.AllowAutoTopicCreation != nil {
		writer.AllowAutoTopicCreation = *clientConfig.AllowAutoTopicCreation
	}
	kafkaDialer := kafka.DefaultDialer
	nonDefaultDialer := false
	if clientConfig.TransportDialerTimeout != nil {
		kafkaDialer.Timeout = *clientConfig.TransportDialerTimeout
		nonDefaultDialer = true
	}
	if clientConfig.TransportDialerDeadline != nil {
		kafkaDialer.Deadline = *clientConfig.TransportDialerDeadline
		nonDefaultDialer = true
	}
	if clientConfig.TransportDialerLocalAddr != nil {
		addr, err := net.ResolveIPAddr("ip", *clientConfig.TransportDialerLocalAddr)
		if err != nil {
			return nil, err
		}
		kafkaDialer.LocalAddr = addr
		nonDefaultDialer = true
	}
	if clientConfig.TransportDialerDualStack != nil {
		kafkaDialer.DualStack = *clientConfig.TransportDialerDualStack
		nonDefaultDialer = true
	}
	if clientConfig.TransportDialerFallbackDelay != nil {
		kafkaDialer.FallbackDelay = *clientConfig.TransportDialerFallbackDelay
		nonDefaultDialer = true
	}
	if clientConfig.TransportDialerKeepAlive != nil {
		kafkaDialer.KeepAlive = *clientConfig.TransportDialerKeepAlive
		nonDefaultDialer = true
	}
	dialer := (&net.Dialer{
		Timeout:       kafkaDialer.Timeout,
		Deadline:      kafkaDialer.Deadline,
		LocalAddr:     kafkaDialer.LocalAddr,
		DualStack:     kafkaDialer.DualStack,
		FallbackDelay: kafkaDialer.FallbackDelay,
		KeepAlive:     kafkaDialer.KeepAlive,
	})
	transport := &kafka.Transport{
		Dial: (&net.Dialer{
			Timeout:   3 * time.Second,
			DualStack: true,
		}).DialContext,
	}
	nonDefaultTransport := false
	if nonDefaultDialer {
		transport.Dial = dialer.DialContext
		nonDefaultTransport = true
	}
	if clientConfig.TransportSaslMechanism != nil {
		switch *clientConfig.TransportSaslMechanism {
		case "plain":
			transport.SASL = plain.Mechanism{Username: *clientConfig.TransportSaslUsername, Password: *clientConfig.TransportSaslPassword}
		case "scram":
			mechanism, err := scram.Mechanism(scram.SHA512, *clientConfig.TransportSaslUsername, *clientConfig.TransportSaslPassword)
			if err != nil {
				return nil, err
			}
			transport.SASL = mechanism
		default:
			return nil, fmt.Errorf("invalid config, unknown segment sasl mechanism: %s", *clientConfig.TransportSaslMechanism)
		}
		nonDefaultTransport = true
	}
	if clientConfig.TransportTlsEnable != nil && *clientConfig.TransportTlsEnable {
		tlsCfg := &tls.Config{}
		if clientConfig.TransportTlsSkipVerify != nil {
			tlsCfg.InsecureSkipVerify = *clientConfig.TransportTlsSkipVerify
			nonDefaultTransport = true
		}
		if clientConfig.TransportTlsCertFile != nil && clientConfig.TransportTlsKeyFile != nil && clientConfig.TransportTlsCaFile != nil {
			cert, err := tls.LoadX509KeyPair(*clientConfig.TransportTlsCertFile, *clientConfig.TransportTlsKeyFile)
			if err != nil {
				return nil, err
			}

			caCert, err := os.ReadFile(*clientConfig.TransportTlsCaFile)
			if err != nil {
				return nil, err
			}

			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			tlsCfg.Certificates = []tls.Certificate{cert}
			tlsCfg.RootCAs = caCertPool
			nonDefaultTransport = true
		}
		transport.TLS = tlsCfg
	}
	if clientConfig.TransportClientID != nil {
		transport.ClientID = *clientConfig.TransportClientID
		nonDefaultTransport = true
	}
	if clientConfig.TransportIdleTimeout != nil {
		transport.IdleTimeout = *clientConfig.TransportIdleTimeout
		nonDefaultTransport = true
	}
	if clientConfig.TransportMetadataTTL != nil {
		transport.MetadataTTL = *clientConfig.TransportMetadataTTL
		nonDefaultTransport = true
	}
	if nonDefaultTransport {
		writer.Transport = transport
	}
	return writer, nil
}
