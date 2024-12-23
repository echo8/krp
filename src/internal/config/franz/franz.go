package franz

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"os"
	"strings"
	"time"

	"github.com/creasty/defaults"
	"github.com/echo8/krp/internal/config/schemaregistry"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"go.step.sm/crypto/pemutil"
	"gopkg.in/yaml.v3"
)

type ProducerConfig struct {
	Type           string                 `default:"franz"`
	ClientConfig   *ClientConfig          `yaml:"clientConfig" validate:"required"`
	SchemaRegistry *schemaregistry.Config `yaml:"schemaRegistry"`
}

func (c *ProducerConfig) Load(v any) error {
	bytes, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	cfg := &ProducerConfig{}
	if err := defaults.Set(cfg); err != nil {
		return err
	}
	type plain ProducerConfig
	if err := yaml.Unmarshal(bytes, (*plain)(cfg)); err != nil {
		return err
	}
	*c = *cfg
	return nil
}

func (c *ProducerConfig) SchemaRegistryCfg() *schemaregistry.Config {
	return c.SchemaRegistry
}

type ClientConfig struct {
	AllowAutoTopicCreation              *bool             `yaml:"topic.allow.auto.creation"`
	BrokerMaxReadBytes                  *int32            `yaml:"broker.read.bytes.max"`
	BrokerMaxWriteBytes                 *int32            `yaml:"broker.write.bytes.max"`
	ClientID                            *string           `yaml:"client.id"`
	ConnIdleTimeout                     *time.Duration    `yaml:"conn.idle.timeout"`
	ConsiderMissingTopicDeletedAfter    *time.Duration    `yaml:"topic.consider.missing.deleted.after"`
	TlsEnable                           *bool             `yaml:"tls.enable"`
	TlsSkipVerify                       *bool             `yaml:"tls.skip.verify"`
	TlsCertFile                         *string           `yaml:"tls.cert.file"`
	TlsKeyFile                          *string           `yaml:"tls.key.file"`
	TlsKeyPassword                      *string           `yaml:"tls.key.password"`
	TlsCaFile                           *string           `yaml:"tls.ca.file"`
	DialTimeout                         *time.Duration    `yaml:"dial.timeout"`
	MaxVersions                         *string           `yaml:"versions.max"`
	MinVersions                         *string           `yaml:"versions.min"`
	MetadataMaxAge                      *time.Duration    `yaml:"metadata.age.max"`
	MetadataMinAge                      *time.Duration    `yaml:"metadata.age.min"`
	RequestRetries                      *int              `yaml:"request.retries"`
	RequestTimeoutOverhead              *time.Duration    `yaml:"request.timeout.overhead"`
	RetryTimeout                        *time.Duration    `yaml:"request.retry.timeout"`
	SaslEnable                          *bool             `yaml:"sasl.enable"`
	SaslMechanism                       *string           `yaml:"sasl.mechanism"`
	SaslPlainUsername                   *string           `yaml:"sasl.plain.username"`
	SaslPlainPassword                   *string           `yaml:"sasl.plain.password"`
	SaslPlainAuthorizationID            *string           `yaml:"sasl.plain.auth.id"`
	SaslOauthToken                      *string           `yaml:"sasl.oauth.token"`
	SaslOauthExtensions                 map[string]string `yaml:"sasl.oauth.extensions"`
	SaslOauthAuthorizationID            *string           `yaml:"sasl.oauth.auth.id"`
	SaslAwsAccessKey                    *string           `yaml:"sasl.aws.access.key"`
	SaslAwsSecretKey                    *string           `yaml:"sasl.aws.secret.key"`
	SaslAwsUserAgent                    *string           `yaml:"sasl.aws.user.agent"`
	SeedBrokers                         *string           `yaml:"bootstrap.servers" validate:"required"`
	SoftwareName                        *string           `yaml:"software.name"`
	SoftwareVersion                     *string           `yaml:"software.version"`
	DisableIdempotentWrite              *bool             `yaml:"idempotent.write.disable"`
	MaxBufferedBytes                    *int              `yaml:"buffered.bytes.max"`
	MaxBufferedRecords                  *int              `yaml:"buffered.records.max"`
	MaxProduceRequestsInflightPerBroker *int              `yaml:"producer.request.inflight.per.broker.max"`
	ProduceRequestTimeout               *time.Duration    `yaml:"producer.request.timeout"`
	ProducerBatchMaxBytes               *int32            `yaml:"producer.batch.bytes.max"`
	ProducerLinger                      *time.Duration    `yaml:"producer.linger"`
	RecordDeliveryTimeout               *time.Duration    `yaml:"record.delivery.timeout"`
	RecordRetries                       *int              `yaml:"record.retries"`
	UnknownTopicRetries                 *int              `yaml:"topic.unknown.retries"`
	RequiredAcks                        *string           `yaml:"required.acks"`
	RecordPartitioner                   *string           `yaml:"record.partitioner"`
	ProducerBatchCompression            *string           `yaml:"producer.batch.compression"`
}

func (c *ClientConfig) ToOpts() ([]kgo.Opt, error) {
	opts := make([]kgo.Opt, 0)
	if c.AllowAutoTopicCreation != nil && *c.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	if c.BrokerMaxReadBytes != nil {
		opts = append(opts, kgo.BrokerMaxReadBytes(*c.BrokerMaxReadBytes))
	}
	if c.BrokerMaxWriteBytes != nil {
		opts = append(opts, kgo.BrokerMaxWriteBytes(*c.BrokerMaxWriteBytes))
	}
	if c.ClientID != nil {
		opts = append(opts, kgo.ClientID(*c.ClientID))
	}
	if c.ConnIdleTimeout != nil {
		opts = append(opts, kgo.ConnIdleTimeout(*c.ConnIdleTimeout))
	}
	if c.ConsiderMissingTopicDeletedAfter != nil {
		opts = append(opts, kgo.ConsiderMissingTopicDeletedAfter(*c.ConsiderMissingTopicDeletedAfter))
	}
	if c.DialTimeout != nil {
		opts = append(opts, kgo.DialTimeout(*c.DialTimeout))
	}
	if c.MaxVersions != nil {
		opts = append(opts, kgo.MaxVersions(kversion.FromString(*c.MaxVersions)))
	}
	if c.MinVersions != nil {
		opts = append(opts, kgo.MinVersions(kversion.FromString(*c.MinVersions)))
	}
	if c.MetadataMaxAge != nil {
		opts = append(opts, kgo.MetadataMaxAge(*c.MetadataMaxAge))
	}
	if c.MetadataMinAge != nil {
		opts = append(opts, kgo.MetadataMinAge(*c.MetadataMinAge))
	}
	if c.RequestRetries != nil {
		opts = append(opts, kgo.RequestRetries(*c.RequestRetries))
	}
	if c.RequestTimeoutOverhead != nil {
		opts = append(opts, kgo.RequestTimeoutOverhead(*c.RequestTimeoutOverhead))
	}
	if c.RetryTimeout != nil {
		opts = append(opts, kgo.RetryTimeout(*c.RetryTimeout))
	}
	if c.SeedBrokers != nil {
		opts = append(opts, kgo.SeedBrokers(strings.Split(*c.SeedBrokers, ",")...))
	}
	if c.SoftwareName != nil && c.SoftwareVersion != nil {
		opts = append(opts, kgo.SoftwareNameAndVersion(*c.SoftwareName, *c.SoftwareVersion))
	}
	if c.DisableIdempotentWrite != nil && *c.DisableIdempotentWrite {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	if c.MaxBufferedBytes != nil {
		opts = append(opts, kgo.MaxBufferedBytes(*c.MaxBufferedBytes))
	}
	if c.MaxBufferedRecords != nil {
		opts = append(opts, kgo.MaxBufferedRecords(*c.MaxBufferedRecords))
	}
	if c.MaxProduceRequestsInflightPerBroker != nil {
		opts = append(opts, kgo.MaxProduceRequestsInflightPerBroker(*c.MaxProduceRequestsInflightPerBroker))
	}
	if c.ProduceRequestTimeout != nil {
		opts = append(opts, kgo.ProduceRequestTimeout(*c.ProduceRequestTimeout))
	}
	if c.ProducerBatchMaxBytes != nil {
		opts = append(opts, kgo.ProducerBatchMaxBytes(*c.ProducerBatchMaxBytes))
	}
	if c.ProducerLinger != nil {
		opts = append(opts, kgo.ProducerLinger(*c.ProducerLinger))
	}
	if c.RecordDeliveryTimeout != nil {
		opts = append(opts, kgo.RecordDeliveryTimeout(*c.RecordDeliveryTimeout))
	}
	if c.RecordRetries != nil {
		opts = append(opts, kgo.RecordRetries(*c.RecordRetries))
	}
	if c.UnknownTopicRetries != nil {
		opts = append(opts, kgo.UnknownTopicRetries(*c.UnknownTopicRetries))
	}
	if c.RequiredAcks != nil {
		switch *c.RequiredAcks {
		case "all":
			opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
		case "0":
			opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
		case "1":
			opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
		default:
			return nil, fmt.Errorf("invalid config, unknown required acks value: %v", *c.RequiredAcks)
		}
	}
	if c.RecordPartitioner != nil {
		switch *c.RecordPartitioner {
		case "RoundRobinPartitioner":
			opts = append(opts, kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
		case "LeastBackupPartitioner":
			opts = append(opts, kgo.RecordPartitioner(kgo.LeastBackupPartitioner()))
		case "StickyPartitioner":
			opts = append(opts, kgo.RecordPartitioner(kgo.StickyPartitioner()))
		case "StickyKeyPartitioner":
			opts = append(opts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
		case "SaramaCompatPartitioner":
			opts = append(opts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(kgo.SaramaCompatHasher(fnv32a))))
		case "ConsistentPartitioner":
			opts = append(opts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(kgo.SaramaHasher(crc32.ChecksumIEEE))))
		default:
			return nil, fmt.Errorf("invalid config, unknown record partitioner value: %v",
				*c.RecordPartitioner)
		}
	}
	if c.ProducerBatchCompression != nil {
		switch *c.ProducerBatchCompression {
		case "none":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
		case "gzip":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
		case "snappy":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
		case "lz4":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
		case "zstd":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
		default:
			return nil, fmt.Errorf("invalid config, unknown batch compression value: %v",
				*c.ProducerBatchCompression)
		}
	}
	if c.TlsEnable != nil && *c.TlsEnable {
		tlsCfg := &tls.Config{}
		if c.TlsSkipVerify != nil {
			tlsCfg.InsecureSkipVerify = *c.TlsSkipVerify
		}
		if c.TlsCertFile != nil && c.TlsKeyFile != nil && c.TlsCaFile != nil {
			keyBytes, err := os.ReadFile(*c.TlsKeyFile)
			if err != nil {
				return nil, err
			}

			pemOpts := make([]pemutil.Options, 0)
			if c.TlsKeyPassword != nil {
				pemOpts = append(pemOpts, pemutil.WithPassword([]byte(*c.TlsKeyPassword)))
			}
			parsedKey, err := pemutil.Parse(keyBytes, pemOpts...)
			if err != nil {
				return nil, err
			}

			keyPemBlock, err := pemutil.Serialize(parsedKey)
			if err != nil {
				return nil, err
			}

			keyPemBytes := pem.EncodeToMemory(keyPemBlock)

			certPemBytes, err := os.ReadFile(*c.TlsCertFile)
			if err != nil {
				return nil, err
			}

			cert, err := tls.X509KeyPair(certPemBytes, keyPemBytes)
			if err != nil {
				return nil, err
			}

			caCert, err := os.ReadFile(*c.TlsCaFile)
			if err != nil {
				return nil, err
			}

			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			tlsCfg.Certificates = []tls.Certificate{cert}
			tlsCfg.RootCAs = caCertPool
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	if c.SaslEnable != nil && *c.SaslEnable && c.SaslMechanism != nil {
		switch *c.SaslMechanism {
		case "plain":
			if c.SaslPlainUsername != nil && c.SaslPlainPassword != nil {
				var zid string
				if c.SaslPlainAuthorizationID != nil {
					zid = *c.SaslPlainAuthorizationID
				}
				opts = append(opts, kgo.SASL(plain.Auth{
					Zid:  zid,
					User: *c.SaslPlainUsername,
					Pass: *c.SaslPlainPassword,
				}.AsMechanism()))
			}
		case "oauth":
			if c.SaslOauthToken != nil {
				var zid string
				if c.SaslPlainAuthorizationID != nil {
					zid = *c.SaslPlainAuthorizationID
				}
				opts = append(opts, kgo.SASL(oauth.Auth{
					Zid:        zid,
					Token:      *c.SaslOauthToken,
					Extensions: c.SaslOauthExtensions,
				}.AsMechanism()))
			}
		case "aws":
			if c.SaslAwsAccessKey != nil && c.SaslAwsSecretKey != nil {
				var userAgent string
				if c.SaslAwsUserAgent != nil {
					userAgent = *c.SaslAwsUserAgent
				}
				opts = append(opts, kgo.SASL(aws.Auth{
					AccessKey: *c.SaslAwsAccessKey,
					SecretKey: *c.SaslAwsSecretKey,
					UserAgent: userAgent,
				}.AsManagedStreamingIAMMechanism()))
			}
		default:
			return nil, fmt.Errorf("invalid config, unknown sasl mechanism value: %v",
				*c.SaslMechanism)
		}
	}
	return opts, nil
}

func fnv32a(b []byte) uint32 {
	h := fnv.New32a()
	h.Reset()
	h.Write(b)
	return h.Sum32()
}
