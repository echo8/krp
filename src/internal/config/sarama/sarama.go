package sarama

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/echo8/krp/internal/config/schemaregistry"
	"go.step.sm/crypto/pemutil"

	kafka "github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/yaml.v3"
)

type ProducerConfig struct {
	Type                 string
	ClientConfig         *ClientConfig          `yaml:"clientConfig"`
	MetricsFlushDuration time.Duration          `yaml:"metricsFlushDuration"`
	SchemaRegistry       *schemaregistry.Config `yaml:"schemaRegistry"`
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
	cfg.ClientConfig.MetricRegistry = metrics.NewRegistry()
	*c = *cfg
	return nil
}

func (c *ProducerConfig) SchemaRegistryCfg() *schemaregistry.Config {
	return c.SchemaRegistry
}

type ClientConfig struct {
	BootstrapServers                    *string          `yaml:"bootstrap.servers"`
	NetMaxOpenRequests                  *int             `yaml:"net.max.open.requests"`
	NetDialTimeout                      *time.Duration   `yaml:"net.dial.timeout"`
	NetReadTimeout                      *time.Duration   `yaml:"net.read.timeout"`
	NetWriteTimeout                     *time.Duration   `yaml:"net.write.timeout"`
	NetResolveCanonicalBootstrapServers *bool            `yaml:"net.resolve.canonical.bootstrap.servers"`
	NetTlsEnable                        *bool            `yaml:"net.tls.enable"`
	NetTlsSkipVerify                    *bool            `yaml:"net.tls.skip.verify"`
	NetTlsCertFile                      *string          `yaml:"net.tls.cert.file"`
	NetTlsKeyFile                       *string          `yaml:"net.tls.key.file"`
	NetTlsKeyPassword                   *string          `yaml:"net.tls.key.password"`
	NetTlsCaFile                        *string          `yaml:"net.tls.ca.file"`
	NetSaslEnable                       *bool            `yaml:"net.sasl.enable"`
	NetSaslMechanism                    *string          `yaml:"net.sasl.mechanism"`
	NetSaslVersion                      *int16           `yaml:"net.sasl.version"`
	NetSaslHandshake                    *bool            `yaml:"net.sasl.handshake"`
	NetSaslAuthIdentity                 *string          `yaml:"net.sasl.auth.identity"`
	NetSaslUser                         *string          `yaml:"net.sasl.user"`
	NetSaslPassword                     *string          `yaml:"net.sasl.password"`
	NetSaslScramAuthzId                 *string          `yaml:"net.sasl.scram.authz.id"`
	NetSaslGssApiAuthType               *int             `yaml:"net.sasl.gss.api.auth.type"`
	NetSaslGssApiKeyTabPath             *string          `yaml:"net.sasl.gss.api.key.tab.path"`
	NetSaslGssApiCCachePath             *string          `yaml:"net.sasl.gss.api.ccache.path"`
	NetSaslGssApiKerberosConfigPath     *string          `yaml:"net.sasl.gss.api.kerberos.config.path"`
	NetSaslGssApiServiceName            *string          `yaml:"net.sasl.gss.api.service.name"`
	NetSaslGssApiUsername               *string          `yaml:"net.sasl.gss.api.username"`
	NetSaslGssApiPassword               *string          `yaml:"net.sasl.gss.api.password"`
	NetSaslGssApiRealm                  *string          `yaml:"net.sasl.gss.api.realm"`
	NetSaslGssApiDisablePAFXFAST        *bool            `yaml:"net.sasl.gss.api.disable.pafxfast"`
	NetKeepAlive                        *time.Duration   `yaml:"net.keep.alive"`
	NetLocalAddr                        *string          `yaml:"net.local.addr"`
	MetadataRetryMax                    *int             `yaml:"metadata.retry.max"`
	MetadataRetryBackoff                *time.Duration   `yaml:"metadata.retry.backoff"`
	MetadataRefreshFrequency            *time.Duration   `yaml:"metadata.refresh.frequency"`
	MetadataFull                        *bool            `yaml:"metadata.full"`
	MetadataTimeout                     *time.Duration   `yaml:"metadata.timeout"`
	MetadataAllowAutoTopicCreation      *bool            `yaml:"metadata.allow.auto.topic.creation"`
	ProducerMaxMessageBytes             *int             `yaml:"producer.max.message.bytes"`
	ProducerRequiredAcks                *string          `yaml:"producer.required.acks"`
	ProducerTimeout                     *time.Duration   `yaml:"producer.timeout"`
	ProducerCompression                 *string          `yaml:"producer.compression"`
	ProducerCompressionLevel            *int             `yaml:"producer.compression.level"`
	ProducerPartitioner                 *string          `yaml:"producer.partitioner"`
	ProducerFlushBytes                  *int             `yaml:"producer.flush.bytes"`
	ProducerFlushMessages               *int             `yaml:"producer.flush.messages"`
	ProducerFlushFrequency              *time.Duration   `yaml:"producer.flush.frequency"`
	ProducerFlushMaxMessages            *int             `yaml:"producer.flush.max.messages"`
	ProducerRetryMax                    *int             `yaml:"producer.retry.max"`
	ProducerRetryBackoff                *time.Duration   `yaml:"producer.retry.backoff"`
	ClientID                            *string          `yaml:"client.id"`
	RackID                              *string          `yaml:"rack.id"`
	ChannelBufferSize                   *int             `yaml:"channel.buffer.size"`
	ApiVersionsRequest                  *bool            `yaml:"api.version.request"`
	Version                             *string          `yaml:"version"`
	MetricRegistry                      metrics.Registry `yaml:"-"`
}

func (clientConfig *ClientConfig) ToConfig(includeDefaults bool) (*kafka.Config, error) {
	var cfg *kafka.Config
	if includeDefaults {
		cfg = kafka.NewConfig()
	} else {
		cfg = &kafka.Config{}
	}
	if clientConfig.NetMaxOpenRequests != nil {
		cfg.Net.MaxOpenRequests = *clientConfig.NetMaxOpenRequests
	}
	if clientConfig.NetDialTimeout != nil {
		cfg.Net.DialTimeout = *clientConfig.NetDialTimeout
	}
	if clientConfig.NetReadTimeout != nil {
		cfg.Net.ReadTimeout = *clientConfig.NetReadTimeout
	}
	if clientConfig.NetWriteTimeout != nil {
		cfg.Net.WriteTimeout = *clientConfig.NetWriteTimeout
	}
	if clientConfig.NetResolveCanonicalBootstrapServers != nil {
		cfg.Net.ResolveCanonicalBootstrapServers = *clientConfig.NetResolveCanonicalBootstrapServers
	}
	if clientConfig.NetTlsEnable != nil && *clientConfig.NetTlsEnable {
		tlsCfg := &tls.Config{}
		if clientConfig.NetTlsSkipVerify != nil {
			tlsCfg.InsecureSkipVerify = *clientConfig.NetTlsSkipVerify
		}
		if clientConfig.NetTlsCertFile != nil && clientConfig.NetTlsKeyFile != nil && clientConfig.NetTlsCaFile != nil {
			keyBytes, err := os.ReadFile(*clientConfig.NetTlsKeyFile)
			if err != nil {
				return nil, err
			}

			pemOpts := make([]pemutil.Options, 0)
			if clientConfig.NetTlsKeyPassword != nil {
				pemOpts = append(pemOpts, pemutil.WithPassword([]byte(*clientConfig.NetTlsKeyPassword)))
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
			if err != nil {
				return nil, err
			}

			certPemBytes, err := os.ReadFile(*clientConfig.NetTlsCertFile)
			if err != nil {
				return nil, err
			}

			cert, err := tls.X509KeyPair(certPemBytes, keyPemBytes)
			if err != nil {
				return nil, err
			}

			caCert, err := os.ReadFile(*clientConfig.NetTlsCaFile)
			if err != nil {
				return nil, err
			}

			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			tlsCfg.Certificates = []tls.Certificate{cert}
			tlsCfg.RootCAs = caCertPool
		}
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsCfg
	}
	if clientConfig.NetSaslEnable != nil {
		cfg.Net.SASL.Enable = *clientConfig.NetSaslEnable
	}
	if clientConfig.NetSaslMechanism != nil {
		cfg.Net.SASL.Mechanism = kafka.SASLMechanism(*clientConfig.NetSaslMechanism)
	}
	if clientConfig.NetSaslVersion != nil {
		cfg.Net.SASL.Version = *clientConfig.NetSaslVersion
	}
	if clientConfig.NetSaslHandshake != nil {
		cfg.Net.SASL.Handshake = *clientConfig.NetSaslHandshake
	}
	if clientConfig.NetSaslAuthIdentity != nil {
		cfg.Net.SASL.AuthIdentity = *clientConfig.NetSaslAuthIdentity
	}
	if clientConfig.NetSaslUser != nil {
		cfg.Net.SASL.User = *clientConfig.NetSaslUser
	}
	if clientConfig.NetSaslPassword != nil {
		cfg.Net.SASL.Password = *clientConfig.NetSaslPassword
	}
	if clientConfig.NetSaslScramAuthzId != nil {
		cfg.Net.SASL.SCRAMAuthzID = *clientConfig.NetSaslScramAuthzId
	}
	gssApiCfg := kafka.GSSAPIConfig{}
	gssApi := false
	if clientConfig.NetSaslGssApiAuthType != nil {
		gssApiCfg.AuthType = *clientConfig.NetSaslGssApiAuthType
		gssApi = true
	}
	if clientConfig.NetSaslGssApiKeyTabPath != nil {
		gssApiCfg.KeyTabPath = *clientConfig.NetSaslGssApiKeyTabPath
		gssApi = true
	}
	if clientConfig.NetSaslGssApiCCachePath != nil {
		gssApiCfg.CCachePath = *clientConfig.NetSaslGssApiCCachePath
		gssApi = true
	}
	if clientConfig.NetSaslGssApiKerberosConfigPath != nil {
		gssApiCfg.KerberosConfigPath = *clientConfig.NetSaslGssApiKerberosConfigPath
		gssApi = true
	}
	if clientConfig.NetSaslGssApiServiceName != nil {
		gssApiCfg.ServiceName = *clientConfig.NetSaslGssApiServiceName
		gssApi = true
	}
	if clientConfig.NetSaslGssApiUsername != nil {
		gssApiCfg.Username = *clientConfig.NetSaslGssApiUsername
		gssApi = true
	}
	if clientConfig.NetSaslGssApiPassword != nil {
		gssApiCfg.Password = *clientConfig.NetSaslGssApiPassword
		gssApi = true
	}
	if clientConfig.NetSaslGssApiRealm != nil {
		gssApiCfg.Realm = *clientConfig.NetSaslGssApiRealm
		gssApi = true
	}
	if clientConfig.NetSaslGssApiDisablePAFXFAST != nil {
		gssApiCfg.DisablePAFXFAST = *clientConfig.NetSaslGssApiDisablePAFXFAST
		gssApi = true
	}
	if gssApi {
		cfg.Net.SASL.GSSAPI = gssApiCfg
	}
	if clientConfig.NetKeepAlive != nil {
		cfg.Net.KeepAlive = *clientConfig.NetKeepAlive
	}
	if clientConfig.NetLocalAddr != nil {
		addr, err := net.ResolveIPAddr("ip", *clientConfig.NetLocalAddr)
		if err != nil {
			return nil, err
		}
		cfg.Net.LocalAddr = addr
	}
	if clientConfig.MetadataRetryMax != nil {
		cfg.Metadata.Retry.Max = *clientConfig.MetadataRetryMax
	}
	if clientConfig.MetadataRetryBackoff != nil {
		cfg.Metadata.Retry.Backoff = *clientConfig.MetadataRetryBackoff
	}
	if clientConfig.MetadataRefreshFrequency != nil {
		cfg.Metadata.RefreshFrequency = *clientConfig.MetadataRefreshFrequency
	}
	if clientConfig.MetadataFull != nil {
		cfg.Metadata.Full = *clientConfig.MetadataFull
	}
	if clientConfig.MetadataTimeout != nil {
		cfg.Metadata.Timeout = *clientConfig.MetadataTimeout
	}
	if clientConfig.MetadataAllowAutoTopicCreation != nil {
		cfg.Metadata.AllowAutoTopicCreation = *clientConfig.MetadataAllowAutoTopicCreation
	}
	if clientConfig.ProducerMaxMessageBytes != nil {
		cfg.Producer.MaxMessageBytes = *clientConfig.ProducerMaxMessageBytes
	}
	if clientConfig.ProducerRequiredAcks != nil {
		if *clientConfig.ProducerRequiredAcks == "all" {
			cfg.Producer.RequiredAcks = kafka.WaitForAll
		} else {
			i, err := strconv.Atoi(*clientConfig.ProducerRequiredAcks)
			if err != nil {
				return nil, err
			}
			cfg.Producer.RequiredAcks = kafka.RequiredAcks(int16(i))
		}
	}
	if clientConfig.ProducerTimeout != nil {
		cfg.Producer.Timeout = *clientConfig.ProducerTimeout
	}
	if clientConfig.ProducerCompression != nil {
		switch *clientConfig.ProducerCompression {
		case "none":
			cfg.Producer.Compression = kafka.CompressionNone
		case "gzip":
			cfg.Producer.Compression = kafka.CompressionGZIP
		case "snappy":
			cfg.Producer.Compression = kafka.CompressionSnappy
		case "lz4":
			cfg.Producer.Compression = kafka.CompressionLZ4
		case "zstd":
			cfg.Producer.Compression = kafka.CompressionZSTD
		default:
			return nil, fmt.Errorf("invalid config, unknown sarama compression codec: %s", *clientConfig.ProducerCompression)
		}
	}
	if clientConfig.ProducerCompressionLevel != nil {
		cfg.Producer.CompressionLevel = *clientConfig.ProducerCompressionLevel
	}
	if clientConfig.ProducerPartitioner != nil {
		switch *clientConfig.ProducerPartitioner {
		case "hash_crc":
			cfg.Producer.Partitioner = kafka.NewConsistentCRCHashPartitioner
		case "hash":
			cfg.Producer.Partitioner = kafka.NewHashPartitioner
		case "random":
			cfg.Producer.Partitioner = kafka.NewRandomPartitioner
		case "hash_reference":
			cfg.Producer.Partitioner = kafka.NewReferenceHashPartitioner
		case "round_robin":
			cfg.Producer.Partitioner = kafka.NewRoundRobinPartitioner
		default:
			return nil, fmt.Errorf("invalid config, unknown sarama partitioner: %s", *clientConfig.ProducerPartitioner)
		}
	}
	if clientConfig.ProducerFlushBytes != nil {
		cfg.Producer.Flush.Bytes = *clientConfig.ProducerFlushBytes
	}
	if clientConfig.ProducerFlushMessages != nil {
		cfg.Producer.Flush.Messages = *clientConfig.ProducerFlushMessages
	}
	if clientConfig.ProducerFlushFrequency != nil {
		cfg.Producer.Flush.Frequency = *clientConfig.ProducerFlushFrequency
	}
	if clientConfig.ProducerFlushMaxMessages != nil {
		cfg.Producer.Flush.MaxMessages = *clientConfig.ProducerFlushMaxMessages
	}
	if clientConfig.ProducerRetryMax != nil {
		cfg.Producer.Retry.Max = *clientConfig.ProducerRetryMax
	}
	if clientConfig.ProducerRetryBackoff != nil {
		cfg.Producer.Retry.Backoff = *clientConfig.ProducerRetryBackoff
	}
	if clientConfig.ClientID != nil {
		cfg.ClientID = *clientConfig.ClientID
	}
	if clientConfig.RackID != nil {
		cfg.RackID = *clientConfig.RackID
	}
	if clientConfig.ChannelBufferSize != nil {
		cfg.ChannelBufferSize = *clientConfig.ChannelBufferSize
	}
	if clientConfig.ApiVersionsRequest != nil {
		cfg.ApiVersionsRequest = *clientConfig.ApiVersionsRequest
	}
	if clientConfig.Version != nil {
		v, err := kafka.ParseKafkaVersion(*clientConfig.Version)
		if err != nil {
			return nil, err
		}
		cfg.Version = v
	}
	cfg.MetricRegistry = clientConfig.MetricRegistry
	cfg.Producer.Return.Successes = true
	return cfg, nil
}

func (clientConfig *ClientConfig) GetAddrs() ([]string, error) {
	if clientConfig.BootstrapServers != nil {
		return strings.Split(*clientConfig.BootstrapServers, ","), nil
	}
	return nil, fmt.Errorf("invalid config, sarama client config is missing bootstrap.servers")
}
