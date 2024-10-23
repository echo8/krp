package rdk

import (
	"github.com/echo8/krp/internal/config/schemaregistry"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/creasty/defaults"
	"gopkg.in/yaml.v3"
)

type ProducerConfig struct {
	Type            string
	AsyncBufferSize int                    `default:"100000"`
	ClientConfig    *ClientConfig          `yaml:"clientConfig"`
	SchemaRegistry  *schemaregistry.Config `yaml:"schemaRegistry"`
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
	ClientId                            *string `yaml:"client.id"`                               // client.id
	MetadataBrokerList                  *string `yaml:"metadata.broker.list"`                    // metadata.broker.list
	BootstrapServers                    *string `yaml:"bootstrap.servers"`                       // bootstrap.servers
	MessageMaxBytes                     *int    `yaml:"message.max.bytes"`                       // message.max.bytes	*	1000 .. 1000000000
	MessageCopyMaxBytes                 *int    `yaml:"message.copy.max.bytes"`                  // message.copy.max.bytes	*	0 .. 1000000000
	ReceiveMessageMaxBytes              *int    `yaml:"receive.message.max.bytes"`               // receive.message.max.bytes	*	1000 .. 2147483647
	MaxInFlightRequestsPerConnection    *int    `yaml:"max.in.flight.requests.per.connection"`   // max.in.flight.requests.per.connection	*	1 .. 1000000
	MaxInFlight                         *int    `yaml:"max.in.flight"`                           // max.in.flight	*	1 .. 1000000
	TopicMetadataRefreshIntervalMs      *int    `yaml:"topic.metadata.refresh.interval.ms"`      // topic.metadata.refresh.interval.ms	*	-1 .. 3600000
	MetadataMaxAgeMs                    *int    `yaml:"metadata.max.age.ms"`                     // metadata.max.age.ms	*	1 .. 86400000
	TopicMetadataRefreshFastIntervalMs  *int    `yaml:"topic.metadata.refresh.fast.interval.ms"` // topic.metadata.refresh.fast.interval.ms	*	1 .. 60000
	TopicMetadataRefreshSparse          *bool   `yaml:"topic.metadata.refresh.sparse"`           // topic.metadata.refresh.sparse	*	true, false
	TopicMetadataPropagationMaxMs       *int    `yaml:"topic.metadata.propagation.max.ms"`       // topic.metadata.propagation.max.ms	*	0 .. 3600000
	TopicBlacklist                      *string `yaml:"topic.blacklist"`                         // topic.blacklist
	Debug                               *string `yaml:"debug"`                                   // debug
	SocketTimeoutMs                     *int    `yaml:"socket.timeout.ms"`                       // socket.timeout.ms	*	10 .. 300000
	SocketSendBufferBytes               *int    `yaml:"socket.send.buffer.bytes"`                // socket.send.buffer.bytes	*	0 .. 100000000
	SocketReceiveBufferBytes            *int    `yaml:"socket.receive.buffer.bytes"`             // socket.receive.buffer.bytes	*	0 .. 100000000
	SocketKeepAliveEnable               *bool   `yaml:"socket.keepalive.enable"`                 // socket.keepalive.enable
	SocketNagleDisable                  *bool   `yaml:"socket.nagle.disable"`                    // socket.nagle.disable
	SocketMaxFails                      *int    `yaml:"socket.max.fails"`                        // socket.max.fails	*	0 .. 1000000
	BrokerAddressTtl                    *int    `yaml:"broker.address.ttl"`                      // broker.address.ttl	*	0 .. 86400000
	BrokerAddressFamily                 *string `yaml:"broker.address.family"`                   // broker.address.family	*	any, v4, v6
	SocketConnectionSetupTimeoutMs      *int    `yaml:"socket.connection.setup.timeout.ms"`      // socket.connection.setup.timeout.ms	*	1000 .. 2147483647
	ConnectionMaxIdleMs                 *int    `yaml:"connections.max.idle.ms"`                 // connections.max.idle.ms	*	0 .. 2147483647
	ReconnectBackoffMs                  *int    `yaml:"reconnect.backoff.ms"`                    // reconnect.backoff.ms	*	0 .. 3600000
	ReconnectBackoffMaxMs               *int    `yaml:"reconnect.backoff.max.ms"`                // reconnect.backoff.max.ms	*	0 .. 3600000
	StatisticsIntervalMs                *int    `yaml:"statistics.interval.ms"`                  // statistics.interval.ms	*	0 .. 86400000
	EnabledEvents                       *int    `yaml:"enabled_events"`                          // enabled_events	*	0 .. 2147483647
	LogLevel                            *int    `yaml:"log_level"`                               // log_level	*	0 .. 7
	LogQueue                            *bool   `yaml:"log.queue"`                               // log.queue	*	true, false
	LogThreadName                       *bool   `yaml:"log.thread.name"`                         // log.thread.name
	EnableRandomSeed                    *bool   `yaml:"enable.random.seed"`                      // enable.random.seed
	LogConnectionClose                  *bool   `yaml:"log.connection.close"`                    // log.connection.close
	InternalTerminationSignal           *int    `yaml:"internal.termination.signal"`             // internal.termination.signal	*	0 .. 128
	ApiVersionRequest                   *bool   `yaml:"api.version.request"`                     // api.version.request	*	true, false
	ApiVersionRequestTimeoutMs          *int    `yaml:"api.version.request.timeout.ms"`          // api.version.request.timeout.ms	*	1 .. 300000
	ApiVersionFallbackMs                *int    `yaml:"api.version.fallback.ms"`                 // api.version.fallback.ms	*	0 .. 604800000
	BrokerVersionFallback               *string `yaml:"broker.version.fallback"`                 // broker.version.fallback
	AllowAutoCreateTopics               *bool   `yaml:"allow.auto.create.topics"`                // allow.auto.create.topics
	SecurityProtocol                    *string `yaml:"security.protocol"`                       // security.protocol
	SslCipherSuites                     *string `yaml:"ssl.cipher.suites"`                       // ssl.cipher.suites
	SslCurvesList                       *string `yaml:"ssl.curves.list"`                         // ssl.curves.list
	SslSigalgsList                      *string `yaml:"ssl.sigalgs.list"`                        // ssl.sigalgs.list
	SslKeyLocation                      *string `yaml:"ssl.key.location"`                        // ssl.key.location
	SslKeyPassword                      *string `yaml:"ssl.key.password"`                        // ssl.key.password
	SslKeyPem                           *string `yaml:"ssl.key.pem"`                             // ssl.key.pem
	SslCertificateLocation              *string `yaml:"ssl.certificate.location"`                // ssl.certificate.location
	SslCertificatePem                   *string `yaml:"ssl.certificate.pem"`                     // ssl.certificate.pem
	SslCaLocation                       *string `yaml:"ssl.ca.location"`                         // ssl.ca.location
	SslCaPem                            *string `yaml:"ssl.ca.pem"`                              // ssl.ca.pem
	SslCaCertificateStores              *string `yaml:"ssl.ca.certificate.stores"`               // ssl.ca.certificate.stores
	SslCrlLocation                      *string `yaml:"ssl.crl.location"`                        // ssl.crl.location
	SslKeystoreLocation                 *string `yaml:"ssl.keystore.location"`                   // ssl.keystore.location
	SslKeystorePassword                 *string `yaml:"ssl.keystore.password"`                   // ssl.keystore.password
	SslProviders                        *string `yaml:"ssl.providers"`                           // ssl.providers
	SslEngineId                         *string `yaml:"ssl.engine.id"`                           // ssl.engine.id
	EnableSslCertificateVerification    *bool   `yaml:"enable.ssl.certificate.verification"`     // enable.ssl.certificate.verification
	SslEndpointIdentificationAlgorithm  *string `yaml:"ssl.endpoint.identification.algorithm"`   // ssl.endpoint.identification.algorithm
	SaslMechanisms                      *string `yaml:"sasl.mechanisms"`                         // sasl.mechanisms
	SaslMechanism                       *string `yaml:"sasl.mechanism"`                          // sasl.mechanism
	SaslKerberosServiceName             *string `yaml:"sasl.kerberos.service.name"`              // sasl.kerberos.service.name
	SaslKerberosPrinciple               *string `yaml:"sasl.kerberos.principal"`                 // sasl.kerberos.principal
	SaslKerberosKinitCmd                *string `yaml:"sasl.kerberos.kinit.cmd"`                 // sasl.kerberos.kinit.cmd
	SaslKerberosKeytab                  *string `yaml:"sasl.kerberos.keytab"`                    // sasl.kerberos.keytab
	SaslKerberosMinTimeBeforeRelogin    *int    `yaml:"sasl.kerberos.min.time.before.relogin"`   // sasl.kerberos.min.time.before.relogin	*	0 .. 86400000
	SaslUsername                        *string `yaml:"sasl.username"`                           // sasl.username
	SaslPassword                        *string `yaml:"sasl.password"`                           // sasl.password
	SaslOauthbearerConfig               *string `yaml:"sasl.oauthbearer.config"`                 // sasl.oauthbearer.config
	EnableSaslOauthbearerUnsecureJwt    *bool   `yaml:"enable.sasl.oauthbearer.unsecure.jwt"`    // enable.sasl.oauthbearer.unsecure.jwt
	SaslOauthbearerMethod               *string `yaml:"sasl.oauthbearer.method"`                 // sasl.oauthbearer.method
	SaslOauthbearerClientId             *string `yaml:"sasl.oauthbearer.client.id"`              // sasl.oauthbearer.client.id
	SaslOauthbearerClientSecret         *string `yaml:"sasl.oauthbearer.client.secret"`          // sasl.oauthbearer.client.secret
	SaslOauthbearerScope                *string `yaml:"sasl.oauthbearer.scope"`                  // sasl.oauthbearer.scope
	SaslOauthbearerExtensions           *string `yaml:"sasl.oauthbearer.extensions"`             // sasl.oauthbearer.extensions
	SaslOauthbearerTokenEndpointUrl     *string `yaml:"sasl.oauthbearer.token.endpoint.url"`     // sasl.oauthbearer.token.endpoint.url
	PluginLibraryPaths                  *string `yaml:"plugin.library.paths"`                    // plugin.library.paths
	ClientRack                          *string `yaml:"client.rack"`                             // client.rack
	QueueBufferingMaxMessages           *int    `yaml:"queue.buffering.max.messages"`            // queue.buffering.max.messages	P	0 .. 2147483647
	QueueBufferingMaxKbytes             *int    `yaml:"queue.buffering.max.kbytes"`              // queue.buffering.max.kbytes	P	1 .. 2147483647
	QueueBufferingMaxMs                 *int    `yaml:"queue.buffering.max.ms"`                  // queue.buffering.max.ms	P	0 .. 900000
	LingerMs                            *int    `yaml:"linger.ms"`                               // linger.ms	P	0 .. 900000
	MessageSendMaxRetries               *int    `yaml:"message.send.max.retries"`                // message.send.max.retries	P	0 .. 2147483647
	Retries                             *int    `yaml:"retries"`                                 // retries	P	0 .. 2147483647
	RetryBackoffMs                      *int    `yaml:"retry.backoff.ms"`                        // retry.backoff.ms	*	1 .. 300000
	RetryBackoffMaxMs                   *int    `yaml:"retry.backoff.max.ms"`                    // retry.backoff.max.ms	*	1 .. 300000
	QueueBufferingBackpressureThreshold *int    `yaml:"queue.buffering.backpressure.threshold"`  // queue.buffering.backpressure.threshold	P	1 .. 1000000
	CompressionCodec                    *string `yaml:"compression.codec"`                       // compression.codec
	CompressionType                     *string `yaml:"compression.type"`                        // compression.type
	BatchNumMessages                    *int    `yaml:"batch.num.messages"`                      // batch.num.messages	P	1 .. 1000000
	BatchSize                           *int    `yaml:"batch.size"`                              // batch.size	P	1 .. 2147483647
	DeliveryReportOnlyError             *bool   `yaml:"delivery.report.only.error"`              // delivery.report.only.error
	StickyPartitioningLingerMs          *int    `yaml:"sticky.partitioning.linger.ms"`           // sticky.partitioning.linger.ms
	ClientDnsLookup                     *string `yaml:"client.dns.lookup"`                       // client.dns.lookup
	RequestRequiredAcks                 *string `yaml:"request.required.acks"`                   // request.required.acks	P	-1 .. 1000
	Acks                                *string `yaml:"acks"`                                    // acks	P	-1 .. 1000
	RequestTimeoutMs                    *int    `yaml:"request.timeout.ms"`                      // request.timeout.ms	P	1 .. 900000
	MessageTimeoutMs                    *int    `yaml:"message.timeout.ms"`                      // message.timeout.ms	P	0 .. 2147483647
	DeliveryTimeoutMs                   *int    `yaml:"delivery.timeout.ms"`                     // delivery.timeout.ms	P	0 .. 2147483647
	Partitioner                         *string `yaml:"partitioner"`                             // partitioner
	CompressionLevel                    *int    `yaml:"compression.level"`                       // compression.level	P	-1 .. 12
}

func (clientConfig *ClientConfig) ToConfigMap() *kafka.ConfigMap {
	cm := kafka.ConfigMap{}
	if clientConfig.ClientId != nil {
		cm["client.id"] = *clientConfig.ClientId
	}
	if clientConfig.MetadataBrokerList != nil {
		cm["metadata.broker.list"] = *clientConfig.MetadataBrokerList
	}
	if clientConfig.BootstrapServers != nil {
		cm["bootstrap.servers"] = *clientConfig.BootstrapServers
	}
	if clientConfig.MessageMaxBytes != nil {
		cm["message.max.bytes"] = *clientConfig.MessageMaxBytes
	}
	if clientConfig.MessageCopyMaxBytes != nil {
		cm["message.copy.max.bytes"] = *clientConfig.MessageCopyMaxBytes
	}
	if clientConfig.ReceiveMessageMaxBytes != nil {
		cm["receive.message.max.bytes"] = *clientConfig.ReceiveMessageMaxBytes
	}
	if clientConfig.MaxInFlightRequestsPerConnection != nil {
		cm["max.in.flight.requests.per.connection"] = *clientConfig.MaxInFlightRequestsPerConnection
	}
	if clientConfig.MaxInFlight != nil {
		cm["max.in.flight"] = *clientConfig.MaxInFlight
	}
	if clientConfig.TopicMetadataRefreshIntervalMs != nil {
		cm["topic.metadata.refresh.interval.ms"] = *clientConfig.TopicMetadataRefreshIntervalMs
	}
	if clientConfig.MetadataMaxAgeMs != nil {
		cm["metadata.max.age.ms"] = *clientConfig.MetadataMaxAgeMs
	}
	if clientConfig.TopicMetadataRefreshFastIntervalMs != nil {
		cm["topic.metadata.refresh.fast.interval.ms"] = *clientConfig.TopicMetadataRefreshFastIntervalMs
	}
	if clientConfig.TopicMetadataRefreshSparse != nil {
		cm["topic.metadata.refresh.sparse"] = *clientConfig.TopicMetadataRefreshSparse
	}
	if clientConfig.TopicMetadataPropagationMaxMs != nil {
		cm["topic.metadata.propagation.max.ms"] = *clientConfig.TopicMetadataPropagationMaxMs
	}
	if clientConfig.TopicBlacklist != nil {
		cm["topic.blacklist"] = *clientConfig.TopicBlacklist
	}
	if clientConfig.Debug != nil {
		cm["debug"] = *clientConfig.Debug
	}
	if clientConfig.SocketTimeoutMs != nil {
		cm["socket.timeout.ms"] = *clientConfig.SocketTimeoutMs
	}
	if clientConfig.SocketSendBufferBytes != nil {
		cm["socket.send.buffer.bytes"] = *clientConfig.SocketSendBufferBytes
	}
	if clientConfig.SocketReceiveBufferBytes != nil {
		cm["socket.receive.buffer.bytes"] = *clientConfig.SocketReceiveBufferBytes
	}
	if clientConfig.SocketKeepAliveEnable != nil {
		cm["socket.keepalive.enable"] = *clientConfig.SocketKeepAliveEnable
	}
	if clientConfig.SocketNagleDisable != nil {
		cm["socket.nagle.disable"] = *clientConfig.SocketNagleDisable
	}
	if clientConfig.SocketMaxFails != nil {
		cm["socket.max.fails"] = *clientConfig.SocketMaxFails
	}
	if clientConfig.BrokerAddressTtl != nil {
		cm["broker.address.ttl"] = *clientConfig.BrokerAddressTtl
	}
	if clientConfig.BrokerAddressFamily != nil {
		cm["broker.address.family"] = *clientConfig.BrokerAddressFamily
	}
	if clientConfig.SocketConnectionSetupTimeoutMs != nil {
		cm["socket.connection.setup.timeout.ms"] = *clientConfig.SocketConnectionSetupTimeoutMs
	}
	if clientConfig.ConnectionMaxIdleMs != nil {
		cm["connections.max.idle.ms"] = *clientConfig.ConnectionMaxIdleMs
	}
	if clientConfig.ReconnectBackoffMs != nil {
		cm["reconnect.backoff.ms"] = *clientConfig.ReconnectBackoffMs
	}
	if clientConfig.ReconnectBackoffMaxMs != nil {
		cm["reconnect.backoff.max.ms"] = *clientConfig.ReconnectBackoffMaxMs
	}
	if clientConfig.StatisticsIntervalMs != nil {
		cm["statistics.interval.ms"] = *clientConfig.StatisticsIntervalMs
	}
	if clientConfig.EnabledEvents != nil {
		cm["enabled_events"] = *clientConfig.EnabledEvents
	}
	if clientConfig.LogLevel != nil {
		cm["log_level"] = *clientConfig.LogLevel
	}
	if clientConfig.LogQueue != nil {
		cm["log.queue"] = *clientConfig.LogQueue
	}
	if clientConfig.LogThreadName != nil {
		cm["log.thread.name"] = *clientConfig.LogThreadName
	}
	if clientConfig.EnableRandomSeed != nil {
		cm["enable.random.seed"] = *clientConfig.EnableRandomSeed
	}
	if clientConfig.LogConnectionClose != nil {
		cm["log.connection.close"] = *clientConfig.LogConnectionClose
	}
	if clientConfig.InternalTerminationSignal != nil {
		cm["internal.termination.signal"] = *clientConfig.InternalTerminationSignal
	}
	if clientConfig.ApiVersionRequest != nil {
		cm["api.version.request"] = *clientConfig.ApiVersionRequest
	}
	if clientConfig.ApiVersionRequestTimeoutMs != nil {
		cm["api.version.request.timeout.ms"] = *clientConfig.ApiVersionRequestTimeoutMs
	}
	if clientConfig.ApiVersionFallbackMs != nil {
		cm["api.version.fallback.ms"] = *clientConfig.ApiVersionFallbackMs
	}
	if clientConfig.BrokerVersionFallback != nil {
		cm["broker.version.fallback"] = *clientConfig.BrokerVersionFallback
	}
	if clientConfig.AllowAutoCreateTopics != nil {
		cm["allow.auto.create.topics"] = *clientConfig.AllowAutoCreateTopics
	}
	if clientConfig.SecurityProtocol != nil {
		cm["security.protocol"] = *clientConfig.SecurityProtocol
	}
	if clientConfig.SslCipherSuites != nil {
		cm["ssl.cipher.suites"] = *clientConfig.SslCipherSuites
	}
	if clientConfig.SslCurvesList != nil {
		cm["ssl.curves.list"] = *clientConfig.SslCurvesList
	}
	if clientConfig.SslSigalgsList != nil {
		cm["ssl.sigalgs.list"] = *clientConfig.SslSigalgsList
	}
	if clientConfig.SslKeyLocation != nil {
		cm["ssl.key.location"] = *clientConfig.SslKeyLocation
	}
	if clientConfig.SslKeyPassword != nil {
		cm["ssl.key.password"] = *clientConfig.SslKeyPassword
	}
	if clientConfig.SslKeyPem != nil {
		cm["ssl.key.pem"] = *clientConfig.SslKeyPem
	}
	if clientConfig.SslCertificateLocation != nil {
		cm["ssl.certificate.location"] = *clientConfig.SslCertificateLocation
	}
	if clientConfig.SslCertificatePem != nil {
		cm["ssl.certificate.pem"] = *clientConfig.SslCertificatePem
	}
	if clientConfig.SslCaLocation != nil {
		cm["ssl.ca.location"] = *clientConfig.SslCaLocation
	}
	if clientConfig.SslCaPem != nil {
		cm["ssl.ca.pem"] = *clientConfig.SslCaPem
	}
	if clientConfig.SslCaCertificateStores != nil {
		cm["ssl.ca.certificate.stores"] = *clientConfig.SslCaCertificateStores
	}
	if clientConfig.SslCrlLocation != nil {
		cm["ssl.crl.location"] = *clientConfig.SslCrlLocation
	}
	if clientConfig.SslKeystoreLocation != nil {
		cm["ssl.keystore.location"] = *clientConfig.SslKeystoreLocation
	}
	if clientConfig.SslKeystorePassword != nil {
		cm["ssl.keystore.password"] = *clientConfig.SslKeystorePassword
	}
	if clientConfig.SslProviders != nil {
		cm["ssl.providers"] = *clientConfig.SslProviders
	}
	if clientConfig.SslEngineId != nil {
		cm["ssl.engine.id"] = *clientConfig.SslEngineId
	}
	if clientConfig.EnableSslCertificateVerification != nil {
		cm["enable.ssl.certificate.verification"] = *clientConfig.EnableSslCertificateVerification
	}
	if clientConfig.SslEndpointIdentificationAlgorithm != nil {
		cm["ssl.endpoint.identification.algorithm"] = *clientConfig.SslEndpointIdentificationAlgorithm
	}
	if clientConfig.SaslMechanisms != nil {
		cm["sasl.mechanisms"] = *clientConfig.SaslMechanisms
	}
	if clientConfig.SaslMechanism != nil {
		cm["sasl.mechanism"] = *clientConfig.SaslMechanism
	}
	if clientConfig.SaslKerberosServiceName != nil {
		cm["sasl.kerberos.service.name"] = *clientConfig.SaslKerberosServiceName
	}
	if clientConfig.SaslKerberosPrinciple != nil {
		cm["sasl.kerberos.principal"] = *clientConfig.SaslKerberosPrinciple
	}
	if clientConfig.SaslKerberosKinitCmd != nil {
		cm["sasl.kerberos.kinit.cmd"] = *clientConfig.SaslKerberosKinitCmd
	}
	if clientConfig.SaslKerberosKeytab != nil {
		cm["sasl.kerberos.keytab"] = *clientConfig.SaslKerberosKeytab
	}
	if clientConfig.SaslKerberosMinTimeBeforeRelogin != nil {
		cm["sasl.kerberos.min.time.before.relogin"] = *clientConfig.SaslKerberosMinTimeBeforeRelogin
	}
	if clientConfig.SaslUsername != nil {
		cm["sasl.username"] = *clientConfig.SaslUsername
	}
	if clientConfig.SaslPassword != nil {
		cm["sasl.password"] = *clientConfig.SaslPassword
	}
	if clientConfig.SaslOauthbearerConfig != nil {
		cm["sasl.oauthbearer.config"] = *clientConfig.SaslOauthbearerConfig
	}
	if clientConfig.EnableSaslOauthbearerUnsecureJwt != nil {
		cm["enable.sasl.oauthbearer.unsecure.jwt"] = *clientConfig.EnableSaslOauthbearerUnsecureJwt
	}
	if clientConfig.SaslOauthbearerMethod != nil {
		cm["sasl.oauthbearer.method"] = *clientConfig.SaslOauthbearerMethod
	}
	if clientConfig.SaslOauthbearerClientId != nil {
		cm["sasl.oauthbearer.client.id"] = *clientConfig.SaslOauthbearerClientId
	}
	if clientConfig.SaslOauthbearerClientSecret != nil {
		cm["sasl.oauthbearer.client.secret"] = *clientConfig.SaslOauthbearerClientSecret
	}
	if clientConfig.SaslOauthbearerScope != nil {
		cm["sasl.oauthbearer.scope"] = *clientConfig.SaslOauthbearerScope
	}
	if clientConfig.SaslOauthbearerExtensions != nil {
		cm["sasl.oauthbearer.extensions"] = *clientConfig.SaslOauthbearerExtensions
	}
	if clientConfig.SaslOauthbearerTokenEndpointUrl != nil {
		cm["sasl.oauthbearer.token.endpoint.url"] = *clientConfig.SaslOauthbearerTokenEndpointUrl
	}
	if clientConfig.PluginLibraryPaths != nil {
		cm["plugin.library.paths"] = *clientConfig.PluginLibraryPaths
	}
	if clientConfig.ClientRack != nil {
		cm["client.rack"] = *clientConfig.ClientRack
	}
	if clientConfig.QueueBufferingMaxMessages != nil {
		cm["queue.buffering.max.messages"] = *clientConfig.QueueBufferingMaxMessages
	}
	if clientConfig.QueueBufferingMaxKbytes != nil {
		cm["queue.buffering.max.kbytes"] = *clientConfig.QueueBufferingMaxKbytes
	}
	if clientConfig.QueueBufferingMaxMs != nil {
		cm["queue.buffering.max.ms"] = *clientConfig.QueueBufferingMaxMs
	}
	if clientConfig.LingerMs != nil {
		cm["linger.ms"] = *clientConfig.LingerMs
	}
	if clientConfig.MessageSendMaxRetries != nil {
		cm["message.send.max.retries"] = *clientConfig.MessageSendMaxRetries
	}
	if clientConfig.Retries != nil {
		cm["retries"] = *clientConfig.Retries
	}
	if clientConfig.RetryBackoffMs != nil {
		cm["retry.backoff.ms"] = *clientConfig.RetryBackoffMs
	}
	if clientConfig.RetryBackoffMaxMs != nil {
		cm["retry.backoff.max.ms"] = *clientConfig.RetryBackoffMaxMs
	}
	if clientConfig.QueueBufferingBackpressureThreshold != nil {
		cm["queue.buffering.backpressure.threshold"] = *clientConfig.QueueBufferingBackpressureThreshold
	}
	if clientConfig.CompressionCodec != nil {
		cm["compression.codec"] = *clientConfig.CompressionCodec
	}
	if clientConfig.CompressionType != nil {
		cm["compression.type"] = *clientConfig.CompressionType
	}
	if clientConfig.BatchNumMessages != nil {
		cm["batch.num.messages"] = *clientConfig.BatchNumMessages
	}
	if clientConfig.BatchSize != nil {
		cm["batch.size"] = *clientConfig.BatchSize
	}
	if clientConfig.DeliveryReportOnlyError != nil {
		cm["delivery.report.only.error"] = *clientConfig.DeliveryReportOnlyError
	}
	if clientConfig.StickyPartitioningLingerMs != nil {
		cm["sticky.partitioning.linger.ms"] = *clientConfig.StickyPartitioningLingerMs
	}
	if clientConfig.ClientDnsLookup != nil {
		cm["client.dns.lookup"] = *clientConfig.ClientDnsLookup
	}
	if clientConfig.RequestRequiredAcks != nil {
		cm["request.required.acks"] = *clientConfig.RequestRequiredAcks
	}
	if clientConfig.Acks != nil {
		cm["acks"] = *clientConfig.Acks
	}
	if clientConfig.RequestTimeoutMs != nil {
		cm["request.timeout.ms"] = *clientConfig.RequestTimeoutMs
	}
	if clientConfig.MessageTimeoutMs != nil {
		cm["message.timeout.ms"] = *clientConfig.MessageTimeoutMs
	}
	if clientConfig.DeliveryTimeoutMs != nil {
		cm["delivery.timeout.ms"] = *clientConfig.DeliveryTimeoutMs
	}
	if clientConfig.Partitioner != nil {
		cm["partitioner"] = *clientConfig.Partitioner
	}
	if clientConfig.CompressionLevel != nil {
		cm["compression.level"] = *clientConfig.CompressionLevel
	}
	return &cm
}
