package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"koko/kafka-rest-producer/internal/config"
	"koko/kafka-rest-producer/internal/model"
	"koko/kafka-rest-producer/internal/util"
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type RdKafkaProducer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Close()
}

func NewRdKafkaProducer(cfg config.RdKafkaProducerConfig) (RdKafkaProducer, error) {
	kp, err := kafka.NewProducer(config.ToConfigMap(cfg.ClientConfig))
	if err != nil {
		return nil, err
	}
	return kp, nil
}

type kafkaProducer struct {
	config    config.RdKafkaProducerConfig
	producer  RdKafkaProducer
	asyncChan chan kafka.Event
	meterMap  map[string]metric.Int64Gauge
}

func NewKafkaProducer(cfg config.RdKafkaProducerConfig, rdp RdKafkaProducer) (*kafkaProducer, error) {
	slog.Info("Creating producer.", "config", cfg)
	asyncChan := make(chan kafka.Event, cfg.AsyncBufferSize)
	go func() {
		for e := range asyncChan {
			processResult(e)
		}
	}()
	var meterMap map[string]metric.Int64Gauge
	var err error
	if cfg.MetricsEnabled {
		meterMap, err = newRdKafkaMeters()
		if err != nil {
			return nil, err
		}
	}
	p := &kafkaProducer{cfg, rdp, asyncChan, meterMap}
	go func() {
		for e := range rdp.Events() {
			p.processEvent(e)
		}
	}()
	return p, nil
}

type ProducerError struct {
	Error error
}

func (pe ProducerError) String() string {
	return pe.Error.Error()
}

func (k *kafkaProducer) Send(ctx context.Context, messages []TopicAndMessage) []model.ProduceResult {
	if k.config.Async {
		k.sendAsync(messages)
		return nil
	} else {
		return k.sendSync(ctx, messages)
	}
}

func (k *kafkaProducer) sendSync(ctx context.Context, messages []TopicAndMessage) []model.ProduceResult {
	rcs := make([]chan kafka.Event, len(messages))
	for i, m := range messages {
		msg := toKafkaMessage(&m)
		rc := make(chan kafka.Event, 1)
		err := k.producer.Produce(msg, rc)
		if err != nil {
			select {
			case rc <- ProducerError{err}:
			default:
				slog.Error("Failed to capture producer error.", "error", err.Error())
			}
		}
		rcs[i] = rc
	}
	res := make([]model.ProduceResult, len(messages))
	for i, rc := range rcs {
		select {
		case e := <-rc:
			res[i] = processResult(e)
		case <-ctx.Done():
			err := fmt.Sprintf("Possible delivery failure. Request canceled: %s", ctx.Err().Error())
			slog.Error("Possible delivery failure. Request canceled.", "error", err)
			res[i] = model.ProduceResult{Error: &err}
		}
	}
	return res
}

func (k *kafkaProducer) sendAsync(messages []TopicAndMessage) {
	for _, m := range messages {
		msg := toKafkaMessage(&m)
		err := k.producer.Produce(msg, k.asyncChan)
		if err != nil {
			slog.Error("Kafka delivery failure.", "error", err.Error())
		}
	}
}

func (k *kafkaProducer) Close() error {
	k.producer.Close()
	return nil
}

func (k *kafkaProducer) processEvent(event kafka.Event) {
	switch ev := event.(type) {
	case *kafka.Stats:
		if k.config.MetricsEnabled {
			slog.Info("Recording rdkafka metrics.")
			recordStats(ev, k.meterMap)
		}
	default:
		slog.Info("Kafka event received.", "event", ev.String())
	}
}

func toKafkaMessage(m *TopicAndMessage) *kafka.Message {
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &m.Topic, Partition: kafka.PartitionAny}}
	if m.Message.Key != nil {
		msg.Key = []byte(*m.Message.Key)
	}
	if m.Message.Value != nil {
		msg.Value = []byte(*m.Message.Value)
	}
	if m.Message.Headers != nil && len(m.Message.Headers) > 0 {
		headers := make([]kafka.Header, len(m.Message.Headers))
		for j, h := range m.Message.Headers {
			headers[j] = kafka.Header{Key: *h.Key, Value: []byte(*h.Value)}
		}
		msg.Headers = headers
	}
	if m.Message.Timestamp != nil {
		msg.Timestamp = *m.Message.Timestamp
	}
	return msg
}

func processResult(event kafka.Event) model.ProduceResult {
	switch ev := event.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			err := fmt.Sprintf("Delivery failure: %s", ev.TopicPartition.Error.Error())
			slog.Error("Kafka delivery failure.", "error", err)
			return model.ProduceResult{Error: &err}
		} else {
			return model.ProduceResult{
				Partition: &ev.TopicPartition.Partition,
				Offset:    util.Ptr(int64(ev.TopicPartition.Offset))}
		}
	case ProducerError:
		err := fmt.Sprintf("Delivery failure: %s", ev.String())
		slog.Error("Kafka delivery failure.", "error", err)
		return model.ProduceResult{Error: &err}
	default:
		err := fmt.Sprintf("Possible delivery failure. Unrecognizable event: %s", ev.String())
		slog.Error("Possible kafka delivery failure.", "error", err)
		return model.ProduceResult{Error: &err}
	}
}

type rdKafkaStats struct {
	Name             *string                // Handle instance name
	ClientId         *string                `json:"client_id"` // "rdkafka"	The configured (or default) client.id
	ClientType       *string                `json:"type"`      // "producer"	Instance type (producer or consumer)
	Ts               *int64                 // 12345678912345	librdkafka's internal monotonic clock (microseconds)
	Time             *int64                 // Wall clock time in seconds since the epoch
	Age              *int64                 // Time since this client instance was created (microseconds)
	Replyq           *int64                 // gauge		Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll()
	MsgCnt           *int64                 `json:"msg_cnt"`      // gauge		Current number of messages in producer queues
	MsgSize          *int64                 `json:"msg_size"`     // gauge		Current total size of messages in producer queues
	MsgMax           *int64                 `json:"msg_max"`      // Threshold: maximum number of messages allowed allowed on the producer queues
	MsgSizeMax       *int64                 `json:"msg_size_max"` // Threshold: maximum total size of messages allowed on the producer queues
	Tx               *int64                 // Total number of requests sent to Kafka brokers
	TxBytes          *int64                 `json:"tx_bytes"` // Total number of bytes transmitted to Kafka brokers
	Rx               *int64                 // Total number of responses received from Kafka brokers
	RxBytes          *int64                 `json:"rx_bytes"` // Total number of bytes received from Kafka brokers
	Txmsgs           *int64                 // Total number of messages transmitted (produced) to Kafka brokers
	TxmsgBytes       *int64                 `json:"txmsg_bytes"` // Total number of message bytes (including framing, such as per-Message framing and MessageSet/batch framing) transmitted to Kafka brokers
	Rxmsgs           *int64                 // Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers.
	RxmsgBytes       *int64                 `json:"rxmsg_bytes"`        // Total number of message bytes (including framing) received from Kafka brokers
	SimpleCnt        *int64                 `json:"simple_cnt"`         // gauge		Internal tracking of legacy vs new consumer API state
	MetadataCacheCnt *int64                 `json:"metadata_cache_cnt"` // gauge		Number of topics in the metadata cache.
	Brokers          map[string]brokerStats // Dict of brokers, key is broker name, value is object. See brokers below
	Topics           map[string]topicStats  // Dict of topics, key is topic name, value is object. See topics below
}

type brokerStats struct {
	Name           *string                // "example.com:9092/13"	Broker hostname, port and broker id
	Nodeid         *int64                 // 13	Broker id (-1 for bootstraps)
	Nodename       *string                // "example.com:9092"	Broker hostname
	Source         *string                // "configured"	Broker source (learned, configured, internal, logical)
	State          *string                // "UP"	Broker state (INIT, DOWN, CONNECT, AUTH, APIVERSION_QUERY, AUTH_HANDSHAKE, UP, UPDATE)
	Stateage       *int64                 // gauge		Time since last broker state change (microseconds)
	OutbufCnt      *int64                 `json:"outbuf_cnt"`       // gauge		Number of requests awaiting transmission to broker
	OutbufMsgCnt   *int64                 `json:"outbuf_msg_cnt"`   // gauge		Number of messages awaiting transmission to broker
	WaitrespCnt    *int64                 `json:"waitresp_cnt"`     // gauge		Number of requests in-flight to broker awaiting response
	WaitrespMsgCnt *int64                 `json:"waitresp_msg_cnt"` // gauge		Number of messages in-flight to broker awaiting response
	Tx             *int64                 // Total number of requests sent
	Txbytes        *int64                 // Total number of bytes sent
	Txerrs         *int64                 // Total number of transmission errors
	Txretries      *int64                 // Total number of request retries
	Txidle         *int64                 // Microseconds since last socket send (or -1 if no sends yet for current connection).
	ReqTimeouts    *int64                 `json:"req_timeouts"` // Total number of requests timed out
	Rx             *int64                 // Total number of responses received
	Rxbytes        *int64                 // Total number of bytes received
	Rxerrs         *int64                 // Total number of receive errors
	Rxcorriderrs   *int64                 // Total number of unmatched correlation ids in response (typically for timed out requests)
	Rxpartial      *int64                 // Total number of partial MessageSets received. The broker may return partial responses if the full MessageSet could not fit in the remaining Fetch response size.
	Rxidle         *int64                 // Microseconds since last socket receive (or -1 if no receives yet for current connection).
	Req            map[string]int64       // Request type counters. Object key is the request name, value is the number of requests sent.
	ZbufGrow       *int64                 `json:"zbuf_grow"` // Total number of decompression buffer size increases
	BufGrow        *int64                 `json:"buf_grow"`  // Total number of buffer size increases (deprecated, unused)
	Wakeups        *int64                 // Broker thread poll loop wakeups
	Connects       *int64                 // Number of connection attempts, including successful and failed, and name resolution failures.
	Disconnects    *int64                 // Number of disconnects (triggered by broker, network, load-balancer, etc.).
	IntLatency     *windowStats           `json:"int_latency"`    // Internal producer queue latency in microseconds. See Window stats below
	OutbufLatency  *windowStats           `json:"outbuf_latency"` // Internal request queue latency in microseconds. This is the time between a request is enqueued on the transmit (outbuf) queue and the time the request is written to the TCP socket. Additional buffering and latency may be incurred by the TCP stack and network. See Window stats below
	Rtt            *windowStats           // Broker latency / round-trip time in microseconds. See Window stats below
	Throttle       *windowStats           // Broker throttling time in milliseconds. See Window stats below
	Toppars        map[string]topparStats // Partitions handled by this broker handle. Key is "topic-partition". See brokers.toppars below
}

type topicStats struct {
	Topic       *string                   // "myatopic"	Topic name
	Age         *int64                    // gauge		Age of client's topic object (milliseconds)
	MetadataAge *int64                    `json:"metadata_age"` // gauge		Age of metadata from broker for this topic (milliseconds)
	Batchsize   *windowStats              // Batch sizes in bytes. See Window stats·
	Batchcnt    *windowStats              // Batch message counts. See Window stats·
	Partitions  map[string]partitionStats // Partitions dict, key is partition id. See partitions below.
}

type windowStats struct {
	Min        *int64 // gauge		Smallest value
	Max        *int64 // gauge		Largest value
	Avg        *int64 // gauge		Average value
	Sum        *int64 // gauge		Sum of values
	Cnt        *int64 // gauge		Number of values sampled
	Stddev     *int64 // gauge		Standard deviation (based on histogram)
	Hdrsize    *int64 // gauge		Memory size of Hdr Histogram
	P50        *int64 // gauge		50th percentile
	P75        *int64 // gauge		75th percentile
	P90        *int64 // gauge		90th percentile
	P95        *int64 // gauge		95th percentile
	P99        *int64 // gauge		99th percentile
	P99_99     *int64 // gauge		99.99th percentile
	Outofrange *int64 // gauge		Values skipped due to out of histogram range
}

type topparStats struct {
	Topic     *string // "mytopic"	Topic name
	Partition *int64  // 3	Partition id
}

type partitionStats struct {
	Partition            *int64  // 3	Partition Id (-1 for internal UA/UnAssigned partition)
	Broker               *int64  // The id of the broker that messages are currently being fetched from
	Leader               *int64  // Current leader broker id
	Desired              *bool   // Partition is explicitly desired by application
	Unknown              *bool   // Partition not seen in topic metadata from broker
	MsgqCnt              *int64  `json:"msgq_cnt"`               // gauge		Number of messages waiting to be produced in first-level queue
	MsgqBytes            *int64  `json:"msgq_bytes"`             // gauge		Number of bytes in msgq_cnt
	XmitMsgqCnt          *int64  `json:"xmit_msgq_cnt"`          // gauge		Number of messages ready to be produced in transmit queue
	XmitMsgqBytes        *int64  `json:"xmit_msgq_bytes"`        // gauge		Number of bytes in xmit_msgq
	FetchqCnt            *int64  `json:"fetchq_cnt"`             // gauge		Number of pre-fetched messages in fetch queue
	FetchqSize           *int64  `json:"fetchq_size"`            // gauge		Bytes in fetchq
	FetchState           *string `json:"fetch_state"`            // "active"	Consumer fetch state for this partition (none, stopping, stopped, offset-query, offset-wait, active).
	QueryOffset          *int64  `json:"query_offset"`           // gauge		Current/Last logical offset query
	NextOffset           *int64  `json:"next_offset"`            // gauge		Next offset to fetch
	AppOffset            *int64  `json:"app_offset"`             // gauge		Offset of last message passed to application + 1
	StoredOffset         *int64  `json:"stored_offset"`          // gauge		Offset to be committed
	StoredLeaderEpoch    *int64  `json:"stored_leader_epoch"`    // Partition leader epoch of stored offset
	CommittedOffset      *int64  `json:"committed_offset"`       // gauge		Last committed offset
	CommittedLeaderEpoch *int64  `json:"committed_leader_epoch"` // Partition leader epoch of committed offset
	EofOffset            *int64  `json:"eof_offset"`             // gauge		Last PARTITION_EOF signaled offset
	LoOffset             *int64  `json:"lo_offset"`              // gauge		Partition's low watermark offset on broker
	HiOffset             *int64  `json:"hi_offset"`              // gauge		Partition's high watermark offset on broker
	LsOffset             *int64  `json:"ls_offset"`              // gauge		Partition's last stable offset on broker, or same as hi_offset is broker version is less than 0.11.0.0.
	ConsumerLag          *int64  `json:"consumer_lag"`           // gauge		Difference between (hi_offset or ls_offset) and committed_offset). hi_offset is used when isolation.level=read_uncommitted, otherwise ls_offset.
	ConsumerLagStored    *int64  `json:"consumer_lag_stored"`    // gauge		Difference between (hi_offset or ls_offset) and stored_offset. See consumer_lag and stored_offset.
	LeaderEpoch          *int64  `json:"leader_epoch"`           // Last known partition leader epoch, or -1 if unknown.
	Txmsgs               *int64  // Total number of messages transmitted (produced)
	Txbytes              *int64  // Total number of bytes transmitted for txmsgs
	Rxmsgs               *int64  // Total number of messages consumed, not including ignored messages (due to offset, etc).
	Rxbytes              *int64  // Total number of bytes received for rxmsgs
	Msgs                 *int64  // Total number of messages received (consumer, same as rxmsgs), or total number of messages produced (possibly not yet transmitted) (producer).
	RxVerDrops           *int64  `json:"rx_ver_drops"`  // Dropped outdated messages
	MsgsInflight         *int64  `json:"msgs_inflight"` // gauge		Current number of messages in-flight to/from broker
	NextAckSeq           *int64  `json:"next_ack_seq"`  // gauge		Next expected acked sequence (idempotent producer)
	NextErrSeq           *int64  `json:"next_err_seq"`  // gauge		Next expected errored sequence (idempotent producer)
	AckedMsgid           *int64  `json:"acked_msgid"`   // Last acked internal message id (idempotent producer)
}

type rdKafkaMeter struct {
	Name        string
	Description string
	Unit        string
}

func newRdKafkaMeters() (map[string]metric.Int64Gauge, error) {
	meters := []rdKafkaMeter{
		{"rdkafka.age", "Time since this client instance was created (microseconds)", ""},
		{"rdkafka.replyq", "gauge		Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll()", ""},
		{"rdkafka.msg_cnt", "gauge		Current number of messages in producer queues", ""},
		{"rdkafka.msg_size", "gauge		Current total size of messages in producer queues", ""},
		{"rdkafka.msg_max", "Threshold: maximum number of messages allowed allowed on the producer queues", ""},
		{"rdkafka.msg_size_max", "Threshold: maximum total size of messages allowed on the producer queues", ""},
		{"rdkafka.tx", "Total number of requests sent to Kafka brokers", ""},
		{"rdkafka.tx_bytes", "Total number of bytes transmitted to Kafka brokers", ""},
		{"rdkafka.rx", "Total number of responses received from Kafka brokers", ""},
		{"rdkafka.rx_bytes", "Total number of bytes received from Kafka brokers", ""},
		{"rdkafka.txmsgs", "Total number of messages transmitted (produced) to Kafka brokers", ""},
		{"rdkafka.txmsg_bytes", "Total number of message bytes (including framing, such as per-Message framing and MessageSet/batch framing) transmitted to Kafka brokers", ""},
		{"rdkafka.rxmsgs", "Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers.", ""},
		{"rdkafka.rxmsg_bytes", "Total number of message bytes (including framing) received from Kafka brokers", ""},
		{"rdkafka.simple_cnt", "gauge		Internal tracking of legacy vs new consumer API state", ""},
		{"rdkafka.metadata_cache_cnt", "gauge		Number of topics in the metadata cache.", ""},
		{"rdkafka.broker.stateage", "gauge		Time since last broker state change (microseconds)", ""},
		{"rdkafka.broker.outbuf_cnt", "gauge		Number of requests awaiting transmission to broker", ""},
		{"rdkafka.broker.outbuf_msg_cnt", "gauge		Number of messages awaiting transmission to broker", ""},
		{"rdkafka.broker.waitresp_cnt", "gauge		Number of requests in-flight to broker awaiting response", ""},
		{"rdkafka.broker.waitresp_msg_cnt", "gauge		Number of messages in-flight to broker awaiting response", ""},
		{"rdkafka.broker.tx", "Total number of requests sent", ""},
		{"rdkafka.broker.txbytes", "Total number of bytes sent", ""},
		{"rdkafka.broker.txerrs", "Total number of transmission errors", ""},
		{"rdkafka.broker.txretries", "Total number of request retries", ""},
		{"rdkafka.broker.txidle", "Microseconds since last socket send (or -1 if no sends yet for current connection).", ""},
		{"rdkafka.broker.req_timeouts", "Total number of requests timed out", ""},
		{"rdkafka.broker.rx", "Total number of responses received", ""},
		{"rdkafka.broker.rxbytes", "Total number of bytes received", ""},
		{"rdkafka.broker.rxerrs", "Total number of receive errors", ""},
		{"rdkafka.broker.rxcorriderrs", "Total number of unmatched correlation ids in response (typically for timed out requests)", ""},
		{"rdkafka.broker.rxpartial", "Total number of partial MessageSets received. The broker may return partial responses if the full MessageSet could not fit in the remaining Fetch response size.", ""},
		{"rdkafka.broker.rxidle", "Microseconds since last socket receive (or -1 if no receives yet for current connection).", ""},
		{"rdkafka.broker.req", "Request type counters. Object key is the request name, value is the number of requests sent.", ""},
		{"rdkafka.broker.zbuf_grow", "Total number of decompression buffer size increases", ""},
		{"rdkafka.broker.buf_grow", "Total number of buffer size increases (deprecated, unused)", ""},
		{"rdkafka.broker.wakeups", "Broker thread poll loop wakeups", ""},
		{"rdkafka.broker.connects", "Number of connection attempts, including successful and failed, and name resolution failures.", ""},
		{"rdkafka.broker.disconnects", "Number of disconnects (triggered by broker, network, load-balancer, etc.).", ""},
		{"rdkafka.broker.int_latency.min", "gauge		Internal producer queue latency in microseconds. Smallest value", ""},
		{"rdkafka.broker.int_latency.max", "gauge		Internal producer queue latency in microseconds. Largest value", ""},
		{"rdkafka.broker.int_latency.avg", "gauge		Internal producer queue latency in microseconds. Average value", ""},
		{"rdkafka.broker.int_latency.sum", "gauge		Internal producer queue latency in microseconds. Sum of values", ""},
		{"rdkafka.broker.int_latency.cnt", "gauge		Internal producer queue latency in microseconds. Number of values sampled", ""},
		{"rdkafka.broker.int_latency.stddev", "gauge		Internal producer queue latency in microseconds. Standard deviation (based on histogram)", ""},
		{"rdkafka.broker.int_latency.hdrsize", "gauge		Internal producer queue latency in microseconds. Memory size of Hdr Histogram", ""},
		{"rdkafka.broker.int_latency.p50", "gauge		Internal producer queue latency in microseconds. 50th percentile", ""},
		{"rdkafka.broker.int_latency.p75", "gauge		Internal producer queue latency in microseconds. 75th percentile", ""},
		{"rdkafka.broker.int_latency.p90", "gauge		Internal producer queue latency in microseconds. 90th percentile", ""},
		{"rdkafka.broker.int_latency.p95", "gauge		Internal producer queue latency in microseconds. 95th percentile", ""},
		{"rdkafka.broker.int_latency.p99", "gauge		Internal producer queue latency in microseconds. 99th percentile", ""},
		{"rdkafka.broker.int_latency.p99_99", "gauge		Internal producer queue latency in microseconds. 99.99th percentile", ""},
		{"rdkafka.broker.int_latency.outofrange", "gauge		Internal producer queue latency in microseconds. Values skipped due to out of histogram range", ""},
		{"rdkafka.broker.outbuf_latency.min", "gauge		Internal request queue latency in microseconds. Smallest value", ""},
		{"rdkafka.broker.outbuf_latency.max", "gauge		Internal request queue latency in microseconds. Largest value", ""},
		{"rdkafka.broker.outbuf_latency.avg", "gauge		Internal request queue latency in microseconds. Average value", ""},
		{"rdkafka.broker.outbuf_latency.sum", "gauge		Internal request queue latency in microseconds. Sum of values", ""},
		{"rdkafka.broker.outbuf_latency.cnt", "gauge		Internal request queue latency in microseconds. Number of values sampled", ""},
		{"rdkafka.broker.outbuf_latency.stddev", "gauge		Internal request queue latency in microseconds. Standard deviation (based on histogram)", ""},
		{"rdkafka.broker.outbuf_latency.hdrsize", "gauge		Internal request queue latency in microseconds. Memory size of Hdr Histogram", ""},
		{"rdkafka.broker.outbuf_latency.p50", "gauge		Internal request queue latency in microseconds. 50th percentile", ""},
		{"rdkafka.broker.outbuf_latency.p75", "gauge		Internal request queue latency in microseconds. 75th percentile", ""},
		{"rdkafka.broker.outbuf_latency.p90", "gauge		Internal request queue latency in microseconds. 90th percentile", ""},
		{"rdkafka.broker.outbuf_latency.p95", "gauge		Internal request queue latency in microseconds. 95th percentile", ""},
		{"rdkafka.broker.outbuf_latency.p99", "gauge		Internal request queue latency in microseconds. 99th percentile", ""},
		{"rdkafka.broker.outbuf_latency.p99_99", "gauge		Internal request queue latency in microseconds. 99.99th percentile", ""},
		{"rdkafka.broker.outbuf_latency.outofrange", "gauge		Internal request queue latency in microseconds. Values skipped due to out of histogram range", ""},
		{"rdkafka.broker.rtt.min", "gauge		Broker latency / round-trip time in microseconds. Smallest value", ""},
		{"rdkafka.broker.rtt.max", "gauge		Broker latency / round-trip time in microseconds. Largest value", ""},
		{"rdkafka.broker.rtt.avg", "gauge		Broker latency / round-trip time in microseconds. Average value", ""},
		{"rdkafka.broker.rtt.sum", "gauge		Broker latency / round-trip time in microseconds. Sum of values", ""},
		{"rdkafka.broker.rtt.cnt", "gauge		Broker latency / round-trip time in microseconds. Number of values sampled", ""},
		{"rdkafka.broker.rtt.stddev", "gauge		Broker latency / round-trip time in microseconds. Standard deviation (based on histogram)", ""},
		{"rdkafka.broker.rtt.hdrsize", "gauge		Broker latency / round-trip time in microseconds. Memory size of Hdr Histogram", ""},
		{"rdkafka.broker.rtt.p50", "gauge		Broker latency / round-trip time in microseconds. 50th percentile", ""},
		{"rdkafka.broker.rtt.p75", "gauge		Broker latency / round-trip time in microseconds. 75th percentile", ""},
		{"rdkafka.broker.rtt.p90", "gauge		Broker latency / round-trip time in microseconds. 90th percentile", ""},
		{"rdkafka.broker.rtt.p95", "gauge		Broker latency / round-trip time in microseconds. 95th percentile", ""},
		{"rdkafka.broker.rtt.p99", "gauge		Broker latency / round-trip time in microseconds. 99th percentile", ""},
		{"rdkafka.broker.rtt.p99_99", "gauge		Broker latency / round-trip time in microseconds. 99.99th percentile", ""},
		{"rdkafka.broker.rtt.outofrange", "gauge		Broker latency / round-trip time in microseconds. Values skipped due to out of histogram range", ""},
		{"rdkafka.broker.throttle.min", "gauge		Broker throttling time in milliseconds. Smallest value", ""},
		{"rdkafka.broker.throttle.max", "gauge		Broker throttling time in milliseconds. Largest value", ""},
		{"rdkafka.broker.throttle.avg", "gauge		Broker throttling time in milliseconds. Average value", ""},
		{"rdkafka.broker.throttle.sum", "gauge		Broker throttling time in milliseconds. Sum of values", ""},
		{"rdkafka.broker.throttle.cnt", "gauge		Broker throttling time in milliseconds. Number of values sampled", ""},
		{"rdkafka.broker.throttle.stddev", "gauge		Broker throttling time in milliseconds. Standard deviation (based on histogram)", ""},
		{"rdkafka.broker.throttle.hdrsize", "gauge		Broker throttling time in milliseconds. Memory size of Hdr Histogram", ""},
		{"rdkafka.broker.throttle.p50", "gauge		Broker throttling time in milliseconds. 50th percentile", ""},
		{"rdkafka.broker.throttle.p75", "gauge		Broker throttling time in milliseconds. 75th percentile", ""},
		{"rdkafka.broker.throttle.p90", "gauge		Broker throttling time in milliseconds. 90th percentile", ""},
		{"rdkafka.broker.throttle.p95", "gauge		Broker throttling time in milliseconds. 95th percentile", ""},
		{"rdkafka.broker.throttle.p99", "gauge		Broker throttling time in milliseconds. 99th percentile", ""},
		{"rdkafka.broker.throttle.p99_99", "gauge		Broker throttling time in milliseconds. 99.99th percentile", ""},
		{"rdkafka.broker.throttle.outofrange", "gauge		Broker throttling time in milliseconds. Values skipped due to out of histogram range", ""},
		{"rdkafka.topic.age", "gauge		Age of client's topic object (milliseconds)", ""},
		{"rdkafka.topic.metadata_age", "gauge		Age of metadata from broker for this topic (milliseconds)", ""},
		{"rdkafka.topic.batchsize.min", "gauge		Batch sizes in bytes. Smallest value", ""},
		{"rdkafka.topic.batchsize.max", "gauge		Batch sizes in bytes. Largest value", ""},
		{"rdkafka.topic.batchsize.avg", "gauge		Batch sizes in bytes. Average value", ""},
		{"rdkafka.topic.batchsize.sum", "gauge		Batch sizes in bytes. Sum of values", ""},
		{"rdkafka.topic.batchsize.cnt", "gauge		Batch sizes in bytes. Number of values sampled", ""},
		{"rdkafka.topic.batchsize.stddev", "gauge		Batch sizes in bytes. Standard deviation (based on histogram)", ""},
		{"rdkafka.topic.batchsize.hdrsize", "gauge		Batch sizes in bytes. Memory size of Hdr Histogram", ""},
		{"rdkafka.topic.batchsize.p50", "gauge		Batch sizes in bytes. 50th percentile", ""},
		{"rdkafka.topic.batchsize.p75", "gauge		Batch sizes in bytes. 75th percentile", ""},
		{"rdkafka.topic.batchsize.p90", "gauge		Batch sizes in bytes. 90th percentile", ""},
		{"rdkafka.topic.batchsize.p95", "gauge		Batch sizes in bytes. 95th percentile", ""},
		{"rdkafka.topic.batchsize.p99", "gauge		Batch sizes in bytes. 99th percentile", ""},
		{"rdkafka.topic.batchsize.p99_99", "gauge		Batch sizes in bytes. 99.99th percentile", ""},
		{"rdkafka.topic.batchsize.outofrange", "gauge		Batch sizes in bytes. Values skipped due to out of histogram range", ""},
		{"rdkafka.topic.batchcnt.min", "gauge		Batch message counts. Smallest value", ""},
		{"rdkafka.topic.batchcnt.max", "gauge		Batch message counts. Largest value", ""},
		{"rdkafka.topic.batchcnt.avg", "gauge		Batch message counts. Average value", ""},
		{"rdkafka.topic.batchcnt.sum", "gauge		Batch message counts. Sum of values", ""},
		{"rdkafka.topic.batchcnt.cnt", "gauge		Batch message counts. Number of values sampled", ""},
		{"rdkafka.topic.batchcnt.stddev", "gauge		Batch message counts. Standard deviation (based on histogram)", ""},
		{"rdkafka.topic.batchcnt.hdrsize", "gauge		Batch message counts. Memory size of Hdr Histogram", ""},
		{"rdkafka.topic.batchcnt.p50", "gauge		Batch message counts. 50th percentile", ""},
		{"rdkafka.topic.batchcnt.p75", "gauge		Batch message counts. 75th percentile", ""},
		{"rdkafka.topic.batchcnt.p90", "gauge		Batch message counts. 90th percentile", ""},
		{"rdkafka.topic.batchcnt.p95", "gauge		Batch message counts. 95th percentile", ""},
		{"rdkafka.topic.batchcnt.p99", "gauge		Batch message counts. 99th percentile", ""},
		{"rdkafka.topic.batchcnt.p99_99", "gauge		Batch message counts. 99.99th percentile", ""},
		{"rdkafka.topic.batchcnt.outofrange", "gauge		Batch message counts. Values skipped due to out of histogram range", ""},
		{"rdkafka.topic.partition.msgq_cnt", "gauge		Number of messages waiting to be produced in first-level queue", ""},
		{"rdkafka.topic.partition.msgq_bytes", "gauge		Number of bytes in msgq_cnt", ""},
		{"rdkafka.topic.partition.xmit_msgq_cnt", "gauge		Number of messages ready to be produced in transmit queue", ""},
		{"rdkafka.topic.partition.xmit_msgq_bytes", "gauge		Number of bytes in xmit_msgq", ""},
		{"rdkafka.topic.partition.txmsgs", "Total number of messages transmitted (produced)", ""},
		{"rdkafka.topic.partition.txbytes", "Total number of bytes transmitted for txmsgs", ""},
		{"rdkafka.topic.partition.msgs", "Total number of messages received (consumer, same as rxmsgs), or total number of messages produced (possibly not yet transmitted) (producer).", ""},
		{"rdkafka.topic.partition.rx_ver_drops", "Dropped outdated messages", ""},
		{"rdkafka.topic.partition.msgs_inflight", "gauge		Current number of messages in-flight to/from broker", ""},
	}
	meter := otel.Meter("koko/kafka-rest-producer")
	meterMap := make(map[string]metric.Int64Gauge, len(meters))
	for _, rdkMeter := range meters {
		g, err := meter.Int64Gauge(rdkMeter.Name, metric.WithDescription(rdkMeter.Description), metric.WithUnit(rdkMeter.Unit))
		if err != nil {
			return nil, err
		}
		meterMap[rdkMeter.Name] = g
	}
	return meterMap, nil
}

func recordStats(stats *kafka.Stats, meterMap map[string]metric.Int64Gauge) {
	rdkStats := &rdKafkaStats{}
	if err := json.Unmarshal([]byte(stats.String()), rdkStats); err != nil {
		slog.Error("Failed to parse rdkafka stats.", "error", err.Error())
		return
	}

	slog.Info("rdkafka stats.", "json", stats.String(), "struct", rdkStats)

	ctx := context.Background()

	nameAttribute := attribute.String("name", *rdkStats.Name)
	clientIdAttribute := attribute.String("client_id", *rdkStats.ClientId)
	topLevelAttributes := metric.WithAttributes(nameAttribute, clientIdAttribute)

	if rdkStats.Age != nil {
		meterMap["rdkafka.age"].Record(ctx, *rdkStats.Age, topLevelAttributes)
	}
	if rdkStats.Replyq != nil {
		meterMap["rdkafka.replyq"].Record(ctx, *rdkStats.Replyq, topLevelAttributes)
	}
	if rdkStats.MsgCnt != nil {
		meterMap["rdkafka.msg_cnt"].Record(ctx, *rdkStats.MsgCnt, topLevelAttributes)
	}
	if rdkStats.MsgSize != nil {
		meterMap["rdkafka.msg_size"].Record(ctx, *rdkStats.MsgSize, topLevelAttributes)
	}
	if rdkStats.MsgMax != nil {
		meterMap["rdkafka.msg_max"].Record(ctx, *rdkStats.MsgMax, topLevelAttributes)
	}
	if rdkStats.MsgSizeMax != nil {
		meterMap["rdkafka.msg_size_max"].Record(ctx, *rdkStats.MsgSizeMax, topLevelAttributes)
	}
	if rdkStats.Tx != nil {
		meterMap["rdkafka.tx"].Record(ctx, *rdkStats.Tx, topLevelAttributes)
	}
	if rdkStats.TxBytes != nil {
		meterMap["rdkafka.tx_bytes"].Record(ctx, *rdkStats.TxBytes, topLevelAttributes)
	}
	if rdkStats.Rx != nil {
		meterMap["rdkafka.rx"].Record(ctx, *rdkStats.Rx, topLevelAttributes)
	}
	if rdkStats.RxBytes != nil {
		meterMap["rdkafka.rx_bytes"].Record(ctx, *rdkStats.RxBytes, topLevelAttributes)
	}
	if rdkStats.Txmsgs != nil {
		meterMap["rdkafka.txmsgs"].Record(ctx, *rdkStats.Txmsgs, topLevelAttributes)
	}
	if rdkStats.TxmsgBytes != nil {
		meterMap["rdkafka.txmsg_bytes"].Record(ctx, *rdkStats.TxmsgBytes, topLevelAttributes)
	}
	if rdkStats.Rxmsgs != nil {
		meterMap["rdkafka.rxmsgs"].Record(ctx, *rdkStats.Rxmsgs, topLevelAttributes)
	}
	if rdkStats.RxmsgBytes != nil {
		meterMap["rdkafka.rxmsg_bytes"].Record(ctx, *rdkStats.RxmsgBytes, topLevelAttributes)
	}
	if rdkStats.SimpleCnt != nil {
		meterMap["rdkafka.simple_cnt"].Record(ctx, *rdkStats.SimpleCnt, topLevelAttributes)
	}
	if rdkStats.MetadataCacheCnt != nil {
		meterMap["rdkafka.metadata_cache_cnt"].Record(ctx, *rdkStats.MetadataCacheCnt, topLevelAttributes)
	}

	for _, bStats := range rdkStats.Brokers {
		nodeIdAttribute := attribute.Int64("node_id", *bStats.Nodeid)
		nodeNameAttribute := attribute.String("node_id", *bStats.Nodename)
		brokerAttributes := metric.WithAttributes(nameAttribute, clientIdAttribute, nodeIdAttribute, nodeNameAttribute)

		if bStats.Stateage != nil {
			meterMap["rdkafka.broker.stateage"].Record(ctx, *bStats.Stateage, brokerAttributes)
		}
		if bStats.OutbufCnt != nil {
			meterMap["rdkafka.broker.outbuf_cnt"].Record(ctx, *bStats.OutbufCnt, brokerAttributes)
		}
		if bStats.OutbufMsgCnt != nil {
			meterMap["rdkafka.broker.outbuf_msg_cnt"].Record(ctx, *bStats.OutbufMsgCnt, brokerAttributes)
		}
		if bStats.WaitrespCnt != nil {
			meterMap["rdkafka.broker.waitresp_cnt"].Record(ctx, *bStats.WaitrespCnt, brokerAttributes)
		}
		if bStats.WaitrespMsgCnt != nil {
			meterMap["rdkafka.broker.waitresp_msg_cnt"].Record(ctx, *bStats.WaitrespMsgCnt, brokerAttributes)
		}
		if bStats.Tx != nil {
			meterMap["rdkafka.broker.tx"].Record(ctx, *bStats.Tx, brokerAttributes)
		}
		if bStats.Txbytes != nil {
			meterMap["rdkafka.broker.txbytes"].Record(ctx, *bStats.Txbytes, brokerAttributes)
		}
		if bStats.Txerrs != nil {
			meterMap["rdkafka.broker.txerrs"].Record(ctx, *bStats.Txerrs, brokerAttributes)
		}
		if bStats.Txretries != nil {
			meterMap["rdkafka.broker.txretries"].Record(ctx, *bStats.Txretries, brokerAttributes)
		}
		if bStats.Txidle != nil {
			meterMap["rdkafka.broker.txidle"].Record(ctx, *bStats.Txidle, brokerAttributes)
		}
		if bStats.ReqTimeouts != nil {
			meterMap["rdkafka.broker.req_timeouts"].Record(ctx, *bStats.ReqTimeouts, brokerAttributes)
		}
		if bStats.Rx != nil {
			meterMap["rdkafka.broker.rx"].Record(ctx, *bStats.Rx, brokerAttributes)
		}
		if bStats.Rxbytes != nil {
			meterMap["rdkafka.broker.rxbytes"].Record(ctx, *bStats.Rxbytes, brokerAttributes)
		}
		if bStats.Rxerrs != nil {
			meterMap["rdkafka.broker.rxerrs"].Record(ctx, *bStats.Rxerrs, brokerAttributes)
		}
		if bStats.Rxcorriderrs != nil {
			meterMap["rdkafka.broker.rxcorriderrs"].Record(ctx, *bStats.Rxcorriderrs, brokerAttributes)
		}
		if bStats.Rxpartial != nil {
			meterMap["rdkafka.broker.rxpartial"].Record(ctx, *bStats.Rxpartial, brokerAttributes)
		}
		if bStats.Rxidle != nil {
			meterMap["rdkafka.broker.rxidle"].Record(ctx, *bStats.Rxidle, brokerAttributes)
		}
		if bStats.ZbufGrow != nil {
			meterMap["rdkafka.broker.zbuf_grow"].Record(ctx, *bStats.ZbufGrow, brokerAttributes)
		}
		if bStats.BufGrow != nil {
			meterMap["rdkafka.broker.buf_grow"].Record(ctx, *bStats.BufGrow, brokerAttributes)
		}
		if bStats.Wakeups != nil {
			meterMap["rdkafka.broker.wakeups"].Record(ctx, *bStats.Wakeups, brokerAttributes)
		}
		if bStats.Connects != nil {
			meterMap["rdkafka.broker.connects"].Record(ctx, *bStats.Connects, brokerAttributes)
		}
		if bStats.Disconnects != nil {
			meterMap["rdkafka.broker.disconnects"].Record(ctx, *bStats.Disconnects, brokerAttributes)
		}

		if bStats.IntLatency.Min != nil {
			meterMap["rdkafka.broker.int_latency.min"].Record(ctx, *bStats.IntLatency.Min, brokerAttributes)
		}
		if bStats.IntLatency.Max != nil {
			meterMap["rdkafka.broker.int_latency.max"].Record(ctx, *bStats.IntLatency.Max, brokerAttributes)
		}
		if bStats.IntLatency.Avg != nil {
			meterMap["rdkafka.broker.int_latency.avg"].Record(ctx, *bStats.IntLatency.Avg, brokerAttributes)
		}
		if bStats.IntLatency.Sum != nil {
			meterMap["rdkafka.broker.int_latency.sum"].Record(ctx, *bStats.IntLatency.Sum, brokerAttributes)
		}
		if bStats.IntLatency.Cnt != nil {
			meterMap["rdkafka.broker.int_latency.cnt"].Record(ctx, *bStats.IntLatency.Cnt, brokerAttributes)
		}
		if bStats.IntLatency.Stddev != nil {
			meterMap["rdkafka.broker.int_latency.stddev"].Record(ctx, *bStats.IntLatency.Stddev, brokerAttributes)
		}
		if bStats.IntLatency.Hdrsize != nil {
			meterMap["rdkafka.broker.int_latency.hdrsize"].Record(ctx, *bStats.IntLatency.Hdrsize, brokerAttributes)
		}
		if bStats.IntLatency.P50 != nil {
			meterMap["rdkafka.broker.int_latency.p50"].Record(ctx, *bStats.IntLatency.P50, brokerAttributes)
		}
		if bStats.IntLatency.P75 != nil {
			meterMap["rdkafka.broker.int_latency.p75"].Record(ctx, *bStats.IntLatency.P75, brokerAttributes)
		}
		if bStats.IntLatency.P90 != nil {
			meterMap["rdkafka.broker.int_latency.p90"].Record(ctx, *bStats.IntLatency.P90, brokerAttributes)
		}
		if bStats.IntLatency.P95 != nil {
			meterMap["rdkafka.broker.int_latency.p95"].Record(ctx, *bStats.IntLatency.P95, brokerAttributes)
		}
		if bStats.IntLatency.P99 != nil {
			meterMap["rdkafka.broker.int_latency.p99"].Record(ctx, *bStats.IntLatency.P99, brokerAttributes)
		}
		if bStats.IntLatency.P99_99 != nil {
			meterMap["rdkafka.broker.int_latency.p99_99"].Record(ctx, *bStats.IntLatency.P99_99, brokerAttributes)
		}
		if bStats.IntLatency.Outofrange != nil {
			meterMap["rdkafka.broker.int_latency.outofrange"].Record(ctx, *bStats.IntLatency.Outofrange, brokerAttributes)
		}
		if bStats.OutbufLatency.Min != nil {
			meterMap["rdkafka.broker.outbuf_latency.min"].Record(ctx, *bStats.OutbufLatency.Min, brokerAttributes)
		}
		if bStats.OutbufLatency.Max != nil {
			meterMap["rdkafka.broker.outbuf_latency.max"].Record(ctx, *bStats.OutbufLatency.Max, brokerAttributes)
		}
		if bStats.OutbufLatency.Avg != nil {
			meterMap["rdkafka.broker.outbuf_latency.avg"].Record(ctx, *bStats.OutbufLatency.Avg, brokerAttributes)
		}
		if bStats.OutbufLatency.Sum != nil {
			meterMap["rdkafka.broker.outbuf_latency.sum"].Record(ctx, *bStats.OutbufLatency.Sum, brokerAttributes)
		}
		if bStats.OutbufLatency.Cnt != nil {
			meterMap["rdkafka.broker.outbuf_latency.cnt"].Record(ctx, *bStats.OutbufLatency.Cnt, brokerAttributes)
		}
		if bStats.OutbufLatency.Stddev != nil {
			meterMap["rdkafka.broker.outbuf_latency.stddev"].Record(ctx, *bStats.OutbufLatency.Stddev, brokerAttributes)
		}
		if bStats.OutbufLatency.Hdrsize != nil {
			meterMap["rdkafka.broker.outbuf_latency.hdrsize"].Record(ctx, *bStats.OutbufLatency.Hdrsize, brokerAttributes)
		}
		if bStats.OutbufLatency.P50 != nil {
			meterMap["rdkafka.broker.outbuf_latency.p50"].Record(ctx, *bStats.OutbufLatency.P50, brokerAttributes)
		}
		if bStats.OutbufLatency.P75 != nil {
			meterMap["rdkafka.broker.outbuf_latency.p75"].Record(ctx, *bStats.OutbufLatency.P75, brokerAttributes)
		}
		if bStats.OutbufLatency.P90 != nil {
			meterMap["rdkafka.broker.outbuf_latency.p90"].Record(ctx, *bStats.OutbufLatency.P90, brokerAttributes)
		}
		if bStats.OutbufLatency.P95 != nil {
			meterMap["rdkafka.broker.outbuf_latency.p95"].Record(ctx, *bStats.OutbufLatency.P95, brokerAttributes)
		}
		if bStats.OutbufLatency.P99 != nil {
			meterMap["rdkafka.broker.outbuf_latency.p99"].Record(ctx, *bStats.OutbufLatency.P99, brokerAttributes)
		}
		if bStats.OutbufLatency.P99_99 != nil {
			meterMap["rdkafka.broker.outbuf_latency.p99_99"].Record(ctx, *bStats.OutbufLatency.P99_99, brokerAttributes)
		}
		if bStats.OutbufLatency.Outofrange != nil {
			meterMap["rdkafka.broker.outbuf_latency.outofrange"].Record(ctx, *bStats.OutbufLatency.Outofrange, brokerAttributes)
		}
		if bStats.Rtt.Min != nil {
			meterMap["rdkafka.broker.rtt.min"].Record(ctx, *bStats.Rtt.Min, brokerAttributes)
		}
		if bStats.Rtt.Max != nil {
			meterMap["rdkafka.broker.rtt.max"].Record(ctx, *bStats.Rtt.Max, brokerAttributes)
		}
		if bStats.Rtt.Avg != nil {
			meterMap["rdkafka.broker.rtt.avg"].Record(ctx, *bStats.Rtt.Avg, brokerAttributes)
		}
		if bStats.Rtt.Sum != nil {
			meterMap["rdkafka.broker.rtt.sum"].Record(ctx, *bStats.Rtt.Sum, brokerAttributes)
		}
		if bStats.Rtt.Cnt != nil {
			meterMap["rdkafka.broker.rtt.cnt"].Record(ctx, *bStats.Rtt.Cnt, brokerAttributes)
		}
		if bStats.Rtt.Stddev != nil {
			meterMap["rdkafka.broker.rtt.stddev"].Record(ctx, *bStats.Rtt.Stddev, brokerAttributes)
		}
		if bStats.Rtt.Hdrsize != nil {
			meterMap["rdkafka.broker.rtt.hdrsize"].Record(ctx, *bStats.Rtt.Hdrsize, brokerAttributes)
		}
		if bStats.Rtt.P50 != nil {
			meterMap["rdkafka.broker.rtt.p50"].Record(ctx, *bStats.Rtt.P50, brokerAttributes)
		}
		if bStats.Rtt.P75 != nil {
			meterMap["rdkafka.broker.rtt.p75"].Record(ctx, *bStats.Rtt.P75, brokerAttributes)
		}
		if bStats.Rtt.P90 != nil {
			meterMap["rdkafka.broker.rtt.p90"].Record(ctx, *bStats.Rtt.P90, brokerAttributes)
		}
		if bStats.Rtt.P95 != nil {
			meterMap["rdkafka.broker.rtt.p95"].Record(ctx, *bStats.Rtt.P95, brokerAttributes)
		}
		if bStats.Rtt.P99 != nil {
			meterMap["rdkafka.broker.rtt.p99"].Record(ctx, *bStats.Rtt.P99, brokerAttributes)
		}
		if bStats.Rtt.P99_99 != nil {
			meterMap["rdkafka.broker.rtt.p99_99"].Record(ctx, *bStats.Rtt.P99_99, brokerAttributes)
		}
		if bStats.Rtt.Outofrange != nil {
			meterMap["rdkafka.broker.rtt.outofrange"].Record(ctx, *bStats.Rtt.Outofrange, brokerAttributes)
		}
		if bStats.Throttle.Min != nil {
			meterMap["rdkafka.broker.throttle.min"].Record(ctx, *bStats.Throttle.Min, brokerAttributes)
		}
		if bStats.Throttle.Max != nil {
			meterMap["rdkafka.broker.throttle.max"].Record(ctx, *bStats.Throttle.Max, brokerAttributes)
		}
		if bStats.Throttle.Avg != nil {
			meterMap["rdkafka.broker.throttle.avg"].Record(ctx, *bStats.Throttle.Avg, brokerAttributes)
		}
		if bStats.Throttle.Sum != nil {
			meterMap["rdkafka.broker.throttle.sum"].Record(ctx, *bStats.Throttle.Sum, brokerAttributes)
		}
		if bStats.Throttle.Cnt != nil {
			meterMap["rdkafka.broker.throttle.cnt"].Record(ctx, *bStats.Throttle.Cnt, brokerAttributes)
		}
		if bStats.Throttle.Stddev != nil {
			meterMap["rdkafka.broker.throttle.stddev"].Record(ctx, *bStats.Throttle.Stddev, brokerAttributes)
		}
		if bStats.Throttle.Hdrsize != nil {
			meterMap["rdkafka.broker.throttle.hdrsize"].Record(ctx, *bStats.Throttle.Hdrsize, brokerAttributes)
		}
		if bStats.Throttle.P50 != nil {
			meterMap["rdkafka.broker.throttle.p50"].Record(ctx, *bStats.Throttle.P50, brokerAttributes)
		}
		if bStats.Throttle.P75 != nil {
			meterMap["rdkafka.broker.throttle.p75"].Record(ctx, *bStats.Throttle.P75, brokerAttributes)
		}
		if bStats.Throttle.P90 != nil {
			meterMap["rdkafka.broker.throttle.p90"].Record(ctx, *bStats.Throttle.P90, brokerAttributes)
		}
		if bStats.Throttle.P95 != nil {
			meterMap["rdkafka.broker.throttle.p95"].Record(ctx, *bStats.Throttle.P95, brokerAttributes)
		}
		if bStats.Throttle.P99 != nil {
			meterMap["rdkafka.broker.throttle.p99"].Record(ctx, *bStats.Throttle.P99, brokerAttributes)
		}
		if bStats.Throttle.P99_99 != nil {
			meterMap["rdkafka.broker.throttle.p99_99"].Record(ctx, *bStats.Throttle.P99_99, brokerAttributes)
		}
		if bStats.Throttle.Outofrange != nil {
			meterMap["rdkafka.broker.throttle.outofrange"].Record(ctx, *bStats.Throttle.Outofrange, brokerAttributes)
		}

		for req, val := range bStats.Req {
			requestNameAttribute := attribute.String("request_name", req)
			requestAttributes := metric.WithAttributes(nameAttribute, clientIdAttribute, nodeIdAttribute, nodeNameAttribute, requestNameAttribute)
			meterMap["rdkafka.broker.req"].Record(ctx, val, requestAttributes)
		}
	}

	for _, tStats := range rdkStats.Topics {
		topicAttribute := attribute.String("topic", *tStats.Topic)
		topicAttributes := metric.WithAttributes(nameAttribute, clientIdAttribute, topicAttribute)

		if tStats.Age != nil {
			meterMap["rdkafka.topic.age"].Record(ctx, *tStats.Age, topicAttributes)
		}
		if tStats.MetadataAge != nil {
			meterMap["rdkafka.topic.metadata_age"].Record(ctx, *tStats.MetadataAge, topicAttributes)
		}
		if tStats.Batchsize.Min != nil {
			meterMap["rdkafka.topic.batchsize.min"].Record(ctx, *tStats.Batchsize.Min, topicAttributes)
		}
		if tStats.Batchsize.Max != nil {
			meterMap["rdkafka.topic.batchsize.max"].Record(ctx, *tStats.Batchsize.Max, topicAttributes)
		}
		if tStats.Batchsize.Avg != nil {
			meterMap["rdkafka.topic.batchsize.avg"].Record(ctx, *tStats.Batchsize.Avg, topicAttributes)
		}
		if tStats.Batchsize.Sum != nil {
			meterMap["rdkafka.topic.batchsize.sum"].Record(ctx, *tStats.Batchsize.Sum, topicAttributes)
		}
		if tStats.Batchsize.Cnt != nil {
			meterMap["rdkafka.topic.batchsize.cnt"].Record(ctx, *tStats.Batchsize.Cnt, topicAttributes)
		}
		if tStats.Batchsize.Stddev != nil {
			meterMap["rdkafka.topic.batchsize.stddev"].Record(ctx, *tStats.Batchsize.Stddev, topicAttributes)
		}
		if tStats.Batchsize.Hdrsize != nil {
			meterMap["rdkafka.topic.batchsize.hdrsize"].Record(ctx, *tStats.Batchsize.Hdrsize, topicAttributes)
		}
		if tStats.Batchsize.P50 != nil {
			meterMap["rdkafka.topic.batchsize.p50"].Record(ctx, *tStats.Batchsize.P50, topicAttributes)
		}
		if tStats.Batchsize.P75 != nil {
			meterMap["rdkafka.topic.batchsize.p75"].Record(ctx, *tStats.Batchsize.P75, topicAttributes)
		}
		if tStats.Batchsize.P90 != nil {
			meterMap["rdkafka.topic.batchsize.p90"].Record(ctx, *tStats.Batchsize.P90, topicAttributes)
		}
		if tStats.Batchsize.P95 != nil {
			meterMap["rdkafka.topic.batchsize.p95"].Record(ctx, *tStats.Batchsize.P95, topicAttributes)
		}
		if tStats.Batchsize.P99 != nil {
			meterMap["rdkafka.topic.batchsize.p99"].Record(ctx, *tStats.Batchsize.P99, topicAttributes)
		}
		if tStats.Batchsize.P99_99 != nil {
			meterMap["rdkafka.topic.batchsize.p99_99"].Record(ctx, *tStats.Batchsize.P99_99, topicAttributes)
		}
		if tStats.Batchsize.Outofrange != nil {
			meterMap["rdkafka.topic.batchsize.outofrange"].Record(ctx, *tStats.Batchsize.Outofrange, topicAttributes)
		}
		if tStats.Batchcnt.Min != nil {
			meterMap["rdkafka.topic.batchcnt.min"].Record(ctx, *tStats.Batchcnt.Min, topicAttributes)
		}
		if tStats.Batchcnt.Max != nil {
			meterMap["rdkafka.topic.batchcnt.max"].Record(ctx, *tStats.Batchcnt.Max, topicAttributes)
		}
		if tStats.Batchcnt.Avg != nil {
			meterMap["rdkafka.topic.batchcnt.avg"].Record(ctx, *tStats.Batchcnt.Avg, topicAttributes)
		}
		if tStats.Batchcnt.Sum != nil {
			meterMap["rdkafka.topic.batchcnt.sum"].Record(ctx, *tStats.Batchcnt.Sum, topicAttributes)
		}
		if tStats.Batchcnt.Cnt != nil {
			meterMap["rdkafka.topic.batchcnt.cnt"].Record(ctx, *tStats.Batchcnt.Cnt, topicAttributes)
		}
		if tStats.Batchcnt.Stddev != nil {
			meterMap["rdkafka.topic.batchcnt.stddev"].Record(ctx, *tStats.Batchcnt.Stddev, topicAttributes)
		}
		if tStats.Batchcnt.Hdrsize != nil {
			meterMap["rdkafka.topic.batchcnt.hdrsize"].Record(ctx, *tStats.Batchcnt.Hdrsize, topicAttributes)
		}
		if tStats.Batchcnt.P50 != nil {
			meterMap["rdkafka.topic.batchcnt.p50"].Record(ctx, *tStats.Batchcnt.P50, topicAttributes)
		}
		if tStats.Batchcnt.P75 != nil {
			meterMap["rdkafka.topic.batchcnt.p75"].Record(ctx, *tStats.Batchcnt.P75, topicAttributes)
		}
		if tStats.Batchcnt.P90 != nil {
			meterMap["rdkafka.topic.batchcnt.p90"].Record(ctx, *tStats.Batchcnt.P90, topicAttributes)
		}
		if tStats.Batchcnt.P95 != nil {
			meterMap["rdkafka.topic.batchcnt.p95"].Record(ctx, *tStats.Batchcnt.P95, topicAttributes)
		}
		if tStats.Batchcnt.P99 != nil {
			meterMap["rdkafka.topic.batchcnt.p99"].Record(ctx, *tStats.Batchcnt.P99, topicAttributes)
		}
		if tStats.Batchcnt.P99_99 != nil {
			meterMap["rdkafka.topic.batchcnt.p99_99"].Record(ctx, *tStats.Batchcnt.P99_99, topicAttributes)
		}
		if tStats.Batchcnt.Outofrange != nil {
			meterMap["rdkafka.topic.batchcnt.outofrange"].Record(ctx, *tStats.Batchcnt.Outofrange, topicAttributes)
		}

		for _, pStats := range tStats.Partitions {
			partitionAttribute := attribute.Int64("partition", *pStats.Partition)
			partitionAttributes := metric.WithAttributes(nameAttribute, clientIdAttribute, topicAttribute, partitionAttribute)

			if pStats.MsgqCnt != nil {
				meterMap["rdkafka.topic.partition.msgq_cnt"].Record(ctx, *pStats.MsgqCnt, partitionAttributes)
			}
			if pStats.MsgqBytes != nil {
				meterMap["rdkafka.topic.partition.msgq_bytes"].Record(ctx, *pStats.MsgqBytes, partitionAttributes)
			}
			if pStats.XmitMsgqCnt != nil {
				meterMap["rdkafka.topic.partition.xmit_msgq_cnt"].Record(ctx, *pStats.XmitMsgqCnt, partitionAttributes)
			}
			if pStats.XmitMsgqBytes != nil {
				meterMap["rdkafka.topic.partition.xmit_msgq_bytes"].Record(ctx, *pStats.XmitMsgqBytes, partitionAttributes)
			}
			if pStats.Txmsgs != nil {
				meterMap["rdkafka.topic.partition.txmsgs"].Record(ctx, *pStats.Txmsgs, partitionAttributes)
			}
			if pStats.Txbytes != nil {
				meterMap["rdkafka.topic.partition.txbytes"].Record(ctx, *pStats.Txbytes, partitionAttributes)
			}
			if pStats.Msgs != nil {
				meterMap["rdkafka.topic.partition.msgs"].Record(ctx, *pStats.Msgs, partitionAttributes)
			}
			if pStats.RxVerDrops != nil {
				meterMap["rdkafka.topic.partition.rx_ver_drops"].Record(ctx, *pStats.RxVerDrops, partitionAttributes)
			}
			if pStats.MsgsInflight != nil {
				meterMap["rdkafka.topic.partition.msgs_inflight"].Record(ctx, *pStats.MsgsInflight, partitionAttributes)
			}
		}
	}
}
