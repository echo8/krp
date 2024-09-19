package metric

import (
	"context"
	"encoding/json"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	otm "go.opentelemetry.io/otel/metric"
)

func newRdkMeters() (*rdkMeters, error) {
	rm := &rdkMeters{}
	if err := createMeters(rm); err != nil {
		return nil, err
	}
	return rm, nil
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

type rdkMeters struct {
	Age                           otm.Int64Gauge `name:"rdkafka.age" description:"Time since this client instance was created (microseconds)" unit:""`
	Replyq                        otm.Int64Gauge `name:"rdkafka.replyq" description:"gauge		Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll()" unit:""`
	MsgCnt                        otm.Int64Gauge `name:"rdkafka.msg_cnt" description:"gauge		Current number of messages in producer queues" unit:""`
	MsgSize                       otm.Int64Gauge `name:"rdkafka.msg_size" description:"gauge		Current total size of messages in producer queues" unit:""`
	MsgMax                        otm.Int64Gauge `name:"rdkafka.msg_max" description:"Threshold: maximum number of messages allowed allowed on the producer queues" unit:""`
	MsgSizeMax                    otm.Int64Gauge `name:"rdkafka.msg_size_max" description:"Threshold: maximum total size of messages allowed on the producer queues" unit:""`
	Tx                            otm.Int64Gauge `name:"rdkafka.tx" description:"Total number of requests sent to Kafka brokers" unit:""`
	TxBytes                       otm.Int64Gauge `name:"rdkafka.tx_bytes" description:"Total number of bytes transmitted to Kafka brokers" unit:""`
	Rx                            otm.Int64Gauge `name:"rdkafka.rx" description:"Total number of responses received from Kafka brokers" unit:""`
	RxBytes                       otm.Int64Gauge `name:"rdkafka.rx_bytes" description:"Total number of bytes received from Kafka brokers" unit:""`
	Txmsgs                        otm.Int64Gauge `name:"rdkafka.txmsgs" description:"Total number of messages transmitted (produced) to Kafka brokers" unit:""`
	TxmsgBytes                    otm.Int64Gauge `name:"rdkafka.txmsg_bytes" description:"Total number of message bytes (including framing, such as per-Message framing and MessageSet/batch framing) transmitted to Kafka brokers" unit:""`
	Rxmsgs                        otm.Int64Gauge `name:"rdkafka.rxmsgs" description:"Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers." unit:""`
	RxmsgBytes                    otm.Int64Gauge `name:"rdkafka.rxmsg_bytes" description:"Total number of message bytes (including framing) received from Kafka brokers" unit:""`
	SimpleCnt                     otm.Int64Gauge `name:"rdkafka.simple_cnt" description:"gauge		Internal tracking of legacy vs new consumer API state" unit:""`
	MetadataCacheCnt              otm.Int64Gauge `name:"rdkafka.metadata_cache_cnt" description:"gauge		Number of topics in the metadata cache." unit:""`
	BrokerStateage                otm.Int64Gauge `name:"rdkafka.broker.stateage" description:"gauge		Time since last broker state change (microseconds)" unit:""`
	BrokerOutbufCnt               otm.Int64Gauge `name:"rdkafka.broker.outbuf_cnt" description:"gauge		Number of requests awaiting transmission to broker" unit:""`
	BrokerOutbufMsgCnt            otm.Int64Gauge `name:"rdkafka.broker.outbuf_msg_cnt" description:"gauge		Number of messages awaiting transmission to broker" unit:""`
	BrokerWaitrespCnt             otm.Int64Gauge `name:"rdkafka.broker.waitresp_cnt" description:"gauge		Number of requests in-flight to broker awaiting response" unit:""`
	BrokerWaitrespMsgCnt          otm.Int64Gauge `name:"rdkafka.broker.waitresp_msg_cnt" description:"gauge		Number of messages in-flight to broker awaiting response" unit:""`
	BrokerTx                      otm.Int64Gauge `name:"rdkafka.broker.tx" description:"Total number of requests sent" unit:""`
	BrokerTxbytes                 otm.Int64Gauge `name:"rdkafka.broker.txbytes" description:"Total number of bytes sent" unit:""`
	BrokerTxerrs                  otm.Int64Gauge `name:"rdkafka.broker.txerrs" description:"Total number of transmission errors" unit:""`
	BrokerTxretries               otm.Int64Gauge `name:"rdkafka.broker.txretries" description:"Total number of request retries" unit:""`
	BrokerTxidle                  otm.Int64Gauge `name:"rdkafka.broker.txidle" description:"Microseconds since last socket send (or -1 if no sends yet for current connection)." unit:""`
	BrokerReqTimeouts             otm.Int64Gauge `name:"rdkafka.broker.req_timeouts" description:"Total number of requests timed out" unit:""`
	BrokerRx                      otm.Int64Gauge `name:"rdkafka.broker.rx" description:"Total number of responses received" unit:""`
	BrokerRxbytes                 otm.Int64Gauge `name:"rdkafka.broker.rxbytes" description:"Total number of bytes received" unit:""`
	BrokerRxerrs                  otm.Int64Gauge `name:"rdkafka.broker.rxerrs" description:"Total number of receive errors" unit:""`
	BrokerRxcorriderrs            otm.Int64Gauge `name:"rdkafka.broker.rxcorriderrs" description:"Total number of unmatched correlation ids in response (typically for timed out requests)" unit:""`
	BrokerRxpartial               otm.Int64Gauge `name:"rdkafka.broker.rxpartial" description:"Total number of partial MessageSets received. The broker may return partial responses if the full MessageSet could not fit in the remaining Fetch response size." unit:""`
	BrokerRxidle                  otm.Int64Gauge `name:"rdkafka.broker.rxidle" description:"Microseconds since last socket receive (or -1 if no receives yet for current connection)." unit:""`
	BrokerReq                     otm.Int64Gauge `name:"rdkafka.broker.req" description:"Request type counters. Object key is the request name, value is the number of requests sent." unit:""`
	BrokerZbufGrow                otm.Int64Gauge `name:"rdkafka.broker.zbuf_grow" description:"Total number of decompression buffer size increases" unit:""`
	BrokerBufGrow                 otm.Int64Gauge `name:"rdkafka.broker.buf_grow" description:"Total number of buffer size increases (deprecated, unused)" unit:""`
	BrokerWakeups                 otm.Int64Gauge `name:"rdkafka.broker.wakeups" description:"Broker thread poll loop wakeups" unit:""`
	BrokerConnects                otm.Int64Gauge `name:"rdkafka.broker.connects" description:"Number of connection attempts, including successful and failed, and name resolution failures." unit:""`
	BrokerDisconnects             otm.Int64Gauge `name:"rdkafka.broker.disconnects" description:"Number of disconnects (triggered by broker, network, load-balancer, etc.)." unit:""`
	BrokerIntLatencyMin           otm.Int64Gauge `name:"rdkafka.broker.int_latency.min" description:"gauge		Internal producer queue latency in microseconds. Smallest value" unit:""`
	BrokerIntLatencyMax           otm.Int64Gauge `name:"rdkafka.broker.int_latency.max" description:"gauge		Internal producer queue latency in microseconds. Largest value" unit:""`
	BrokerIntLatencyAvg           otm.Int64Gauge `name:"rdkafka.broker.int_latency.avg" description:"gauge		Internal producer queue latency in microseconds. Average value" unit:""`
	BrokerIntLatencySum           otm.Int64Gauge `name:"rdkafka.broker.int_latency.sum" description:"gauge		Internal producer queue latency in microseconds. Sum of values" unit:""`
	BrokerIntLatencyCnt           otm.Int64Gauge `name:"rdkafka.broker.int_latency.cnt" description:"gauge		Internal producer queue latency in microseconds. Number of values sampled" unit:""`
	BrokerIntLatencyStddev        otm.Int64Gauge `name:"rdkafka.broker.int_latency.stddev" description:"gauge		Internal producer queue latency in microseconds. Standard deviation (based on histogram)" unit:""`
	BrokerIntLatencyHdrsize       otm.Int64Gauge `name:"rdkafka.broker.int_latency.hdrsize" description:"gauge		Internal producer queue latency in microseconds. Memory size of Hdr Histogram" unit:""`
	BrokerIntLatencyP50           otm.Int64Gauge `name:"rdkafka.broker.int_latency.p50" description:"gauge		Internal producer queue latency in microseconds. 50th percentile" unit:""`
	BrokerIntLatencyP75           otm.Int64Gauge `name:"rdkafka.broker.int_latency.p75" description:"gauge		Internal producer queue latency in microseconds. 75th percentile" unit:""`
	BrokerIntLatencyP90           otm.Int64Gauge `name:"rdkafka.broker.int_latency.p90" description:"gauge		Internal producer queue latency in microseconds. 90th percentile" unit:""`
	BrokerIntLatencyP95           otm.Int64Gauge `name:"rdkafka.broker.int_latency.p95" description:"gauge		Internal producer queue latency in microseconds. 95th percentile" unit:""`
	BrokerIntLatencyP99           otm.Int64Gauge `name:"rdkafka.broker.int_latency.p99" description:"gauge		Internal producer queue latency in microseconds. 99th percentile" unit:""`
	BrokerIntLatencyP99_99        otm.Int64Gauge `name:"rdkafka.broker.int_latency.p99_99" description:"gauge		Internal producer queue latency in microseconds. 99.99th percentile" unit:""`
	BrokerIntLatencyOutofrange    otm.Int64Gauge `name:"rdkafka.broker.int_latency.outofrange" description:"gauge		Internal producer queue latency in microseconds. Values skipped due to out of histogram range" unit:""`
	BrokerOutbufLatencyMin        otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.min" description:"gauge		Internal request queue latency in microseconds. Smallest value" unit:""`
	BrokerOutbufLatencyMax        otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.max" description:"gauge		Internal request queue latency in microseconds. Largest value" unit:""`
	BrokerOutbufLatencyAvg        otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.avg" description:"gauge		Internal request queue latency in microseconds. Average value" unit:""`
	BrokerOutbufLatencySum        otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.sum" description:"gauge		Internal request queue latency in microseconds. Sum of values" unit:""`
	BrokerOutbufLatencyCnt        otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.cnt" description:"gauge		Internal request queue latency in microseconds. Number of values sampled" unit:""`
	BrokerOutbufLatencyStddev     otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.stddev" description:"gauge		Internal request queue latency in microseconds. Standard deviation (based on histogram)" unit:""`
	BrokerOutbufLatencyHdrsize    otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.hdrsize" description:"gauge		Internal request queue latency in microseconds. Memory size of Hdr Histogram" unit:""`
	BrokerOutbufLatencyP50        otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.p50" description:"gauge		Internal request queue latency in microseconds. 50th percentile" unit:""`
	BrokerOutbufLatencyP75        otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.p75" description:"gauge		Internal request queue latency in microseconds. 75th percentile" unit:""`
	BrokerOutbufLatencyP90        otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.p90" description:"gauge		Internal request queue latency in microseconds. 90th percentile" unit:""`
	BrokerOutbufLatencyP95        otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.p95" description:"gauge		Internal request queue latency in microseconds. 95th percentile" unit:""`
	BrokerOutbufLatencyP99        otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.p99" description:"gauge		Internal request queue latency in microseconds. 99th percentile" unit:""`
	BrokerOutbufLatencyP99_99     otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.p99_99" description:"gauge		Internal request queue latency in microseconds. 99.99th percentile" unit:""`
	BrokerOutbufLatencyOutofrange otm.Int64Gauge `name:"rdkafka.broker.outbuf_latency.outofrange" description:"gauge		Internal request queue latency in microseconds. Values skipped due to out of histogram range" unit:""`
	BrokerRttMin                  otm.Int64Gauge `name:"rdkafka.broker.rtt.min" description:"gauge		Broker latency / round-trip time in microseconds. Smallest value" unit:""`
	BrokerRttMax                  otm.Int64Gauge `name:"rdkafka.broker.rtt.max" description:"gauge		Broker latency / round-trip time in microseconds. Largest value" unit:""`
	BrokerRttAvg                  otm.Int64Gauge `name:"rdkafka.broker.rtt.avg" description:"gauge		Broker latency / round-trip time in microseconds. Average value" unit:""`
	BrokerRttSum                  otm.Int64Gauge `name:"rdkafka.broker.rtt.sum" description:"gauge		Broker latency / round-trip time in microseconds. Sum of values" unit:""`
	BrokerRttCnt                  otm.Int64Gauge `name:"rdkafka.broker.rtt.cnt" description:"gauge		Broker latency / round-trip time in microseconds. Number of values sampled" unit:""`
	BrokerRttStddev               otm.Int64Gauge `name:"rdkafka.broker.rtt.stddev" description:"gauge		Broker latency / round-trip time in microseconds. Standard deviation (based on histogram)" unit:""`
	BrokerRttHdrsize              otm.Int64Gauge `name:"rdkafka.broker.rtt.hdrsize" description:"gauge		Broker latency / round-trip time in microseconds. Memory size of Hdr Histogram" unit:""`
	BrokerRttP50                  otm.Int64Gauge `name:"rdkafka.broker.rtt.p50" description:"gauge		Broker latency / round-trip time in microseconds. 50th percentile" unit:""`
	BrokerRttP75                  otm.Int64Gauge `name:"rdkafka.broker.rtt.p75" description:"gauge		Broker latency / round-trip time in microseconds. 75th percentile" unit:""`
	BrokerRttP90                  otm.Int64Gauge `name:"rdkafka.broker.rtt.p90" description:"gauge		Broker latency / round-trip time in microseconds. 90th percentile" unit:""`
	BrokerRttP95                  otm.Int64Gauge `name:"rdkafka.broker.rtt.p95" description:"gauge		Broker latency / round-trip time in microseconds. 95th percentile" unit:""`
	BrokerRttP99                  otm.Int64Gauge `name:"rdkafka.broker.rtt.p99" description:"gauge		Broker latency / round-trip time in microseconds. 99th percentile" unit:""`
	BrokerRttP99_99               otm.Int64Gauge `name:"rdkafka.broker.rtt.p99_99" description:"gauge		Broker latency / round-trip time in microseconds. 99.99th percentile" unit:""`
	BrokerRttOutofrange           otm.Int64Gauge `name:"rdkafka.broker.rtt.outofrange" description:"gauge		Broker latency / round-trip time in microseconds. Values skipped due to out of histogram range" unit:""`
	BrokerThrottleMin             otm.Int64Gauge `name:"rdkafka.broker.throttle.min" description:"gauge		Broker throttling time in milliseconds. Smallest value" unit:""`
	BrokerThrottleMax             otm.Int64Gauge `name:"rdkafka.broker.throttle.max" description:"gauge		Broker throttling time in milliseconds. Largest value" unit:""`
	BrokerThrottleAvg             otm.Int64Gauge `name:"rdkafka.broker.throttle.avg" description:"gauge		Broker throttling time in milliseconds. Average value" unit:""`
	BrokerThrottleSum             otm.Int64Gauge `name:"rdkafka.broker.throttle.sum" description:"gauge		Broker throttling time in milliseconds. Sum of values" unit:""`
	BrokerThrottleCnt             otm.Int64Gauge `name:"rdkafka.broker.throttle.cnt" description:"gauge		Broker throttling time in milliseconds. Number of values sampled" unit:""`
	BrokerThrottleStddev          otm.Int64Gauge `name:"rdkafka.broker.throttle.stddev" description:"gauge		Broker throttling time in milliseconds. Standard deviation (based on histogram)" unit:""`
	BrokerThrottleHdrsize         otm.Int64Gauge `name:"rdkafka.broker.throttle.hdrsize" description:"gauge		Broker throttling time in milliseconds. Memory size of Hdr Histogram" unit:""`
	BrokerThrottleP50             otm.Int64Gauge `name:"rdkafka.broker.throttle.p50" description:"gauge		Broker throttling time in milliseconds. 50th percentile" unit:""`
	BrokerThrottleP75             otm.Int64Gauge `name:"rdkafka.broker.throttle.p75" description:"gauge		Broker throttling time in milliseconds. 75th percentile" unit:""`
	BrokerThrottleP90             otm.Int64Gauge `name:"rdkafka.broker.throttle.p90" description:"gauge		Broker throttling time in milliseconds. 90th percentile" unit:""`
	BrokerThrottleP95             otm.Int64Gauge `name:"rdkafka.broker.throttle.p95" description:"gauge		Broker throttling time in milliseconds. 95th percentile" unit:""`
	BrokerThrottleP99             otm.Int64Gauge `name:"rdkafka.broker.throttle.p99" description:"gauge		Broker throttling time in milliseconds. 99th percentile" unit:""`
	BrokerThrottleP99_99          otm.Int64Gauge `name:"rdkafka.broker.throttle.p99_99" description:"gauge		Broker throttling time in milliseconds. 99.99th percentile" unit:""`
	BrokerThrottleOutofrange      otm.Int64Gauge `name:"rdkafka.broker.throttle.outofrange" description:"gauge		Broker throttling time in milliseconds. Values skipped due to out of histogram range" unit:""`
	TopicAge                      otm.Int64Gauge `name:"rdkafka.topic.age" description:"gauge		Age of client's topic object (milliseconds)" unit:""`
	TopicMetadataAge              otm.Int64Gauge `name:"rdkafka.topic.metadata_age" description:"gauge		Age of metadata from broker for this topic (milliseconds)" unit:""`
	TopicBatchsizeMin             otm.Int64Gauge `name:"rdkafka.topic.batchsize.min" description:"gauge		Batch sizes in bytes. Smallest value" unit:""`
	TopicBatchsizeMax             otm.Int64Gauge `name:"rdkafka.topic.batchsize.max" description:"gauge		Batch sizes in bytes. Largest value" unit:""`
	TopicBatchsizeAvg             otm.Int64Gauge `name:"rdkafka.topic.batchsize.avg" description:"gauge		Batch sizes in bytes. Average value" unit:""`
	TopicBatchsizeSum             otm.Int64Gauge `name:"rdkafka.topic.batchsize.sum" description:"gauge		Batch sizes in bytes. Sum of values" unit:""`
	TopicBatchsizeCnt             otm.Int64Gauge `name:"rdkafka.topic.batchsize.cnt" description:"gauge		Batch sizes in bytes. Number of values sampled" unit:""`
	TopicBatchsizeStddev          otm.Int64Gauge `name:"rdkafka.topic.batchsize.stddev" description:"gauge		Batch sizes in bytes. Standard deviation (based on histogram)" unit:""`
	TopicBatchsizeHdrsize         otm.Int64Gauge `name:"rdkafka.topic.batchsize.hdrsize" description:"gauge		Batch sizes in bytes. Memory size of Hdr Histogram" unit:""`
	TopicBatchsizeP50             otm.Int64Gauge `name:"rdkafka.topic.batchsize.p50" description:"gauge		Batch sizes in bytes. 50th percentile" unit:""`
	TopicBatchsizeP75             otm.Int64Gauge `name:"rdkafka.topic.batchsize.p75" description:"gauge		Batch sizes in bytes. 75th percentile" unit:""`
	TopicBatchsizeP90             otm.Int64Gauge `name:"rdkafka.topic.batchsize.p90" description:"gauge		Batch sizes in bytes. 90th percentile" unit:""`
	TopicBatchsizeP95             otm.Int64Gauge `name:"rdkafka.topic.batchsize.p95" description:"gauge		Batch sizes in bytes. 95th percentile" unit:""`
	TopicBatchsizeP99             otm.Int64Gauge `name:"rdkafka.topic.batchsize.p99" description:"gauge		Batch sizes in bytes. 99th percentile" unit:""`
	TopicBatchsizeP99_99          otm.Int64Gauge `name:"rdkafka.topic.batchsize.p99_99" description:"gauge		Batch sizes in bytes. 99.99th percentile" unit:""`
	TopicBatchsizeOutofrange      otm.Int64Gauge `name:"rdkafka.topic.batchsize.outofrange" description:"gauge		Batch sizes in bytes. Values skipped due to out of histogram range" unit:""`
	TopicBatchcntMin              otm.Int64Gauge `name:"rdkafka.topic.batchcnt.min" description:"gauge		Batch message counts. Smallest value" unit:""`
	TopicBatchcntMax              otm.Int64Gauge `name:"rdkafka.topic.batchcnt.max" description:"gauge		Batch message counts. Largest value" unit:""`
	TopicBatchcntAvg              otm.Int64Gauge `name:"rdkafka.topic.batchcnt.avg" description:"gauge		Batch message counts. Average value" unit:""`
	TopicBatchcntSum              otm.Int64Gauge `name:"rdkafka.topic.batchcnt.sum" description:"gauge		Batch message counts. Sum of values" unit:""`
	TopicBatchcntCnt              otm.Int64Gauge `name:"rdkafka.topic.batchcnt.cnt" description:"gauge		Batch message counts. Number of values sampled" unit:""`
	TopicBatchcntStddev           otm.Int64Gauge `name:"rdkafka.topic.batchcnt.stddev" description:"gauge		Batch message counts. Standard deviation (based on histogram)" unit:""`
	TopicBatchcntHdrsize          otm.Int64Gauge `name:"rdkafka.topic.batchcnt.hdrsize" description:"gauge		Batch message counts. Memory size of Hdr Histogram" unit:""`
	TopicBatchcntP50              otm.Int64Gauge `name:"rdkafka.topic.batchcnt.p50" description:"gauge		Batch message counts. 50th percentile" unit:""`
	TopicBatchcntP75              otm.Int64Gauge `name:"rdkafka.topic.batchcnt.p75" description:"gauge		Batch message counts. 75th percentile" unit:""`
	TopicBatchcntP90              otm.Int64Gauge `name:"rdkafka.topic.batchcnt.p90" description:"gauge		Batch message counts. 90th percentile" unit:""`
	TopicBatchcntP95              otm.Int64Gauge `name:"rdkafka.topic.batchcnt.p95" description:"gauge		Batch message counts. 95th percentile" unit:""`
	TopicBatchcntP99              otm.Int64Gauge `name:"rdkafka.topic.batchcnt.p99" description:"gauge		Batch message counts. 99th percentile" unit:""`
	TopicBatchcntP99_99           otm.Int64Gauge `name:"rdkafka.topic.batchcnt.p99_99" description:"gauge		Batch message counts. 99.99th percentile" unit:""`
	TopicBatchcntOutofrange       otm.Int64Gauge `name:"rdkafka.topic.batchcnt.outofrange" description:"gauge		Batch message counts. Values skipped due to out of histogram range" unit:""`
	TopicPartitionMsgqCnt         otm.Int64Gauge `name:"rdkafka.topic.partition.msgq_cnt" description:"gauge		Number of messages waiting to be produced in first-level queue" unit:""`
	TopicPartitionMsgqBytes       otm.Int64Gauge `name:"rdkafka.topic.partition.msgq_bytes" description:"gauge		Number of bytes in msgq_cnt" unit:""`
	TopicPartitionXmitMsgqCnt     otm.Int64Gauge `name:"rdkafka.topic.partition.xmit_msgq_cnt" description:"gauge		Number of messages ready to be produced in transmit queue" unit:""`
	TopicPartitionXmitMsgqBytes   otm.Int64Gauge `name:"rdkafka.topic.partition.xmit_msgq_bytes" description:"gauge		Number of bytes in xmit_msgq" unit:""`
	TopicPartitionTxmsgs          otm.Int64Gauge `name:"rdkafka.topic.partition.txmsgs" description:"Total number of messages transmitted (produced)" unit:""`
	TopicPartitionTxbytes         otm.Int64Gauge `name:"rdkafka.topic.partition.txbytes" description:"Total number of bytes transmitted for txmsgs" unit:""`
	TopicPartitionMsgs            otm.Int64Gauge `name:"rdkafka.topic.partition.msgs" description:"Total number of messages received (consumer, same as rxmsgs), or total number of messages produced (possibly not yet transmitted) (producer)." unit:""`
	TopicPartitionRxVerDrops      otm.Int64Gauge `name:"rdkafka.topic.partition.rx_ver_drops" description:"Dropped outdated messages" unit:""`
	TopicPartitionMsgsInflight    otm.Int64Gauge `name:"rdkafka.topic.partition.msgs_inflight" description:"gauge		Current number of messages in-flight to/from broker" unit:""`
	InternalQueueLen              otm.Int64Gauge `name:"rdkafka.internal.queue.len" description:"Total messages/events in rdkafka and go client queues" unit:""`
	AsyncResultQueueLen           otm.Int64Gauge `name:"rdkafka.async.result.queue.len" description:"Total sent message results that are waiting to be processed" unit:""`
}

func (s *service) RecordRdkMetrics(statsJson string, rdkLen, asyncLen int) {
	rdkStats := &rdKafkaStats{}
	if err := json.Unmarshal([]byte(statsJson), rdkStats); err != nil {
		slog.Error("Failed to parse rdkafka stats.", "error", err.Error())
		return
	}

	ctx := context.Background()

	nameAttribute := attribute.String("name", *rdkStats.Name)
	clientIdAttribute := attribute.String("client_id", *rdkStats.ClientId)
	topLevelAttributes := otm.WithAttributes(nameAttribute, clientIdAttribute)

	if rdkStats.Age != nil {
		s.meters.rdk.Age.Record(ctx, *rdkStats.Age, topLevelAttributes)
	}
	if rdkStats.Replyq != nil {
		s.meters.rdk.Replyq.Record(ctx, *rdkStats.Replyq, topLevelAttributes)
	}
	if rdkStats.MsgCnt != nil {
		s.meters.rdk.MsgCnt.Record(ctx, *rdkStats.MsgCnt, topLevelAttributes)
	}
	if rdkStats.MsgSize != nil {
		s.meters.rdk.MsgSize.Record(ctx, *rdkStats.MsgSize, topLevelAttributes)
	}
	if rdkStats.MsgMax != nil {
		s.meters.rdk.MsgMax.Record(ctx, *rdkStats.MsgMax, topLevelAttributes)
	}
	if rdkStats.MsgSizeMax != nil {
		s.meters.rdk.MsgSizeMax.Record(ctx, *rdkStats.MsgSizeMax, topLevelAttributes)
	}
	if rdkStats.Tx != nil {
		s.meters.rdk.Tx.Record(ctx, *rdkStats.Tx, topLevelAttributes)
	}
	if rdkStats.TxBytes != nil {
		s.meters.rdk.TxBytes.Record(ctx, *rdkStats.TxBytes, topLevelAttributes)
	}
	if rdkStats.Rx != nil {
		s.meters.rdk.Rx.Record(ctx, *rdkStats.Rx, topLevelAttributes)
	}
	if rdkStats.RxBytes != nil {
		s.meters.rdk.RxBytes.Record(ctx, *rdkStats.RxBytes, topLevelAttributes)
	}
	if rdkStats.Txmsgs != nil {
		s.meters.rdk.Txmsgs.Record(ctx, *rdkStats.Txmsgs, topLevelAttributes)
	}
	if rdkStats.TxmsgBytes != nil {
		s.meters.rdk.TxmsgBytes.Record(ctx, *rdkStats.TxmsgBytes, topLevelAttributes)
	}
	if rdkStats.Rxmsgs != nil {
		s.meters.rdk.Rxmsgs.Record(ctx, *rdkStats.Rxmsgs, topLevelAttributes)
	}
	if rdkStats.RxmsgBytes != nil {
		s.meters.rdk.RxmsgBytes.Record(ctx, *rdkStats.RxmsgBytes, topLevelAttributes)
	}
	if rdkStats.SimpleCnt != nil {
		s.meters.rdk.SimpleCnt.Record(ctx, *rdkStats.SimpleCnt, topLevelAttributes)
	}
	if rdkStats.MetadataCacheCnt != nil {
		s.meters.rdk.MetadataCacheCnt.Record(ctx, *rdkStats.MetadataCacheCnt, topLevelAttributes)
	}

	for _, bStats := range rdkStats.Brokers {
		nodeIdAttribute := attribute.Int64("node_id", *bStats.Nodeid)
		nodeNameAttribute := attribute.String("node_name", *bStats.Nodename)
		brokerAttributes := otm.WithAttributes(nameAttribute, clientIdAttribute, nodeIdAttribute, nodeNameAttribute)

		if bStats.Stateage != nil {
			s.meters.rdk.BrokerStateage.Record(ctx, *bStats.Stateage, brokerAttributes)
		}
		if bStats.OutbufCnt != nil {
			s.meters.rdk.BrokerOutbufCnt.Record(ctx, *bStats.OutbufCnt, brokerAttributes)
		}
		if bStats.OutbufMsgCnt != nil {
			s.meters.rdk.BrokerOutbufMsgCnt.Record(ctx, *bStats.OutbufMsgCnt, brokerAttributes)
		}
		if bStats.WaitrespCnt != nil {
			s.meters.rdk.BrokerWaitrespCnt.Record(ctx, *bStats.WaitrespCnt, brokerAttributes)
		}
		if bStats.WaitrespMsgCnt != nil {
			s.meters.rdk.BrokerWaitrespMsgCnt.Record(ctx, *bStats.WaitrespMsgCnt, brokerAttributes)
		}
		if bStats.Tx != nil {
			s.meters.rdk.BrokerTx.Record(ctx, *bStats.Tx, brokerAttributes)
		}
		if bStats.Txbytes != nil {
			s.meters.rdk.BrokerTxbytes.Record(ctx, *bStats.Txbytes, brokerAttributes)
		}
		if bStats.Txerrs != nil {
			s.meters.rdk.BrokerTxerrs.Record(ctx, *bStats.Txerrs, brokerAttributes)
		}
		if bStats.Txretries != nil {
			s.meters.rdk.BrokerTxretries.Record(ctx, *bStats.Txretries, brokerAttributes)
		}
		if bStats.Txidle != nil {
			s.meters.rdk.BrokerTxidle.Record(ctx, *bStats.Txidle, brokerAttributes)
		}
		if bStats.ReqTimeouts != nil {
			s.meters.rdk.BrokerReqTimeouts.Record(ctx, *bStats.ReqTimeouts, brokerAttributes)
		}
		if bStats.Rx != nil {
			s.meters.rdk.BrokerRx.Record(ctx, *bStats.Rx, brokerAttributes)
		}
		if bStats.Rxbytes != nil {
			s.meters.rdk.BrokerRxbytes.Record(ctx, *bStats.Rxbytes, brokerAttributes)
		}
		if bStats.Rxerrs != nil {
			s.meters.rdk.BrokerRxerrs.Record(ctx, *bStats.Rxerrs, brokerAttributes)
		}
		if bStats.Rxcorriderrs != nil {
			s.meters.rdk.BrokerRxcorriderrs.Record(ctx, *bStats.Rxcorriderrs, brokerAttributes)
		}
		if bStats.Rxpartial != nil {
			s.meters.rdk.BrokerRxpartial.Record(ctx, *bStats.Rxpartial, brokerAttributes)
		}
		if bStats.Rxidle != nil {
			s.meters.rdk.BrokerRxidle.Record(ctx, *bStats.Rxidle, brokerAttributes)
		}
		if bStats.ZbufGrow != nil {
			s.meters.rdk.BrokerZbufGrow.Record(ctx, *bStats.ZbufGrow, brokerAttributes)
		}
		if bStats.BufGrow != nil {
			s.meters.rdk.BrokerBufGrow.Record(ctx, *bStats.BufGrow, brokerAttributes)
		}
		if bStats.Wakeups != nil {
			s.meters.rdk.BrokerWakeups.Record(ctx, *bStats.Wakeups, brokerAttributes)
		}
		if bStats.Connects != nil {
			s.meters.rdk.BrokerConnects.Record(ctx, *bStats.Connects, brokerAttributes)
		}
		if bStats.Disconnects != nil {
			s.meters.rdk.BrokerDisconnects.Record(ctx, *bStats.Disconnects, brokerAttributes)
		}

		if bStats.IntLatency.Min != nil {
			s.meters.rdk.BrokerIntLatencyMin.Record(ctx, *bStats.IntLatency.Min, brokerAttributes)
		}
		if bStats.IntLatency.Max != nil {
			s.meters.rdk.BrokerIntLatencyMax.Record(ctx, *bStats.IntLatency.Max, brokerAttributes)
		}
		if bStats.IntLatency.Avg != nil {
			s.meters.rdk.BrokerIntLatencyAvg.Record(ctx, *bStats.IntLatency.Avg, brokerAttributes)
		}
		if bStats.IntLatency.Sum != nil {
			s.meters.rdk.BrokerIntLatencySum.Record(ctx, *bStats.IntLatency.Sum, brokerAttributes)
		}
		if bStats.IntLatency.Cnt != nil {
			s.meters.rdk.BrokerIntLatencyCnt.Record(ctx, *bStats.IntLatency.Cnt, brokerAttributes)
		}
		if bStats.IntLatency.Stddev != nil {
			s.meters.rdk.BrokerIntLatencyStddev.Record(ctx, *bStats.IntLatency.Stddev, brokerAttributes)
		}
		if bStats.IntLatency.Hdrsize != nil {
			s.meters.rdk.BrokerIntLatencyHdrsize.Record(ctx, *bStats.IntLatency.Hdrsize, brokerAttributes)
		}
		if bStats.IntLatency.P50 != nil {
			s.meters.rdk.BrokerIntLatencyP50.Record(ctx, *bStats.IntLatency.P50, brokerAttributes)
		}
		if bStats.IntLatency.P75 != nil {
			s.meters.rdk.BrokerIntLatencyP75.Record(ctx, *bStats.IntLatency.P75, brokerAttributes)
		}
		if bStats.IntLatency.P90 != nil {
			s.meters.rdk.BrokerIntLatencyP90.Record(ctx, *bStats.IntLatency.P90, brokerAttributes)
		}
		if bStats.IntLatency.P95 != nil {
			s.meters.rdk.BrokerIntLatencyP95.Record(ctx, *bStats.IntLatency.P95, brokerAttributes)
		}
		if bStats.IntLatency.P99 != nil {
			s.meters.rdk.BrokerIntLatencyP99.Record(ctx, *bStats.IntLatency.P99, brokerAttributes)
		}
		if bStats.IntLatency.P99_99 != nil {
			s.meters.rdk.BrokerIntLatencyP99_99.Record(ctx, *bStats.IntLatency.P99_99, brokerAttributes)
		}
		if bStats.IntLatency.Outofrange != nil {
			s.meters.rdk.BrokerIntLatencyOutofrange.Record(ctx, *bStats.IntLatency.Outofrange, brokerAttributes)
		}
		if bStats.OutbufLatency.Min != nil {
			s.meters.rdk.BrokerOutbufLatencyMin.Record(ctx, *bStats.OutbufLatency.Min, brokerAttributes)
		}
		if bStats.OutbufLatency.Max != nil {
			s.meters.rdk.BrokerOutbufLatencyMax.Record(ctx, *bStats.OutbufLatency.Max, brokerAttributes)
		}
		if bStats.OutbufLatency.Avg != nil {
			s.meters.rdk.BrokerOutbufLatencyAvg.Record(ctx, *bStats.OutbufLatency.Avg, brokerAttributes)
		}
		if bStats.OutbufLatency.Sum != nil {
			s.meters.rdk.BrokerOutbufLatencySum.Record(ctx, *bStats.OutbufLatency.Sum, brokerAttributes)
		}
		if bStats.OutbufLatency.Cnt != nil {
			s.meters.rdk.BrokerOutbufLatencyCnt.Record(ctx, *bStats.OutbufLatency.Cnt, brokerAttributes)
		}
		if bStats.OutbufLatency.Stddev != nil {
			s.meters.rdk.BrokerOutbufLatencyStddev.Record(ctx, *bStats.OutbufLatency.Stddev, brokerAttributes)
		}
		if bStats.OutbufLatency.Hdrsize != nil {
			s.meters.rdk.BrokerOutbufLatencyHdrsize.Record(ctx, *bStats.OutbufLatency.Hdrsize, brokerAttributes)
		}
		if bStats.OutbufLatency.P50 != nil {
			s.meters.rdk.BrokerOutbufLatencyP50.Record(ctx, *bStats.OutbufLatency.P50, brokerAttributes)
		}
		if bStats.OutbufLatency.P75 != nil {
			s.meters.rdk.BrokerOutbufLatencyP75.Record(ctx, *bStats.OutbufLatency.P75, brokerAttributes)
		}
		if bStats.OutbufLatency.P90 != nil {
			s.meters.rdk.BrokerOutbufLatencyP90.Record(ctx, *bStats.OutbufLatency.P90, brokerAttributes)
		}
		if bStats.OutbufLatency.P95 != nil {
			s.meters.rdk.BrokerOutbufLatencyP95.Record(ctx, *bStats.OutbufLatency.P95, brokerAttributes)
		}
		if bStats.OutbufLatency.P99 != nil {
			s.meters.rdk.BrokerOutbufLatencyP99.Record(ctx, *bStats.OutbufLatency.P99, brokerAttributes)
		}
		if bStats.OutbufLatency.P99_99 != nil {
			s.meters.rdk.BrokerOutbufLatencyP99_99.Record(ctx, *bStats.OutbufLatency.P99_99, brokerAttributes)
		}
		if bStats.OutbufLatency.Outofrange != nil {
			s.meters.rdk.BrokerOutbufLatencyOutofrange.Record(ctx, *bStats.OutbufLatency.Outofrange, brokerAttributes)
		}
		if bStats.Rtt.Min != nil {
			s.meters.rdk.BrokerRttMin.Record(ctx, *bStats.Rtt.Min, brokerAttributes)
		}
		if bStats.Rtt.Max != nil {
			s.meters.rdk.BrokerRttMax.Record(ctx, *bStats.Rtt.Max, brokerAttributes)
		}
		if bStats.Rtt.Avg != nil {
			s.meters.rdk.BrokerRttAvg.Record(ctx, *bStats.Rtt.Avg, brokerAttributes)
		}
		if bStats.Rtt.Sum != nil {
			s.meters.rdk.BrokerRttSum.Record(ctx, *bStats.Rtt.Sum, brokerAttributes)
		}
		if bStats.Rtt.Cnt != nil {
			s.meters.rdk.BrokerRttCnt.Record(ctx, *bStats.Rtt.Cnt, brokerAttributes)
		}
		if bStats.Rtt.Stddev != nil {
			s.meters.rdk.BrokerRttStddev.Record(ctx, *bStats.Rtt.Stddev, brokerAttributes)
		}
		if bStats.Rtt.Hdrsize != nil {
			s.meters.rdk.BrokerRttHdrsize.Record(ctx, *bStats.Rtt.Hdrsize, brokerAttributes)
		}
		if bStats.Rtt.P50 != nil {
			s.meters.rdk.BrokerRttP50.Record(ctx, *bStats.Rtt.P50, brokerAttributes)
		}
		if bStats.Rtt.P75 != nil {
			s.meters.rdk.BrokerRttP75.Record(ctx, *bStats.Rtt.P75, brokerAttributes)
		}
		if bStats.Rtt.P90 != nil {
			s.meters.rdk.BrokerRttP90.Record(ctx, *bStats.Rtt.P90, brokerAttributes)
		}
		if bStats.Rtt.P95 != nil {
			s.meters.rdk.BrokerRttP95.Record(ctx, *bStats.Rtt.P95, brokerAttributes)
		}
		if bStats.Rtt.P99 != nil {
			s.meters.rdk.BrokerRttP99.Record(ctx, *bStats.Rtt.P99, brokerAttributes)
		}
		if bStats.Rtt.P99_99 != nil {
			s.meters.rdk.BrokerRttP99_99.Record(ctx, *bStats.Rtt.P99_99, brokerAttributes)
		}
		if bStats.Rtt.Outofrange != nil {
			s.meters.rdk.BrokerRttOutofrange.Record(ctx, *bStats.Rtt.Outofrange, brokerAttributes)
		}
		if bStats.Throttle.Min != nil {
			s.meters.rdk.BrokerThrottleMin.Record(ctx, *bStats.Throttle.Min, brokerAttributes)
		}
		if bStats.Throttle.Max != nil {
			s.meters.rdk.BrokerThrottleMax.Record(ctx, *bStats.Throttle.Max, brokerAttributes)
		}
		if bStats.Throttle.Avg != nil {
			s.meters.rdk.BrokerThrottleAvg.Record(ctx, *bStats.Throttle.Avg, brokerAttributes)
		}
		if bStats.Throttle.Sum != nil {
			s.meters.rdk.BrokerThrottleSum.Record(ctx, *bStats.Throttle.Sum, brokerAttributes)
		}
		if bStats.Throttle.Cnt != nil {
			s.meters.rdk.BrokerThrottleCnt.Record(ctx, *bStats.Throttle.Cnt, brokerAttributes)
		}
		if bStats.Throttle.Stddev != nil {
			s.meters.rdk.BrokerThrottleStddev.Record(ctx, *bStats.Throttle.Stddev, brokerAttributes)
		}
		if bStats.Throttle.Hdrsize != nil {
			s.meters.rdk.BrokerThrottleHdrsize.Record(ctx, *bStats.Throttle.Hdrsize, brokerAttributes)
		}
		if bStats.Throttle.P50 != nil {
			s.meters.rdk.BrokerThrottleP50.Record(ctx, *bStats.Throttle.P50, brokerAttributes)
		}
		if bStats.Throttle.P75 != nil {
			s.meters.rdk.BrokerThrottleP75.Record(ctx, *bStats.Throttle.P75, brokerAttributes)
		}
		if bStats.Throttle.P90 != nil {
			s.meters.rdk.BrokerThrottleP90.Record(ctx, *bStats.Throttle.P90, brokerAttributes)
		}
		if bStats.Throttle.P95 != nil {
			s.meters.rdk.BrokerThrottleP95.Record(ctx, *bStats.Throttle.P95, brokerAttributes)
		}
		if bStats.Throttle.P99 != nil {
			s.meters.rdk.BrokerThrottleP99.Record(ctx, *bStats.Throttle.P99, brokerAttributes)
		}
		if bStats.Throttle.P99_99 != nil {
			s.meters.rdk.BrokerThrottleP99_99.Record(ctx, *bStats.Throttle.P99_99, brokerAttributes)
		}
		if bStats.Throttle.Outofrange != nil {
			s.meters.rdk.BrokerThrottleOutofrange.Record(ctx, *bStats.Throttle.Outofrange, brokerAttributes)
		}

		for req, val := range bStats.Req {
			requestNameAttribute := attribute.String("request_name", req)
			requestAttributes := otm.WithAttributes(nameAttribute, clientIdAttribute, nodeIdAttribute, nodeNameAttribute, requestNameAttribute)
			s.meters.rdk.BrokerReq.Record(ctx, val, requestAttributes)
		}
	}

	for _, tStats := range rdkStats.Topics {
		topicAttribute := attribute.String("topic", *tStats.Topic)
		topicAttributes := otm.WithAttributes(nameAttribute, clientIdAttribute, topicAttribute)

		if tStats.Age != nil {
			s.meters.rdk.TopicAge.Record(ctx, *tStats.Age, topicAttributes)
		}
		if tStats.MetadataAge != nil {
			s.meters.rdk.TopicMetadataAge.Record(ctx, *tStats.MetadataAge, topicAttributes)
		}
		if tStats.Batchsize.Min != nil {
			s.meters.rdk.TopicBatchsizeMin.Record(ctx, *tStats.Batchsize.Min, topicAttributes)
		}
		if tStats.Batchsize.Max != nil {
			s.meters.rdk.TopicBatchsizeMax.Record(ctx, *tStats.Batchsize.Max, topicAttributes)
		}
		if tStats.Batchsize.Avg != nil {
			s.meters.rdk.TopicBatchsizeAvg.Record(ctx, *tStats.Batchsize.Avg, topicAttributes)
		}
		if tStats.Batchsize.Sum != nil {
			s.meters.rdk.TopicBatchsizeSum.Record(ctx, *tStats.Batchsize.Sum, topicAttributes)
		}
		if tStats.Batchsize.Cnt != nil {
			s.meters.rdk.TopicBatchsizeCnt.Record(ctx, *tStats.Batchsize.Cnt, topicAttributes)
		}
		if tStats.Batchsize.Stddev != nil {
			s.meters.rdk.TopicBatchsizeStddev.Record(ctx, *tStats.Batchsize.Stddev, topicAttributes)
		}
		if tStats.Batchsize.Hdrsize != nil {
			s.meters.rdk.TopicBatchsizeHdrsize.Record(ctx, *tStats.Batchsize.Hdrsize, topicAttributes)
		}
		if tStats.Batchsize.P50 != nil {
			s.meters.rdk.TopicBatchsizeP50.Record(ctx, *tStats.Batchsize.P50, topicAttributes)
		}
		if tStats.Batchsize.P75 != nil {
			s.meters.rdk.TopicBatchsizeP75.Record(ctx, *tStats.Batchsize.P75, topicAttributes)
		}
		if tStats.Batchsize.P90 != nil {
			s.meters.rdk.TopicBatchsizeP90.Record(ctx, *tStats.Batchsize.P90, topicAttributes)
		}
		if tStats.Batchsize.P95 != nil {
			s.meters.rdk.TopicBatchsizeP95.Record(ctx, *tStats.Batchsize.P95, topicAttributes)
		}
		if tStats.Batchsize.P99 != nil {
			s.meters.rdk.TopicBatchsizeP99.Record(ctx, *tStats.Batchsize.P99, topicAttributes)
		}
		if tStats.Batchsize.P99_99 != nil {
			s.meters.rdk.TopicBatchsizeP99_99.Record(ctx, *tStats.Batchsize.P99_99, topicAttributes)
		}
		if tStats.Batchsize.Outofrange != nil {
			s.meters.rdk.TopicBatchsizeOutofrange.Record(ctx, *tStats.Batchsize.Outofrange, topicAttributes)
		}
		if tStats.Batchcnt.Min != nil {
			s.meters.rdk.TopicBatchcntMin.Record(ctx, *tStats.Batchcnt.Min, topicAttributes)
		}
		if tStats.Batchcnt.Max != nil {
			s.meters.rdk.TopicBatchcntMax.Record(ctx, *tStats.Batchcnt.Max, topicAttributes)
		}
		if tStats.Batchcnt.Avg != nil {
			s.meters.rdk.TopicBatchcntAvg.Record(ctx, *tStats.Batchcnt.Avg, topicAttributes)
		}
		if tStats.Batchcnt.Sum != nil {
			s.meters.rdk.TopicBatchcntSum.Record(ctx, *tStats.Batchcnt.Sum, topicAttributes)
		}
		if tStats.Batchcnt.Cnt != nil {
			s.meters.rdk.TopicBatchcntCnt.Record(ctx, *tStats.Batchcnt.Cnt, topicAttributes)
		}
		if tStats.Batchcnt.Stddev != nil {
			s.meters.rdk.TopicBatchcntStddev.Record(ctx, *tStats.Batchcnt.Stddev, topicAttributes)
		}
		if tStats.Batchcnt.Hdrsize != nil {
			s.meters.rdk.TopicBatchcntHdrsize.Record(ctx, *tStats.Batchcnt.Hdrsize, topicAttributes)
		}
		if tStats.Batchcnt.P50 != nil {
			s.meters.rdk.TopicBatchcntP50.Record(ctx, *tStats.Batchcnt.P50, topicAttributes)
		}
		if tStats.Batchcnt.P75 != nil {
			s.meters.rdk.TopicBatchcntP75.Record(ctx, *tStats.Batchcnt.P75, topicAttributes)
		}
		if tStats.Batchcnt.P90 != nil {
			s.meters.rdk.TopicBatchcntP90.Record(ctx, *tStats.Batchcnt.P90, topicAttributes)
		}
		if tStats.Batchcnt.P95 != nil {
			s.meters.rdk.TopicBatchcntP95.Record(ctx, *tStats.Batchcnt.P95, topicAttributes)
		}
		if tStats.Batchcnt.P99 != nil {
			s.meters.rdk.TopicBatchcntP99.Record(ctx, *tStats.Batchcnt.P99, topicAttributes)
		}
		if tStats.Batchcnt.P99_99 != nil {
			s.meters.rdk.TopicBatchcntP99_99.Record(ctx, *tStats.Batchcnt.P99_99, topicAttributes)
		}
		if tStats.Batchcnt.Outofrange != nil {
			s.meters.rdk.TopicBatchcntOutofrange.Record(ctx, *tStats.Batchcnt.Outofrange, topicAttributes)
		}

		for _, pStats := range tStats.Partitions {
			partitionAttribute := attribute.Int64("partition", *pStats.Partition)
			partitionAttributes := otm.WithAttributes(nameAttribute, clientIdAttribute, topicAttribute, partitionAttribute)

			if pStats.MsgqCnt != nil {
				s.meters.rdk.TopicPartitionMsgqCnt.Record(ctx, *pStats.MsgqCnt, partitionAttributes)
			}
			if pStats.MsgqBytes != nil {
				s.meters.rdk.TopicPartitionMsgqBytes.Record(ctx, *pStats.MsgqBytes, partitionAttributes)
			}
			if pStats.XmitMsgqCnt != nil {
				s.meters.rdk.TopicPartitionXmitMsgqCnt.Record(ctx, *pStats.XmitMsgqCnt, partitionAttributes)
			}
			if pStats.XmitMsgqBytes != nil {
				s.meters.rdk.TopicPartitionXmitMsgqBytes.Record(ctx, *pStats.XmitMsgqBytes, partitionAttributes)
			}
			if pStats.Txmsgs != nil {
				s.meters.rdk.TopicPartitionTxmsgs.Record(ctx, *pStats.Txmsgs, partitionAttributes)
			}
			if pStats.Txbytes != nil {
				s.meters.rdk.TopicPartitionTxbytes.Record(ctx, *pStats.Txbytes, partitionAttributes)
			}
			if pStats.Msgs != nil {
				s.meters.rdk.TopicPartitionMsgs.Record(ctx, *pStats.Msgs, partitionAttributes)
			}
			if pStats.RxVerDrops != nil {
				s.meters.rdk.TopicPartitionRxVerDrops.Record(ctx, *pStats.RxVerDrops, partitionAttributes)
			}
			if pStats.MsgsInflight != nil {
				s.meters.rdk.TopicPartitionMsgsInflight.Record(ctx, *pStats.MsgsInflight, partitionAttributes)
			}
		}
	}

	s.meters.rdk.InternalQueueLen.Record(ctx, int64(rdkLen), topLevelAttributes)
	s.meters.rdk.AsyncResultQueueLen.Record(ctx, int64(asyncLen), topLevelAttributes)
}
