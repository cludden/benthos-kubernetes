package reader

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/kafka/sasl"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/Shopify/sarama"
)

//------------------------------------------------------------------------------

// KafkaBalancedGroupConfig contains config fields for Kafka consumer groups.
type KafkaBalancedGroupConfig struct {
	SessionTimeout    string `json:"session_timeout" yaml:"session_timeout"`
	HeartbeatInterval string `json:"heartbeat_interval" yaml:"heartbeat_interval"`
	RebalanceTimeout  string `json:"rebalance_timeout" yaml:"rebalance_timeout"`
}

// NewKafkaBalancedGroupConfig returns a KafkaBalancedGroupConfig with default
// values.
func NewKafkaBalancedGroupConfig() KafkaBalancedGroupConfig {
	return KafkaBalancedGroupConfig{
		SessionTimeout:    "10s",
		HeartbeatInterval: "3s",
		RebalanceTimeout:  "60s",
	}
}

// KafkaBalancedConfig contains configuration for the KafkaBalanced input type.
type KafkaBalancedConfig struct {
	Addresses           []string                 `json:"addresses" yaml:"addresses"`
	ClientID            string                   `json:"client_id" yaml:"client_id"`
	ConsumerGroup       string                   `json:"consumer_group" yaml:"consumer_group"`
	Group               KafkaBalancedGroupConfig `json:"group" yaml:"group"`
	CommitPeriod        string                   `json:"commit_period" yaml:"commit_period"`
	MaxProcessingPeriod string                   `json:"max_processing_period" yaml:"max_processing_period"`
	FetchBufferCap      int                      `json:"fetch_buffer_cap" yaml:"fetch_buffer_cap"`
	Topics              []string                 `json:"topics" yaml:"topics"`
	Batching            batch.PolicyConfig       `json:"batching" yaml:"batching"`
	StartFromOldest     bool                     `json:"start_from_oldest" yaml:"start_from_oldest"`
	TargetVersion       string                   `json:"target_version" yaml:"target_version"`
	// TODO: V4 Remove this.
	MaxBatchCount int         `json:"max_batch_count" yaml:"max_batch_count"`
	TLS           btls.Config `json:"tls" yaml:"tls"`
	SASL          sasl.Config `json:"sasl" yaml:"sasl"`
}

// NewKafkaBalancedConfig creates a new KafkaBalancedConfig with default values.
// TODO: V4 Remove this unused implementation.
func NewKafkaBalancedConfig() KafkaBalancedConfig {
	return KafkaBalancedConfig{
		Addresses:           []string{"localhost:9092"},
		ClientID:            "benthos_kafka_input",
		ConsumerGroup:       "benthos_consumer_group",
		Group:               NewKafkaBalancedGroupConfig(),
		CommitPeriod:        "1s",
		MaxProcessingPeriod: "100ms",
		FetchBufferCap:      256,
		Topics:              []string{"benthos_stream"},
		StartFromOldest:     true,
		TargetVersion:       sarama.V1_0_0_0.String(),
		Batching:            batch.NewPolicyConfig(),
		MaxBatchCount:       1,
		TLS:                 btls.NewConfig(),
		SASL:                sasl.NewConfig(),
	}
}

//------------------------------------------------------------------------------

type consumerMessage struct {
	*sarama.ConsumerMessage
	highWaterMark int64
}

// KafkaBalanced is an input type that reads from a Kafka cluster by balancing
// partitions across other consumers of the same consumer group.
type KafkaBalanced struct {
	version   sarama.KafkaVersion
	tlsConf   *tls.Config
	addresses []string
	topics    []string

	commitPeriod      time.Duration
	sessionTimeout    time.Duration
	heartbeatInterval time.Duration
	rebalanceTimeout  time.Duration
	maxProcPeriod     time.Duration

	cMut          sync.Mutex
	groupCancelFn context.CancelFunc
	session       sarama.ConsumerGroupSession
	msgChan       chan consumerMessage

	offsets map[string]map[int32]int64

	mRebalanced metrics.StatCounter

	conf  KafkaBalancedConfig
	stats metrics.Type
	log   log.Modular
	mgr   types.Manager
}

// NewKafkaBalanced creates a new KafkaBalanced input type.
func NewKafkaBalanced(
	conf KafkaBalancedConfig, mgr types.Manager, log log.Modular, stats metrics.Type,
) (*KafkaBalanced, error) {
	if conf.Batching.IsNoop() {
		conf.Batching.Count = 1
	}
	k := KafkaBalanced{
		conf:          conf,
		stats:         stats,
		groupCancelFn: func() {},
		log:           log,
		mgr:           mgr,
		offsets:       map[string]map[int32]int64{},
		mRebalanced:   stats.GetCounter("rebalanced"),
	}
	if conf.MaxBatchCount < 1 {
		return nil, errors.New("max_batch_count must be greater than or equal to 1")
	}
	if conf.TLS.Enabled {
		var err error
		if k.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	for _, addr := range conf.Addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if trimmed := strings.TrimSpace(splitAddr); len(trimmed) > 0 {
				k.addresses = append(k.addresses, trimmed)
			}
		}
	}
	for _, t := range conf.Topics {
		for _, splitTopics := range strings.Split(t, ",") {
			if trimmed := strings.TrimSpace(splitTopics); len(trimmed) > 0 {
				k.topics = append(k.topics, trimmed)
			}
		}
	}
	if tout := conf.CommitPeriod; len(tout) > 0 {
		var err error
		if k.commitPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse commit period string: %v", err)
		}
	}
	if tout := conf.Group.SessionTimeout; len(tout) > 0 {
		var err error
		if k.sessionTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse session timeout string: %v", err)
		}
	}
	if tout := conf.Group.HeartbeatInterval; len(tout) > 0 {
		var err error
		if k.heartbeatInterval, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse heartbeat interval string: %v", err)
		}
	}
	if tout := conf.Group.RebalanceTimeout; len(tout) > 0 {
		var err error
		if k.rebalanceTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse rebalance timeout string: %v", err)
		}
	}
	if tout := conf.MaxProcessingPeriod; len(tout) > 0 {
		var err error
		if k.maxProcPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse max processing period string: %v", err)
		}
	}

	var err error
	if k.version, err = sarama.ParseKafkaVersion(conf.TargetVersion); err != nil {
		return nil, err
	}
	return &k, nil
}

//------------------------------------------------------------------------------

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (k *KafkaBalanced) Setup(sesh sarama.ConsumerGroupSession) error {
	k.cMut.Lock()
	k.session = sesh
	k.cMut.Unlock()
	k.mRebalanced.Incr(1)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have
// exited but before the offsets are committed for the very last time.
func (k *KafkaBalanced) Cleanup(sesh sarama.ConsumerGroupSession) error {
	k.cMut.Lock()
	k.session = nil
	k.cMut.Unlock()
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (k *KafkaBalanced) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	k.log.Debugf("Consuming messages from topic '%v' partition '%v'\n", claim.Topic(), claim.Partition())
	for {
		select {
		case msg, open := <-claim.Messages():
			if !open {
				return nil
			}
			select {
			case k.msgChan <- consumerMessage{
				ConsumerMessage: msg,
				highWaterMark:   claim.HighWaterMarkOffset(),
			}:
			case <-sess.Context().Done():
				k.log.Debugf("Stopped consuming messages from topic '%v' partition '%v'\n", claim.Topic(), claim.Partition())
				return nil
			}
		case <-sess.Context().Done():
			k.log.Debugf("Stopped consuming messages from topic '%v' partition '%v'\n", claim.Topic(), claim.Partition())
			return nil
		}
	}
}

//------------------------------------------------------------------------------

func (k *KafkaBalanced) setOffset(topic string, partition int32, offset int64) {
	var topicMap map[int32]int64
	var exists bool
	if topicMap, exists = k.offsets[topic]; !exists {
		topicMap = map[int32]int64{}
		k.offsets[topic] = topicMap
	}
	topicMap[partition] = offset
}

func (k *KafkaBalanced) closeGroup() {
	k.cMut.Lock()
	cancelFn := k.groupCancelFn
	k.cMut.Unlock()

	if cancelFn != nil {
		cancelFn()
	}
}

//------------------------------------------------------------------------------

// Connect establishes a KafkaBalanced connection.
func (k *KafkaBalanced) Connect() error {
	k.cMut.Lock()
	defer k.cMut.Unlock()
	if k.msgChan != nil {
		return nil
	}

	config := sarama.NewConfig()
	config.ClientID = k.conf.ClientID
	config.Net.DialTimeout = time.Second
	config.Version = k.version
	config.Consumer.Return.Errors = true
	config.Consumer.MaxProcessingTime = k.maxProcPeriod
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = k.commitPeriod
	config.Consumer.Group.Session.Timeout = k.sessionTimeout
	config.Consumer.Group.Heartbeat.Interval = k.heartbeatInterval
	config.Consumer.Group.Rebalance.Timeout = k.rebalanceTimeout
	config.ChannelBufferSize = k.conf.FetchBufferCap

	if config.Net.ReadTimeout <= k.sessionTimeout {
		config.Net.ReadTimeout = k.sessionTimeout * 2
	}
	if config.Net.ReadTimeout <= k.rebalanceTimeout {
		config.Net.ReadTimeout = k.rebalanceTimeout * 2
	}

	config.Net.TLS.Enable = k.conf.TLS.Enabled
	if k.conf.TLS.Enabled {
		config.Net.TLS.Config = k.tlsConf
	}
	if k.conf.StartFromOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if err := k.conf.SASL.Apply(k.mgr, config); err != nil {
		return err
	}

	// Start a new consumer group
	group, err := sarama.NewConsumerGroup(k.addresses, k.conf.ConsumerGroup, config)
	if err != nil {
		return err
	}

	// Handle errors
	go func() {
		for {
			gerr, open := <-group.Errors()
			if !open {
				return
			}
			if gerr != nil {
				k.log.Errorf("KafkaBalanced message recv error: %v\n", gerr)
				if cerr, ok := gerr.(*sarama.ConsumerError); ok {
					if cerr.Err == sarama.ErrUnknownMemberId {
						// Sarama doesn't seem to recover from this error.
						go k.closeGroup()
					}
				}
			}
		}
	}()

	// Handle session
	go func() {
	groupLoop:
		for {
			ctx, doneFn := context.WithCancel(context.Background())

			k.cMut.Lock()
			k.groupCancelFn = doneFn
			k.cMut.Unlock()

			k.log.Debugln("Starting consumer group")
			gerr := group.Consume(ctx, k.topics, k)
			select {
			case <-ctx.Done():
				break groupLoop
			default:
			}
			doneFn()
			if gerr != nil {
				if gerr != io.EOF {
					k.log.Errorf("KafkaBalanced group session error: %v\n", gerr)
				}
				break groupLoop
			}
		}
		k.log.Debugln("Closing consumer group")

		group.Close()

		k.cMut.Lock()
		if k.msgChan != nil {
			close(k.msgChan)
			k.msgChan = nil
		}
		k.cMut.Unlock()
	}()

	k.msgChan = make(chan consumerMessage, k.conf.MaxBatchCount)
	k.offsets = map[string]map[int32]int64{}

	k.log.Infof("Receiving KafkaBalanced messages from addresses: %s\n", k.addresses)
	return nil
}

// Read attempts to read a message from a KafkaBalanced topic.
func (k *KafkaBalanced) Read() (types.Message, error) {
	k.cMut.Lock()
	msgChan := k.msgChan
	k.cMut.Unlock()

	if msgChan == nil {
		return nil, types.ErrNotConnected
	}

	msg := message.New(nil)
	addPart := func(data consumerMessage) {
		part := message.NewPart(data.Value)

		meta := part.Metadata()
		for _, hdr := range data.Headers {
			meta.Set(string(hdr.Key), string(hdr.Value))
		}

		lag := data.highWaterMark - data.Offset - 1
		if lag < 0 {
			lag = 0
		}

		meta.Set("kafka_key", string(data.Key))
		meta.Set("kafka_partition", strconv.Itoa(int(data.Partition)))
		meta.Set("kafka_topic", data.Topic)
		meta.Set("kafka_offset", strconv.Itoa(int(data.Offset)))
		meta.Set("kafka_lag", strconv.FormatInt(lag, 10))
		meta.Set("kafka_timestamp_unix", strconv.FormatInt(data.Timestamp.Unix(), 10))

		msg.Append(part)

		k.setOffset(data.Topic, data.Partition, data.Offset)
	}

	data, open := <-msgChan
	if !open {
		return nil, types.ErrNotConnected
	}
	addPart(data)

batchLoop:
	for i := 1; i < k.conf.MaxBatchCount; i++ {
		select {
		case data, open = <-msgChan:
			if !open {
				return nil, types.ErrNotConnected
			}
			addPart(data)
		default:
			// Drained the buffer
			break batchLoop
		}
	}

	if msg.Len() == 0 {
		return nil, types.ErrTimeout
	}
	return msg, nil
}

// Acknowledge instructs whether the current offset should be committed.
func (k *KafkaBalanced) Acknowledge(err error) error {
	if err == nil {
		k.cMut.Lock()
		if k.session != nil {
			for topic, v := range k.offsets {
				for part, offset := range v {
					k.session.MarkOffset(topic, part, offset+1, "")
				}
			}
		}
		k.cMut.Unlock()
	}
	return nil
}

// CloseAsync shuts down the KafkaBalanced input and stops processing requests.
func (k *KafkaBalanced) CloseAsync() {
	go k.closeGroup()
}

// WaitForClose blocks until the KafkaBalanced input has closed down.
func (k *KafkaBalanced) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
