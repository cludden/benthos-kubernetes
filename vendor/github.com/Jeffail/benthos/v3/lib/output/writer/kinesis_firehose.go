package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/cenkalti/backoff/v4"
)

//------------------------------------------------------------------------------

const (
	kinesisFirehoseMaxRecordsCount = 500
)

// KinesisFirehoseConfig contains configuration fields for the KinesisFirehose output type.
type KinesisFirehoseConfig struct {
	sessionConfig  `json:",inline" yaml:",inline"`
	Stream         string `json:"stream" yaml:"stream"`
	MaxInFlight    int    `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config `json:",inline" yaml:",inline"`
	Batching       batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewKinesisFirehoseConfig creates a new Config with default values.
func NewKinesisFirehoseConfig() KinesisFirehoseConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"
	batching := batch.NewPolicyConfig()
	batching.Count = 1
	return KinesisFirehoseConfig{
		sessionConfig: sessionConfig{
			Config: sess.NewConfig(),
		},
		Stream:      "",
		MaxInFlight: 1,
		Config:      rConf,
		Batching:    batching,
	}
}

//------------------------------------------------------------------------------

// KinesisFirehose is a benthos writer.Type implementation that writes messages
// to an Amazon Kinesis Firehose destination.
type KinesisFirehose struct {
	conf KinesisFirehoseConfig

	session  *session.Session
	firehose firehoseiface.FirehoseAPI

	backoffCtor func() backoff.BackOff
	endpoint    *string
	streamName  *string

	log   log.Modular
	stats metrics.Type

	mThrottled       metrics.StatCounter
	mThrottledF      metrics.StatCounter
	mPartsThrottled  metrics.StatCounter
	mPartsThrottledF metrics.StatCounter
}

// NewKinesisFirehose creates a new Amazon Kinesis Firehose writer.Type.
func NewKinesisFirehose(
	conf KinesisFirehoseConfig,
	log log.Modular,
	stats metrics.Type,
) (*KinesisFirehose, error) {
	k := KinesisFirehose{
		conf:            conf,
		log:             log,
		stats:           stats,
		mPartsThrottled: stats.GetCounter("parts.send.throttled"),
		mThrottled:      stats.GetCounter("send.throttled"),
		streamName:      aws.String(conf.Stream),
	}

	var err error
	if k.backoffCtor, err = conf.Config.GetCtor(); err != nil {
		return nil, err
	}
	return &k, nil
}

//------------------------------------------------------------------------------

// toRecords converts an individual benthos message into a slice of Kinesis Firehose
// batch put entries by promoting each message part into a single part message
// and passing each new message through the partition and hash key interpolation
// process, allowing the user to define the partition and hash key per message
// part.
func (a *KinesisFirehose) toRecords(msg types.Message) ([]*firehose.Record, error) {
	entries := make([]*firehose.Record, msg.Len())

	err := msg.Iter(func(i int, p types.Part) error {
		entry := firehose.Record{
			Data: p.Get(),
		}

		if len(entry.Data) > mebibyte {
			a.log.Errorf("part %d exceeds the maximum Kinesis Firehose payload limit of 1 MiB\n", i)
			return types.ErrMessageTooLarge
		}

		entries[i] = &entry
		return nil
	})

	return entries, err
}

//------------------------------------------------------------------------------

// ConnectWithContext creates a new Kinesis Firehose client and ensures that the
// target Kinesis Firehose delivery stream.
func (a *KinesisFirehose) ConnectWithContext(ctx context.Context) error {
	return a.Connect()
}

// Connect creates a new Kinesis Firehose client and ensures that the target
// Kinesis Firehose delivery stream.
func (a *KinesisFirehose) Connect() error {
	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession()
	if err != nil {
		return err
	}

	a.session = sess
	a.firehose = firehose.New(sess)

	if _, err := a.firehose.DescribeDeliveryStream(&firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: a.streamName,
	}); err != nil {
		return err
	}

	a.log.Infof("Sending messages to Kinesis Firehose delivery stream: %v\n", a.conf.Stream)
	return nil
}

// Write attempts to write message contents to a target Kinesis Firehose delivery
// stream in batches of 500. If throttling is detected, failed messages are retried
// according to the configurable backoff settings.
func (a *KinesisFirehose) Write(msg types.Message) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target Kinesis
// Firehose delivery stream in batches of 500. If throttling is detected, failed
// messages are retried according to the configurable backoff settings.
func (a *KinesisFirehose) WriteWithContext(ctx context.Context, msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	backOff := a.backoffCtor()

	records, err := a.toRecords(msg)
	if err != nil {
		return err
	}

	input := &firehose.PutRecordBatchInput{
		Records:            records,
		DeliveryStreamName: a.streamName,
	}

	// trim input record length to max kinesis firehose batch size
	if len(records) > kinesisMaxRecordsCount {
		input.Records, records = records[:kinesisMaxRecordsCount], records[kinesisMaxRecordsCount:]
	} else {
		records = nil
	}

	var failed []*firehose.Record
	for len(input.Records) > 0 {
		wait := backOff.NextBackOff()

		// batch write to kinesis firehose
		output, err := a.firehose.PutRecordBatch(input)
		if err != nil {
			a.log.Warnf("kinesis firehose error: %v\n", err)
			// bail if a message is too large or all retry attempts expired
			if wait == backoff.Stop {
				return err
			}
			continue
		}

		// requeue any individual records that failed due to throttling
		failed = nil
		if output.FailedPutCount != nil {
			for i, entry := range output.RequestResponses {
				if entry.ErrorCode != nil {
					failed = append(failed, input.Records[i])
					if *entry.ErrorCode != firehose.ErrCodeServiceUnavailableException {
						err = fmt.Errorf("record failed with code [%s] %s: %+v", *entry.ErrorCode, *entry.ErrorMessage, input.Records[i])
						a.log.Errorf("kinesis firehose record error: %v\n", err)
						return err
					}
				}
			}
		}
		input.Records = failed

		// if throttling errors detected, pause briefly
		l := len(failed)
		if l > 0 {
			a.mThrottled.Incr(1)
			a.mPartsThrottled.Incr(int64(l))
			a.log.Warnf("scheduling retry of throttled records (%d)\n", l)
			if wait == backoff.Stop {
				return types.ErrTimeout
			}
			time.Sleep(wait)
		}

		// add remaining records to batch
		if n := len(records); n > 0 && l < kinesisMaxRecordsCount {
			if remaining := kinesisMaxRecordsCount - l; remaining < n {
				input.Records, records = append(input.Records, records[:remaining]...), records[remaining:]
			} else {
				input.Records, records = append(input.Records, records...), nil
			}
		}
	}
	return err
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *KinesisFirehose) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *KinesisFirehose) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
