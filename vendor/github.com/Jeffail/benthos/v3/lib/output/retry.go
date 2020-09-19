package output

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/cenkalti/backoff/v4"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRetry] = TypeSpec{
		constructor: NewRetry,
		Summary: `
Attempts to write messages to a child output and if the write fails for any
reason the message is retried either until success or, if the retries or max
elapsed time fields are non-zero, either is reached.`,
		Description: `
All messages in Benthos are always retried on an output error, but this would
usually involve propagating the error back to the source of the message, whereby
it would be reprocessed before reaching the output layer once again.

This output type is useful whenever we wish to avoid reprocessing a message on
the event of a failed send. We might, for example, have a dedupe processor that
we want to avoid reapplying to the same message more than once in the pipeline.

Rather than retrying the same output you may wish to retry the send using a
different output target (a dead letter queue). In which case you should instead
use the ` + "[`try`](/docs/components/outputs/try)" + ` output type.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			confBytes, err := json.Marshal(conf.Retry)
			if err != nil {
				return nil, err
			}

			confMap := map[string]interface{}{}
			if err = json.Unmarshal(confBytes, &confMap); err != nil {
				return nil, err
			}

			var outputSanit interface{} = struct{}{}
			if conf.Retry.Output != nil {
				if outputSanit, err = SanitiseConfig(*conf.Retry.Output); err != nil {
					return nil, err
				}
			}
			confMap["output"] = outputSanit
			return confMap, nil
		},
		FieldSpecs: retries.FieldSpecs().Add(
			docs.FieldCommon("output", "A child output."),
		),
		Categories: []Category{
			CategoryUtility,
		},
	}
}

//------------------------------------------------------------------------------

// RetryConfig contains configuration values for the Retry output type.
type RetryConfig struct {
	Output         *Config `json:"output" yaml:"output"`
	retries.Config `json:",inline" yaml:",inline"`
}

// NewRetryConfig creates a new RetryConfig with default values.
func NewRetryConfig() RetryConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 0
	rConf.Backoff.InitialInterval = "100ms"
	rConf.Backoff.MaxInterval = "1s"
	rConf.Backoff.MaxElapsedTime = "0s"
	return RetryConfig{
		Output: nil,
		Config: retries.NewConfig(),
	}
}

//------------------------------------------------------------------------------

type dummyRetryConfig struct {
	Output         interface{} `json:"output" yaml:"output"`
	retries.Config `json:",inline" yaml:",inline"`
}

// MarshalJSON prints an empty object instead of nil.
func (r RetryConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyRetryConfig{
		Output: r.Output,
		Config: r.Config,
	}
	if r.Output == nil {
		dummy.Output = struct{}{}
	}
	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (r RetryConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyRetryConfig{
		Output: r.Output,
		Config: r.Config,
	}
	if r.Output == nil {
		dummy.Output = struct{}{}
	}
	return dummy, nil
}

//------------------------------------------------------------------------------

// Retry is an output type that continuously writes a message to a child output
// until the send is successful.
type Retry struct {
	running int32
	conf    RetryConfig

	wrapped     Type
	backoffCtor func() backoff.BackOff

	stats metrics.Type
	log   log.Modular

	transactionsIn  <-chan types.Transaction
	transactionsOut chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewRetry creates a new Retry input type.
func NewRetry(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if conf.Retry.Output == nil {
		return nil, errors.New("cannot create retry output without a child")
	}

	wrapped, err := New(*conf.Retry.Output, mgr, log, stats)
	if err != nil {
		return nil, fmt.Errorf("failed to create output '%v': %v", conf.Retry.Output.Type, err)
	}

	var boffCtor func() backoff.BackOff
	if boffCtor, err = conf.Retry.GetCtor(); err != nil {
		return nil, err
	}

	return &Retry{
		running: 1,
		conf:    conf.Retry,

		log:             log,
		stats:           stats,
		wrapped:         wrapped,
		backoffCtor:     boffCtor,
		transactionsOut: make(chan types.Transaction),

		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}, nil
}

//------------------------------------------------------------------------------

func (r *Retry) loop() {
	// Metrics paths
	var (
		mRunning      = r.stats.GetGauge("retry.running")
		mCount        = r.stats.GetCounter("retry.count")
		mSuccess      = r.stats.GetCounter("retry.send.success")
		mPartsSuccess = r.stats.GetCounter("retry.parts.send.success")
		mError        = r.stats.GetCounter("retry.send.error")
		mEndOfRetries = r.stats.GetCounter("retry.end_of_retries")
	)

	wg := sync.WaitGroup{}

	defer func() {
		wg.Wait()
		close(r.transactionsOut)
		r.wrapped.CloseAsync()
		err := r.wrapped.WaitForClose(time.Second)
		for ; err != nil; err = r.wrapped.WaitForClose(time.Second) {
		}
		mRunning.Decr(1)
		close(r.closedChan)
	}()
	mRunning.Incr(1)

	errInterruptChan := make(chan struct{})
	var errLooped int64

	for atomic.LoadInt32(&r.running) == 1 {
		// Do not consume another message while pending messages are being
		// reattempted.
		for atomic.LoadInt64(&errLooped) > 0 {
			select {
			case <-errInterruptChan:
			case <-time.After(time.Millisecond * 100):
				// Just incase an interrupt doesn't arrive.
			case <-r.closeChan:
				return
			}
		}

		var tran types.Transaction
		var open bool
		select {
		case tran, open = <-r.transactionsIn:
			if !open {
				return
			}
			mCount.Incr(1)
		case <-r.closeChan:
			return
		}

		rChan := make(chan types.Response)
		select {
		case r.transactionsOut <- types.NewTransaction(tran.Payload, rChan):
		case <-r.closeChan:
			return
		}

		wg.Add(1)
		go func(ts types.Transaction, resChan chan types.Response) {
			var backOff backoff.BackOff
			var resOut types.Response
			var inErrLoop bool

			defer func() {
				wg.Done()
				if inErrLoop {
					atomic.AddInt64(&errLooped, -1)

					// We're exiting our error loop, so (attempt to) interrupt the
					// consumer.
					select {
					case errInterruptChan <- struct{}{}:
					default:
					}
				}
			}()

		retryLoop:
			for atomic.LoadInt32(&r.running) == 1 {
				var res types.Response
				select {
				case res = <-resChan:
				case <-r.closeChan:
					return
				}

				if res.Error() != nil {
					if !inErrLoop {
						inErrLoop = true
						atomic.AddInt64(&errLooped, 1)
					}

					mError.Incr(1)
					r.log.Errorf("Failed to send message: %v\n", res.Error())
					if backOff == nil {
						backOff = r.backoffCtor()
					}

					nextBackoff := backOff.NextBackOff()
					if nextBackoff == backoff.Stop {
						mEndOfRetries.Incr(1)
						resOut = response.NewNoack()
						break retryLoop
					}
					select {
					case <-time.After(nextBackoff):
					case <-r.closeChan:
						return
					}

					select {
					case r.transactionsOut <- types.NewTransaction(ts.Payload, resChan):
					case <-r.closeChan:
						return
					}
				} else {
					mSuccess.Incr(1)
					mPartsSuccess.Incr(int64(ts.Payload.Len()))
					resOut = response.NewAck()
					break retryLoop
				}
			}

			select {
			case ts.ResponseChan <- resOut:
			case <-r.closeChan:
				return
			}
		}(tran, rChan)
	}
}

// Consume assigns a messages channel for the output to read.
func (r *Retry) Consume(ts <-chan types.Transaction) error {
	if r.transactionsIn != nil {
		return types.ErrAlreadyStarted
	}
	if err := r.wrapped.Consume(r.transactionsOut); err != nil {
		return err
	}
	r.transactionsIn = ts
	go r.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (r *Retry) Connected() bool {
	return r.wrapped.Connected()
}

// CloseAsync shuts down the Retry input and stops processing requests.
func (r *Retry) CloseAsync() {
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		close(r.closeChan)
	}
}

// WaitForClose blocks until the Retry input has closed down.
func (r *Retry) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
