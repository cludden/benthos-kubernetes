package input

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/google/go-cmp/cmp"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeReadUntil] = TypeSpec{
		constructor: NewReadUntil,
		Summary: `
Reads messages from a child input until a consumed message passes a [Bloblang query](/docs/guides/bloblang/about/), at which point the input closes.`,
		Description: `
Messages are read continuously while the query check returns false, when the query returns true the message that triggered the check is sent out and the input is closed. Use this to define inputs where the stream should end once a certain message appears.

Sometimes inputs close themselves. For example, when the ` + "`file`" + ` input type reaches the end of a file it will shut down. By default this type will also shut down. If you wish for the input type to be restarted every time it shuts down until the query check is met then set ` + "`restart_input` to `true`." + `

### Metadata

A metadata key ` + "`benthos_read_until` containing the value `final`" + ` is added to the first part of the message that triggers the input to stop.`,
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Consume N Messages",
				Summary: "A common reason to use this input is to consume only N messages from an input and then stop. This can easily be done with the [`count` function](/docs/guides/bloblang/functions/#count):",
				Config: `
# Only read 100 messages, and then exit.
input:
  read_until:
    check: count("messages") >= 100
    input:
      kafka_balanced:
        addresses: [ TODO ]
        topics: [ foo, bar ]
        consumer_group: foogroup
`,
			},
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var inputSanit interface{} = struct{}{}
			if conf.ReadUntil.Input != nil {
				var err error
				if inputSanit, err = SanitiseConfig(*conf.ReadUntil.Input); err != nil {
					return nil, err
				}
			}
			iSanit := map[string]interface{}{
				"input":         inputSanit,
				"check":         conf.ReadUntil.Check,
				"restart_input": conf.ReadUntil.Restart,
			}
			if !isDefaultCond(conf.ReadUntil.Condition) {
				condSanit, err := condition.SanitiseConfig(conf.ReadUntil.Condition)
				if err != nil {
					return nil, err
				}
				iSanit["condition"] = condSanit
			}
			return iSanit, nil
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("input", "The child input to consume from."),
			docs.FieldCommon(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether the input should now be closed.",
				`this.type == "foo"`,
				`count("messages") >= 100`,
			).HasDefault(""),
			docs.FieldDeprecated("condition"),
			docs.FieldCommon("restart_input", "Whether the input should be reopened if it closes itself before the condition has resolved to true."),
		},
		Categories: []Category{
			CategoryUtility,
		},
	}
}

func isDefaultCond(cond condition.Config) bool {
	return cmp.Equal(cond, condition.NewConfig())
}

//------------------------------------------------------------------------------

// ReadUntilConfig contains configuration values for the ReadUntil input type.
type ReadUntilConfig struct {
	Input     *Config          `json:"input" yaml:"input"`
	Restart   bool             `json:"restart_input" yaml:"restart_input"`
	Condition condition.Config `json:"condition" yaml:"condition"`
	Check     string           `json:"check" yaml:"check"`
}

// NewReadUntilConfig creates a new ReadUntilConfig with default values.
func NewReadUntilConfig() ReadUntilConfig {
	return ReadUntilConfig{
		Input:     nil,
		Restart:   false,
		Condition: condition.NewConfig(),
		Check:     "",
	}
}

//------------------------------------------------------------------------------

type dummyReadUntilConfig struct {
	Input     interface{}      `json:"input" yaml:"input"`
	Restart   bool             `json:"restart_input" yaml:"restart_input"`
	Condition condition.Config `json:"condition" yaml:"condition"`
	Check     string           `json:"check" yaml:"check"`
}

// MarshalJSON prints an empty object instead of nil.
func (r ReadUntilConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyReadUntilConfig{
		Input:     r.Input,
		Restart:   r.Restart,
		Condition: r.Condition,
		Check:     r.Check,
	}
	if r.Input == nil {
		dummy.Input = struct{}{}
	}
	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (r ReadUntilConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyReadUntilConfig{
		Input:     r.Input,
		Restart:   r.Restart,
		Condition: r.Condition,
		Check:     r.Check,
	}
	if r.Input == nil {
		dummy.Input = struct{}{}
	}
	return dummy, nil
}

//------------------------------------------------------------------------------

// ReadUntil is an input type that continuously reads another input type until a
// condition returns true on a message consumed.
type ReadUntil struct {
	running int32
	conf    ReadUntilConfig

	wrapped Type
	cond    condition.Type
	check   *mapping.Executor

	wrapperMgr   types.Manager
	wrapperLog   log.Modular
	wrapperStats metrics.Type

	stats metrics.Type
	log   log.Modular

	transactions chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewReadUntil creates a new ReadUntil input type.
func NewReadUntil(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if conf.ReadUntil.Input == nil {
		return nil, errors.New("cannot create read_until input without a child")
	}

	wrapped, err := New(
		*conf.ReadUntil.Input, mgr, log, stats,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create input '%v': %v", conf.ReadUntil.Input.Type, err)
	}

	var cond condition.Type
	if !isDefaultCond(conf.ReadUntil.Condition) {
		if cond, err = condition.New(
			conf.ReadUntil.Condition, mgr,
			log.NewModule(".read_until.condition"),
			metrics.Namespaced(stats, "read_until.condition"),
		); err != nil {
			return nil, fmt.Errorf("failed to create condition '%v': %v", conf.ReadUntil.Condition.Type, err)
		}
	}

	var check *mapping.Executor
	if len(conf.ReadUntil.Check) > 0 {
		if check, err = bloblang.NewMapping("", conf.ReadUntil.Check); err != nil {
			return nil, fmt.Errorf("failed to parse check query: %w", err)
		}
	}

	if cond == nil && check == nil {
		return nil, errors.New("a check query is required")
	}

	if cond != nil && check != nil {
		return nil, errors.New("cannot specify both a condition and a check query")
	}

	rdr := &ReadUntil{
		running: 1,
		conf:    conf.ReadUntil,

		wrapperLog:   log,
		wrapperStats: stats,
		wrapperMgr:   mgr,

		log:          log.NewModule(".read_until"),
		stats:        metrics.Namespaced(stats, "read_until"),
		wrapped:      wrapped,
		cond:         cond,
		check:        check,
		transactions: make(chan types.Transaction),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	go rdr.loop()
	return rdr, nil
}

//------------------------------------------------------------------------------

func (r *ReadUntil) loop() {
	var (
		mRunning         = r.stats.GetGauge("running")
		mRestartErr      = r.stats.GetCounter("restart.error")
		mRestartSucc     = r.stats.GetCounter("restart.success")
		mInputClosed     = r.stats.GetCounter("input.closed")
		mCount           = r.stats.GetCounter("count")
		mPropagated      = r.stats.GetCounter("propagated")
		mFinalPropagated = r.stats.GetCounter("final.propagated")
		mFinalResSent    = r.stats.GetCounter("final.response.sent")
		mFinalResSucc    = r.stats.GetCounter("final.response.success")
		mFinalResErr     = r.stats.GetCounter("final.response.error")
	)

	defer func() {
		if r.wrapped != nil {
			r.wrapped.CloseAsync()
			err := r.wrapped.WaitForClose(time.Second)
			for ; err != nil; err = r.wrapped.WaitForClose(time.Second) {
			}
		}
		mRunning.Decr(1)

		close(r.transactions)
		close(r.closedChan)
	}()
	mRunning.Incr(1)

	var open bool

runLoop:
	for atomic.LoadInt32(&r.running) == 1 {
		if r.wrapped == nil {
			if r.conf.Restart {
				var err error
				if r.wrapped, err = New(
					*r.conf.Input, r.wrapperMgr, r.wrapperLog, r.wrapperStats,
				); err != nil {
					mRestartErr.Incr(1)
					r.log.Errorf("Failed to create input '%v': %v\n", r.conf.Input.Type, err)
					return
				}
				mRestartSucc.Incr(1)
			} else {
				return
			}
		}

		var tran types.Transaction
		select {
		case tran, open = <-r.wrapped.TransactionChan():
			if !open {
				mInputClosed.Incr(1)
				r.wrapped = nil
				continue runLoop
			}
		case <-r.closeChan:
			return
		}
		mCount.Incr(1)

		var check bool
		if r.cond != nil {
			check = r.cond.Check(tran.Payload)
		} else {
			var err error
			if check, err = r.check.QueryPart(0, tran.Payload); err != nil {
				check = false
				r.log.Errorf("Failed to execute check query: %v\n", err)
			}
		}
		if !check {
			select {
			case r.transactions <- tran:
				mPropagated.Incr(1)
			case <-r.closeChan:
				return
			}
			continue
		}

		tran.Payload.Get(0).Metadata().Set("benthos_read_until", "final")

		// If this transaction succeeds we shut down.
		tmpRes := make(chan types.Response)
		select {
		case r.transactions <- types.NewTransaction(tran.Payload, tmpRes):
			mFinalPropagated.Incr(1)
		case <-r.closeChan:
			return
		}

		var res types.Response
		select {
		case res, open = <-tmpRes:
			if !open {
				return
			}
			streamEnds := res.Error() == nil
			select {
			case tran.ResponseChan <- res:
				mFinalResSent.Incr(1)
			case <-r.closeChan:
				return
			}
			if streamEnds {
				mFinalResSucc.Incr(1)
				return
			}
			mFinalResErr.Incr(1)
		case <-r.closeChan:
			return
		}
	}
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (r *ReadUntil) TransactionChan() <-chan types.Transaction {
	return r.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (r *ReadUntil) Connected() bool {
	return r.wrapped.Connected()
}

// CloseAsync shuts down the ReadUntil input and stops processing requests.
func (r *ReadUntil) CloseAsync() {
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		close(r.closeChan)
	}
}

// WaitForClose blocks until the ReadUntil input has closed down.
func (r *ReadUntil) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
