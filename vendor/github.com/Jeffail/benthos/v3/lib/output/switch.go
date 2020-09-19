package output

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
	"golang.org/x/sync/errgroup"
)

//------------------------------------------------------------------------------

var (
	// ErrSwitchNoConditionMet is returned when a message does not match any
	// output conditions.
	ErrSwitchNoConditionMet = errors.New("no switch output conditions were met by message")
	// ErrSwitchNoCasesMatched is returned when a message does not match any
	// output cases.
	ErrSwitchNoCasesMatched = errors.New("no switch cases were matched by message")
	// ErrSwitchNoOutputs is returned when creating a Switch type with less than
	// 2 outputs.
	ErrSwitchNoOutputs = errors.New("attempting to create switch with fewer than 2 cases")
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSwitch] = TypeSpec{
		constructor: NewSwitch,
		Summary: `
The switch output type allows you to route messages to different outputs based
on their contents.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldAdvanced(
				"retry_until_success", `
If a selected output fails to send a message this field determines whether it is
reattempted indefinitely. If set to false the error is instead propagated back
to the input level.

If a message can be routed to >1 outputs it is usually best to set this to true
in order to avoid duplicate messages being routed to an output.`,
			),
			docs.FieldAdvanced(
				"strict_mode", `
This field determines whether an error should be reported if no condition is met.
If set to true, an error is propagated back to the input level. The default
behavior is false, which will drop the message.`,
			),
			docs.FieldCommon(
				"max_in_flight", `
The maximum number of parallel message batches to have in flight at any given time.`,
			),
			docs.FieldCommon(
				"cases",
				"A list of switch cases, outlining outputs that can be routed to.",
				[]interface{}{
					map[string]interface{}{
						"check": `this.urls.contains("http://benthos.dev")`,
						"output": map[string]interface{}{
							"cache": map[string]interface{}{
								"target": "foo",
								"key":    "${!json(\"id\")}",
							},
						},
						"continue": true,
					},
					map[string]interface{}{
						"output": map[string]interface{}{
							"s3": map[string]interface{}{
								"bucket": "bar",
								"path":   "${!json(\"id\")}",
							},
						},
					},
				},
			).HasType(docs.FieldArray).WithChildren(
				docs.FieldCommon(
					"check",
					"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether a message should be routed to the case output. If left empty the case always passes.",
					`this.type == "foo"`,
					`this.contents.urls.contains("https://benthos.dev/")`,
				).HasDefault(""),
				docs.FieldCommon(
					"output", "An [output](/docs/components/outputs/about/) for messages that pass the check to be routed to.",
				).HasDefault(map[string]interface{}{}),
				docs.FieldAdvanced(
					"continue",
					"Indicates whether, if this case passes for a message, the next case should also be tested.",
				).HasDefault(false),
			),
			docs.FieldDeprecated("outputs"),
		},
		Categories: []Category{
			CategoryUtility,
		},
		Examples: []docs.AnnotatedExample{
			{
				Title: "Basic Multiplexing",
				Summary: `
The most common use for a switch output is to multiplex messages across a range of output destinations. The following config checks the contents of the field ` + "`type` of messages and sends `foo` type messages to an `amqp_1` output, `bar` type messages to a `gcp_pubsub` output, and everything else to a `redis_streams` output" + `.

Outputs can have their own processors associated with them, and in this example the ` + "`redis_streams`" + ` output has a processor that enforces the presence of a type field before sending it.`,
				Config: `
output:
  switch:
    cases:
      - check: this.type == "foo"
        output:
          amqp_1:
            url: amqps://guest:guest@localhost:5672/
            target_address: queue:/the_foos

      - check: this.type == "bar"
        output:
          gcp_pubsub:
            project: dealing_with_mike
            topic: mikes_bars

      - output:
          redis_streams:
            url: tcp://localhost:6379
            stream: everything_else
          processors:
            - bloblang: |
                root = this
                root.type = this.type | "unknown"
`,
			},
			{
				Title: "Control Flow",
				Summary: `
The ` + "`continue`" + ` field allows messages that have passed a case to be tested against the next one also. This can be useful when combining non-mutually-exclusive case checks.

In the following example a message that passes both the check of the first case as well as the second will be routed to both.`,
				Config: `
output:
  switch:
    cases:
      - check: 'this.user.interests.contains("walks").catch(false)'
        output:
          amqp_1:
            url: amqps://guest:guest@localhost:5672/
            target_address: queue:/people_what_think_good
        continue: true

      - check: 'this.user.dislikes.contains("videogames").catch(false)'
        output:
          gcp_pubsub:
            project: people
            topic: that_i_dont_want_to_hang_with
`,
			},
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			m := map[string]interface{}{
				"retry_until_success": conf.Switch.RetryUntilSuccess,
				"strict_mode":         conf.Switch.StrictMode,
				"max_in_flight":       conf.Switch.MaxInFlight,
			}
			casesSlice := []interface{}{}
			for _, c := range conf.Switch.Cases {
				sanOutput, err := SanitiseConfig(c.Output)
				if err != nil {
					return nil, err
				}
				sanit := map[string]interface{}{
					"check":    c.Check,
					"output":   sanOutput,
					"continue": c.Continue,
				}
				casesSlice = append(casesSlice, sanit)
			}
			m["cases"] = casesSlice
			if len(conf.Switch.Outputs) > 0 {
				outSlice := []interface{}{}
				for _, out := range conf.Switch.Outputs {
					sanOutput, err := SanitiseConfig(out.Output)
					if err != nil {
						return nil, err
					}
					var sanCond interface{}
					if sanCond, err = condition.SanitiseConfig(out.Condition); err != nil {
						return nil, err
					}
					sanit := map[string]interface{}{
						"output":      sanOutput,
						"fallthrough": out.Fallthrough,
						"condition":   sanCond,
					}
					outSlice = append(outSlice, sanit)
				}
				m["outputs"] = outSlice
			}
			return m, nil
		},
	}
}

//------------------------------------------------------------------------------

// SwitchConfig contains configuration fields for the Switch output type.
type SwitchConfig struct {
	RetryUntilSuccess bool                 `json:"retry_until_success" yaml:"retry_until_success"`
	StrictMode        bool                 `json:"strict_mode" yaml:"strict_mode"`
	MaxInFlight       int                  `json:"max_in_flight" yaml:"max_in_flight"`
	Cases             []SwitchConfigCase   `json:"cases" yaml:"cases"`
	Outputs           []SwitchConfigOutput `json:"outputs" yaml:"outputs"`
}

// NewSwitchConfig creates a new SwitchConfig with default values.
func NewSwitchConfig() SwitchConfig {
	return SwitchConfig{
		RetryUntilSuccess: true,
		// TODO: V4 consider making this true by default.
		StrictMode:  false,
		MaxInFlight: 1,
		Cases:       []SwitchConfigCase{},
		Outputs:     []SwitchConfigOutput{},
	}
}

// SwitchConfigCase contains configuration fields per output of a switch type.
type SwitchConfigCase struct {
	Check    string `json:"check" yaml:"check"`
	Continue bool   `json:"continue" yaml:"continue"`
	Output   Config `json:"output" yaml:"output"`
}

// NewSwitchConfigCase creates a new switch output config with default values.
func NewSwitchConfigCase() SwitchConfigCase {
	return SwitchConfigCase{
		Check:    "",
		Continue: false,
		Output:   NewConfig(),
	}
}

//------------------------------------------------------------------------------

// Switch is a broker that implements types.Consumer and broadcasts each message
// out to an array of outputs.
type Switch struct {
	logger log.Modular
	stats  metrics.Type

	maxInFlight  int
	transactions <-chan types.Transaction

	retryUntilSuccess bool
	strictMode        bool
	outputTsChans     []chan types.Transaction
	outputs           []types.Output
	checks            []*mapping.Executor
	conditions        []types.Condition
	continues         []bool
	fallthroughs      []bool

	ctx        context.Context
	close      func()
	closedChan chan struct{}
}

// NewSwitch creates a new Switch type by providing outputs. Messages will be
// sent to a subset of outputs according to condition and fallthrough settings.
func NewSwitch(
	conf Config,
	mgr types.Manager,
	logger log.Modular,
	stats metrics.Type,
) (Type, error) {
	ctx, done := context.WithCancel(context.Background())
	o := &Switch{
		stats:             stats,
		logger:            logger,
		maxInFlight:       conf.Switch.MaxInFlight,
		transactions:      nil,
		retryUntilSuccess: conf.Switch.RetryUntilSuccess,
		strictMode:        conf.Switch.StrictMode,
		closedChan:        make(chan struct{}),
		ctx:               ctx,
		close:             done,
	}

	lCases := len(conf.Switch.Cases)
	lOutputs := len(conf.Switch.Outputs)
	if lCases < 2 && lOutputs < 2 {
		return nil, ErrSwitchNoOutputs
	}
	if lCases > 0 {
		if lOutputs > 0 {
			return nil, errors.New("combining switch cases with deprecated outputs is not supported")
		}
		o.outputs = make([]types.Output, lCases)
		o.checks = make([]*mapping.Executor, lCases)
		o.continues = make([]bool, lCases)
		o.fallthroughs = make([]bool, lCases)
	} else {
		o.outputs = make([]types.Output, lOutputs)
		o.conditions = make([]types.Condition, lOutputs)
		o.fallthroughs = make([]bool, lOutputs)
	}

	var err error
	for i, oConf := range conf.Switch.Outputs {
		ns := fmt.Sprintf("switch.%v", i)
		if o.outputs[i], err = New(
			oConf.Output, mgr,
			logger.NewModule("."+ns+".output"),
			metrics.Combine(stats, metrics.Namespaced(stats, ns+".output")),
		); err != nil {
			return nil, fmt.Errorf("failed to create output '%v' type '%v': %v", i, oConf.Output.Type, err)
		}
		if o.conditions[i], err = condition.New(
			oConf.Condition, mgr,
			logger.NewModule("."+ns+".condition"),
			metrics.Namespaced(stats, ns+".condition"),
		); err != nil {
			return nil, fmt.Errorf("failed to create output '%v' condition '%v': %v", i, oConf.Condition.Type, err)
		}
		o.fallthroughs[i] = oConf.Fallthrough
	}

	for i, cConf := range conf.Switch.Cases {
		ns := fmt.Sprintf("switch.%v", i)
		if o.outputs[i], err = New(
			cConf.Output, mgr,
			logger.NewModule("."+ns+".output"),
			metrics.Combine(stats, metrics.Namespaced(stats, ns+".output")),
		); err != nil {
			return nil, fmt.Errorf("failed to create case '%v' output type '%v': %v", i, cConf.Output.Type, err)
		}
		if len(cConf.Check) > 0 {
			if o.checks[i], err = bloblang.NewMapping("", cConf.Check); err != nil {
				return nil, fmt.Errorf("failed to parse case '%v' check mapping: %v", i, err)
			}
		}
		o.continues[i] = cConf.Continue
	}

	o.outputTsChans = make([]chan types.Transaction, len(o.outputs))
	for i := range o.outputTsChans {
		o.outputTsChans[i] = make(chan types.Transaction)
		if err := o.outputs[i].Consume(o.outputTsChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

//------------------------------------------------------------------------------

// Consume assigns a new transactions channel for the broker to read.
func (o *Switch) Consume(transactions <-chan types.Transaction) error {
	if o.transactions != nil {
		return types.ErrAlreadyStarted
	}
	o.transactions = transactions

	if len(o.conditions) > 0 {
		o.logger.Warnf("Using deprecated field `outputs` which will be removed in the next major release of Benthos. For more information check out the docs at https://www.benthos.dev/docs/components/outputs/switch.")
		go o.loopDeprecated()
	} else {
		go o.loop()
	}
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (o *Switch) Connected() bool {
	for _, out := range o.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (o *Switch) loop() {
	var (
		wg         = sync.WaitGroup{}
		mMsgRcvd   = o.stats.GetCounter("switch.messages.received")
		mMsgSnt    = o.stats.GetCounter("switch.messages.sent")
		mOutputErr = o.stats.GetCounter("switch.output.error")
	)

	defer func() {
		wg.Wait()
		for i, output := range o.outputs {
			output.CloseAsync()
			close(o.outputTsChans[i])
		}
		for _, output := range o.outputs {
			if err := output.WaitForClose(time.Second); err != nil {
				for err != nil {
					err = output.WaitForClose(time.Second)
				}
			}
		}
		close(o.closedChan)
	}()

	sendLoop := func() {
		defer wg.Done()
		for {
			var ts types.Transaction
			var open bool

			select {
			case ts, open = <-o.transactions:
				if !open {
					return
				}
			case <-o.ctx.Done():
				return
			}
			mMsgRcvd.Incr(1)

			outputTargets := make([][]types.Part, len(o.checks))
			if checksErr := ts.Payload.Iter(func(i int, p types.Part) error {
				routedAtLeastOnce := false
				for j, exe := range o.checks {
					test := true
					if exe != nil {
						var err error
						if test, err = exe.QueryPart(i, ts.Payload); err != nil {
							test = false
							o.logger.Errorf("Failed to test case %v: %v\n", j, err)
						}
					}
					if test {
						routedAtLeastOnce = true
						outputTargets[j] = append(outputTargets[j], p.Copy())
						if !o.continues[j] {
							return nil
						}
					}
				}
				if !routedAtLeastOnce && o.strictMode {
					return ErrSwitchNoConditionMet
				}
				return nil
			}); checksErr != nil {
				select {
				case ts.ResponseChan <- response.NewError(checksErr):
				case <-o.ctx.Done():
					return
				}
				continue
			}

			var owg errgroup.Group
			for target, parts := range outputTargets {
				if len(parts) == 0 {
					continue
				}
				msgCopy, i := message.New(nil), target
				msgCopy.SetAll(parts)
				owg.Go(func() error {
					throt := throttle.New(throttle.OptCloseChan(o.ctx.Done()))
					resChan := make(chan types.Response)

					// Try until success or shutdown.
					for {
						select {
						case o.outputTsChans[i] <- types.NewTransaction(msgCopy, resChan):
						case <-o.ctx.Done():
							return types.ErrTypeClosed
						}
						select {
						case res := <-resChan:
							if res.Error() != nil {
								if o.retryUntilSuccess {
									o.logger.Errorf("Failed to dispatch switch message: %v\n", res.Error())
									mOutputErr.Incr(1)
									if !throt.Retry() {
										return types.ErrTypeClosed
									}
								} else {
									return res.Error()
								}
							} else {
								mMsgSnt.Incr(1)
								return nil
							}
						case <-o.ctx.Done():
							return types.ErrTypeClosed
						}
					}
				})
			}

			var oResponse types.Response = response.NewAck()
			if resErr := owg.Wait(); resErr != nil {
				oResponse = response.NewError(resErr)
			}
			select {
			case ts.ResponseChan <- oResponse:
			case <-o.ctx.Done():
				return
			}
		}
	}

	// Max in flight
	for i := 0; i < o.maxInFlight; i++ {
		wg.Add(1)
		go sendLoop()
	}
}

// CloseAsync shuts down the Switch broker and stops processing requests.
func (o *Switch) CloseAsync() {
	o.close()
}

// WaitForClose blocks until the Switch broker has closed down.
func (o *Switch) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
