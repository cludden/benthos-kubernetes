package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeTry] = TypeSpec{
		constructor: NewTry,
		Summary: `
Behaves similarly to the ` + "[`for_each`](/docs/components/processors/for_each)" + ` processor, where a
list of child processors are applied to individual messages of a batch. However,
if a processor fails for a message then that message will skip all following
processors.`,
		Description: `
For example, with the following config:

` + "``` yaml" + `
- try:
  - type: foo
  - type: bar
  - type: baz
` + "```" + `

If the processor ` + "`foo`" + ` fails for a particular message, that message
will skip the processors ` + "`bar` and `baz`" + `.

This processor is useful for when child processors depend on the successful
output of previous processors. This processor can be followed with a
` + "[catch](/docs/components/processors/catch)" + ` processor for defining child processors to be applied
only to failed messages.

More information about error handing can be found [here](/docs/configuration/error_handling).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			procConfs := make([]interface{}, len(conf.Try))
			for i, pConf := range conf.Try {
				if procConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			return procConfs, nil
		},
	}
}

//------------------------------------------------------------------------------

// TryConfig is a config struct containing fields for the Try processor.
type TryConfig []Config

// NewTryConfig returns a default TryConfig.
func NewTryConfig() TryConfig {
	return []Config{}
}

//------------------------------------------------------------------------------

// Try is a processor that applies a list of child processors to each message of
// a batch individually, where processors are skipped for messages that failed a
// previous processor step.
type Try struct {
	children []types.Processor

	log log.Modular

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewTry returns a Try processor.
func NewTry(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var children []types.Processor
	for i, pconf := range conf.Try {
		prefix := fmt.Sprintf("%v", i)
		proc, err := New(pconf, mgr, log.NewModule("."+prefix), metrics.Namespaced(stats, prefix))
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &Try{
		children: children,
		log:      log,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Try) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	resultMsgs := make([]types.Message, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		tmpMsg := message.New(nil)
		tmpMsg.SetAll([]types.Part{p})
		resultMsgs[i] = tmpMsg
		return nil
	})

	var res types.Response
	if resultMsgs, res = ExecuteTryAll(p.children, resultMsgs...); res != nil {
		return nil, res
	}

	resMsg := message.New(nil)
	for _, m := range resultMsgs {
		m.Iter(func(i int, p types.Part) error {
			resMsg.Append(p)
			return nil
		})
	}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(resMsg.Len()))

	resMsgs := [1]types.Message{resMsg}
	return resMsgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *Try) CloseAsync() {
	for _, c := range p.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (p *Try) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
