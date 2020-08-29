package processor

import (
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeResource] = TypeSpec{
		constructor: NewResource,
		Summary: `
Resource is a processor type that runs a processor resource by its name. This
processor allows you to run the same configured processor resource in multiple
places.`,
		Description: `
Resource processors also have the advantage of name based metrics and logging.
For example, the config:

` + "``` yaml" + `
pipeline:
  processors:
    - jmespath:
        query: foo
` + "```" + `

Is equivalent to:

` + "``` yaml" + `
pipeline:
  processors:
    - resource: foo_proc

resources:
  processors:
    foo_proc:
      jmespath:
        query: foo
` + "```" + `

But now the metrics path of the JMESPath processor will be
` + "`resources.processors.foo_proc`" + `, this way of flattening observability
labels becomes more useful as configs get larger and more nested.`,
	}
}

//------------------------------------------------------------------------------

type procProvider interface {
	GetProcessor(name string) (types.Processor, error)
}

//------------------------------------------------------------------------------

// Resource is a processor that returns the result of a processor resource.
type Resource struct {
	mgr  procProvider
	name string
	log  log.Modular

	mCount       metrics.StatCounter
	mErr         metrics.StatCounter
	mErrNotFound metrics.StatCounter
}

// NewResource returns a resource processor.
func NewResource(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	// TODO: V4 Remove this
	procProvider, ok := mgr.(procProvider)
	if !ok {
		return nil, errors.New("manager does not support processor resources")
	}

	if _, err := procProvider.GetProcessor(conf.Resource); err != nil {
		return nil, fmt.Errorf("failed to obtain processor resource '%v': %v", conf.Resource, err)
	}
	return &Resource{
		mgr:  procProvider,
		name: conf.Resource,
		log:  log,

		mCount:       stats.GetCounter("count"),
		mErrNotFound: stats.GetCounter("error_not_found"),
		mErr:         stats.GetCounter("error"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (r *Resource) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	proc, err := r.mgr.GetProcessor(r.name)
	r.mCount.Incr(1)
	if err != nil {
		r.log.Debugf("Failed to obtain processor resource '%v': %v", r.name, err)
		r.mErrNotFound.Incr(1)
		r.mErr.Incr(1)
		return nil, response.NewError(err)
	}
	return proc.ProcessMessage(msg)
}

// CloseAsync shuts down the processor and stops processing requests.
func (r *Resource) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (r *Resource) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
