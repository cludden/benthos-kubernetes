package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNATS] = TypeSpec{
		constructor: NewNATS,
		Summary: `
Subscribe to a NATS subject.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nats_subject
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.", []string{"nats://127.0.0.1:4222"}),
			docs.FieldCommon("queue", "The queue to consume from."),
			docs.FieldCommon("subject", "A subject to consume from."),
			docs.FieldAdvanced("prefetch_count", "The maximum number of messages to pull at a time."),
		},
	}
}

//------------------------------------------------------------------------------

// NewNATS creates a new NATS input type.
func NewNATS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	n, err := reader.NewNATS(conf.NATS, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeNATS, true, reader.NewAsyncPreserver(n), log, stats)
}

//------------------------------------------------------------------------------
