package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSyncResponse] = TypeSpec{
		constructor: func(_ Config, _ types.Manager, logger log.Modular, stats metrics.Type) (Type, error) {
			return NewWriter(TypeSyncResponse, roundtrip.Writer{}, logger, stats)
		},
		Summary: `
Returns the final message payload back to the input origin of the message, where
it is dealt with according to that specific input type.`,
		Description: `
For most inputs this mechanism is ignored entirely, in which case the sync
response is dropped without penalty. It is therefore safe to use this output
even when combining input types that might not have support for sync responses.
An example of an input able to utilise this is the ` + "`http_server`" + `.

It is safe to combine this output with others using broker types. For example,
with the ` + "`http_server`" + ` input we could send the payload to a Kafka
topic and also send a modified payload back with:

` + "``` yaml" + `
input:
  http_server:
    path: /post
output:
  broker:
    pattern: fan_out
    outputs:
    - kafka:
        addresses: [ TODO:9092 ]
        topic: foo_topic
    - type: sync_response
      processors:
      - text:
          operator: to_upper
` + "```" + `

Using the above example and posting the message 'hello world' to the endpoint
` + "`/post`" + ` Benthos would send it unchanged to the topic
` + "`foo_topic`" + ` and also respond with 'HELLO WORLD'.

For more information please read [Synchronous Responses](/docs/guides/sync_responses).`,
	}
}

//------------------------------------------------------------------------------
