package output

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeTableStorage] = TypeSpec{
		constructor: NewAzureTableStorage,
		Beta:        true,
		Summary: `
Stores message parts in an Azure Table Storage table.`,
		Description: `
In order to set the ` + "`table_name`" + `,  ` + "`partition_key`" + ` and ` + "`row_key`" + ` 
you can use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

If the ` + "`properties`" + ` are not set in the config, all the ` + "`json`" + ` fields
are marshaled and stored in the table, which will be created if it does not exist.
The ` + "`object`" + ` and ` + "`array`" + ` fields are marshaled as strings. e.g.:

The json message:
` + "``` yaml" + `
{
  "foo": 55,
  "bar": {
    "baz": "a",
    "bez": "b"
  },
  "diz": ["a", "b"]
}
` + "```" + `

will store in the table the following properties:
` + "``` yaml" + `
foo: '55'
bar: '{ "baz": "a", "bez": "b" }'
diz: '["a", "b"]'
` + "```" + `

It's also possible to use function interpolations to get or transform the properties values, e.g.:

` + "``` yaml" + `
properties:
	device: '${! json("device") }'
	timestamp: '${! json("timestamp") }'
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.TableStorage, conf.TableStorage.Batching)
		},
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("storage_account", "The storage account to upload messages to."),
			docs.FieldCommon("storage_access_key", "The storage account access key."),
			docs.FieldCommon("table_name", "The table to store messages into.",
				`${!meta("kafka_topic")}`,
			).SupportsInterpolation(false),
			docs.FieldCommon("partition_key", "The partition key.",
				`${!json("date")}`,
			).SupportsInterpolation(false),
			docs.FieldCommon("row_key", "The row key.",
				`${!json("device")}-${!uuid_v4()}`,
			).SupportsInterpolation(false),
			docs.FieldCommon("properties", "A map of properties to store into the table.").SupportsInterpolation(true),
			docs.FieldAdvanced("insert_type", "Type of insert operation").HasOptions(
				"INSERT", "INSERT_MERGE", "INSERT_REPLACE",
			).SupportsInterpolation(false),
			docs.FieldCommon("max_in_flight",
				"The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("timeout", "The maximum period to wait on an upload before abandoning it and reattempting."),
			batch.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
			CategoryAzure,
		},
	}
}

//------------------------------------------------------------------------------

// NewAzureTableStorage creates a new NewAzureTableStorage output type.
func NewAzureTableStorage(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	sthree, err := writer.NewAzureTableStorage(conf.TableStorage, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.TableStorage.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeTableStorage, sthree, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeTableStorage, conf.TableStorage.MaxInFlight, sthree, log, stats,
		)
	}

	if bconf := conf.TableStorage.Batching; err == nil && !bconf.IsNoop() {
		policy, err := batch.NewPolicy(bconf, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		w = NewBatcher(policy, w, log, stats)
	}
	return w, err
}

//------------------------------------------------------------------------------
