package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBlobStorage] = TypeSpec{
		constructor: NewAzureBlobStorage,
		Beta:        true,
		Summary: `
Sends message parts as objects to an Azure Blob Storage Account container. Each
object is uploaded with the filename specified with the ` + "`container`" + `
field.`,
		Description: `
In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("storage_account", "The storage account to upload messages to."),
			docs.FieldCommon("storage_access_key", "The storage account access key."),
			docs.FieldCommon(
				"container", "The container for uploading the messages to.",
				`messages-${!timestamp("2006")}`,
			).SupportsInterpolation(false),
			docs.FieldCommon(
				"path", "The path of each message to upload.",
				`${!count("files")}-${!timestamp_unix_nano()}.json`,
				`${!meta("kafka_key")}.json`,
				`${!json("doc.namespace")}/${!json("doc.id")}.json`,
			).SupportsInterpolation(false),
			docs.FieldAdvanced("blob_type", "Block and Append blobs are comprised of blocks, and each blob can support up to 50,000 blocks.").HasOptions(
				"BLOCK", "APPEND",
			).SupportsInterpolation(false),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("timeout", "The maximum period to wait on an upload before abandoning it and reattempting."),
		},
		Categories: []Category{
			CategoryServices,
			CategoryAzure,
		},
	}
}

//------------------------------------------------------------------------------

// NewAzureBlobStorage creates a new AzureBlobStorage output type.
func NewAzureBlobStorage(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	sthree, err := writer.NewAzureBlobStorage(conf.BlobStorage, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.BlobStorage.MaxInFlight == 1 {
		return NewWriter(
			TypeBlobStorage, sthree, log, stats,
		)
	}
	return NewAsyncWriter(
		TypeBlobStorage, conf.BlobStorage.MaxInFlight, sthree, log, stats,
	)
}

//------------------------------------------------------------------------------
