package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/auth"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWebsocket] = TypeSpec{
		constructor: NewWebsocket,
		Summary: `
Connects to a websocket server and continuously receives messages.`,
		Description: `
It is possible to configure an ` + "`open_message`" + `, which when set to a
non-empty string will be sent to the websocket server each time a connection is
first established.`,
		FieldSpecs: append(docs.FieldSpecs{
			docs.FieldCommon("url", "The URL to connect to.", "ws://localhost:4195/get/ws").HasType("string"),
			docs.FieldAdvanced("open_message", "An optional message to send to the server upon connection."),
		}, auth.FieldSpecs()...),
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// NewWebsocket creates a new Websocket input type.
func NewWebsocket(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	ws, err := reader.NewWebsocket(conf.Websocket, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader("websocket", true, reader.NewAsyncPreserver(ws), log, stats)
}

//------------------------------------------------------------------------------
