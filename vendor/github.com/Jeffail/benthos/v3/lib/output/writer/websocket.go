package writer

import (
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/auth"
	"github.com/gorilla/websocket"
)

//------------------------------------------------------------------------------

// WebsocketConfig contains configuration fields for the Websocket output type.
type WebsocketConfig struct {
	URL         string `json:"url" yaml:"url"`
	auth.Config `json:",inline" yaml:",inline"`
}

// NewWebsocketConfig creates a new WebsocketConfig with default values.
func NewWebsocketConfig() WebsocketConfig {
	return WebsocketConfig{
		URL:    "ws://localhost:4195/post/ws",
		Config: auth.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// Websocket is an output type that serves Websocket messages.
type Websocket struct {
	log   log.Modular
	stats metrics.Type

	lock *sync.Mutex

	conf   WebsocketConfig
	client *websocket.Conn
}

// NewWebsocket creates a new Websocket output type.
func NewWebsocket(
	conf WebsocketConfig,
	log log.Modular,
	stats metrics.Type,
) (*Websocket, error) {
	ws := &Websocket{
		log:   log,
		stats: stats,
		lock:  &sync.Mutex{},
		conf:  conf,
	}
	return ws, nil
}

//------------------------------------------------------------------------------

func (w *Websocket) getWS() *websocket.Conn {
	w.lock.Lock()
	ws := w.client
	w.lock.Unlock()
	return ws
}

//------------------------------------------------------------------------------

// Connect establishes a connection to an Websocket server.
func (w *Websocket) Connect() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.client != nil {
		return nil
	}

	headers := http.Header{}

	purl, err := url.Parse(w.conf.URL)
	if err != nil {
		return err
	}

	if err = w.conf.Sign(&http.Request{
		URL:    purl,
		Header: headers,
	}); err != nil {
		return err
	}

	var client *websocket.Conn
	if client, _, err = websocket.DefaultDialer.Dial(w.conf.URL, headers); err != nil {
		return err
	}

	go func(c *websocket.Conn) {
		for {
			if _, _, cerr := c.NextReader(); cerr != nil {
				c.Close()
				break
			}
		}
	}(client)

	w.client = client
	return nil
}

//------------------------------------------------------------------------------

// Write attempts to write a message by pushing it to an Websocket broker.
func (w *Websocket) Write(msg types.Message) error {
	client := w.getWS()
	if client == nil {
		return types.ErrNotConnected
	}

	err := msg.Iter(func(i int, p types.Part) error {
		return client.WriteMessage(websocket.BinaryMessage, p.Get())
	})
	if err != nil {
		w.lock.Lock()
		w.client = nil
		w.lock.Unlock()
		if err == websocket.ErrCloseSent {
			return types.ErrNotConnected
		}
		return err
	}
	return nil
}

// CloseAsync shuts down the Websocket output and stops processing messages.
func (w *Websocket) CloseAsync() {
	go func() {
		w.lock.Lock()
		if w.client != nil {
			w.client.Close()
			w.client = nil
		}
		w.lock.Unlock()
	}()
}

// WaitForClose blocks until the Websocket output has closed down.
func (w *Websocket) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
