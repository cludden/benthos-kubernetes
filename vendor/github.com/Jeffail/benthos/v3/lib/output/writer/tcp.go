package writer

import (
	"net"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// TCPConfig contains configuration fields for the TCP output type.
type TCPConfig struct {
	Address string `json:"address" yaml:"address"`
}

// NewTCPConfig creates a new TCPConfig with default values.
func NewTCPConfig() TCPConfig {
	return TCPConfig{
		Address: "localhost:4194",
	}
}

//------------------------------------------------------------------------------

// TCP is an output type that sends messages as a continuous steam of line
// delimied messages over TCP.
type TCP struct {
	connMut sync.Mutex
	conn    net.Conn

	address string

	stats metrics.Type
	log   log.Modular
}

// NewTCP creates a new TCP writer type.
func NewTCP(
	conf TCPConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*TCP, error) {
	t := TCP{
		address: conf.Address,
		stats:   stats,
		log:     log,
	}
	return &t, nil
}

//------------------------------------------------------------------------------

// Connect does nothing.
func (t *TCP) Connect() error {
	t.connMut.Lock()
	defer t.connMut.Unlock()
	if t.conn != nil {
		return nil
	}

	var err error
	if t.conn, err = net.Dial("tcp", t.address); err != nil {
		return err
	}

	t.log.Infof("Sending messages over TCP to: %s\n", t.address)
	return nil
}

// Write attempts to write a message.
func (t *TCP) Write(msg types.Message) error {
	t.connMut.Lock()
	conn := t.conn
	t.connMut.Unlock()

	if conn == nil {
		return types.ErrNotConnected
	}

	err := msg.Iter(func(i int, part types.Part) error {
		partBytes := part.Get()
		if partBytes[len(partBytes)-1] != '\n' {
			partBytes = append(partBytes[:len(partBytes):len(partBytes)], []byte("\n")...)
		}
		_, werr := conn.Write(partBytes)
		return werr
	})
	if err == nil && msg.Len() > 1 {
		_, err = conn.Write([]byte("\n"))
	}
	if err != nil {
		t.connMut.Lock()
		t.conn.Close()
		t.conn = nil
		t.connMut.Unlock()
	}
	return err
}

// CloseAsync shuts down the TCP output and stops processing messages.
func (t *TCP) CloseAsync() {
	t.connMut.Lock()
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}
	t.connMut.Unlock()
}

// WaitForClose blocks until the TCP output has closed down.
func (t *TCP) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
