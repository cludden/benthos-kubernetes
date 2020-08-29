package writer

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/nats-io/stan.go"
)

//------------------------------------------------------------------------------

// NATSStreamConfig contains configuration fields for the NATSStream output
// type.
type NATSStreamConfig struct {
	URLs        []string `json:"urls" yaml:"urls"`
	ClusterID   string   `json:"cluster_id" yaml:"cluster_id"`
	ClientID    string   `json:"client_id" yaml:"client_id"`
	Subject     string   `json:"subject" yaml:"subject"`
	MaxInFlight int      `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewNATSStreamConfig creates a new NATSStreamConfig with default values.
func NewNATSStreamConfig() NATSStreamConfig {
	return NATSStreamConfig{
		URLs:        []string{stan.DefaultNatsURL},
		ClusterID:   "test-cluster",
		ClientID:    "benthos_client",
		Subject:     "benthos_messages",
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// NATSStream is an output type that serves NATS messages.
type NATSStream struct {
	log log.Modular

	natsConn stan.Conn
	connMut  sync.RWMutex

	urls string
	conf NATSStreamConfig
}

// NewNATSStream creates a new NATS Stream output type.
func NewNATSStream(conf NATSStreamConfig, log log.Modular, stats metrics.Type) (*NATSStream, error) {
	if len(conf.ClientID) == 0 {
		rgen := rand.New(rand.NewSource(time.Now().UnixNano()))

		// Generate random client id if one wasn't supplied.
		b := make([]byte, 16)
		rgen.Read(b)
		conf.ClientID = fmt.Sprintf("client-%x", b)
	}

	n := NATSStream{
		log:  log,
		conf: conf,
	}
	n.urls = strings.Join(conf.URLs, ",")

	return &n, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to NATS servers.
func (n *NATSStream) ConnectWithContext(ctx context.Context) error {
	return n.Connect()
}

// Connect attempts to establish a connection to NATS servers.
func (n *NATSStream) Connect() error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var err error
	n.natsConn, err = stan.Connect(
		n.conf.ClusterID,
		n.conf.ClientID,
		stan.NatsURL(n.urls),
	)
	if err == nil {
		n.log.Infof("Sending NATS messages to subject: %v\n", n.conf.Subject)
	}
	return err
}

// WriteWithContext attempts to write a message.
func (n *NATSStream) WriteWithContext(ctx context.Context, msg types.Message) error {
	return n.Write(msg)
}

// Write attempts to write a message.
func (n *NATSStream) Write(msg types.Message) error {
	n.connMut.RLock()
	conn := n.natsConn
	n.connMut.RUnlock()

	if conn == nil {
		return types.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		err := conn.Publish(n.conf.Subject, p.Get())
		if err == stan.ErrConnectionClosed {
			conn.Close()
			n.connMut.Lock()
			n.natsConn = nil
			n.connMut.Unlock()
			return types.ErrNotConnected
		}
		return err
	})
}

// CloseAsync shuts down the MQTT output and stops processing messages.
func (n *NATSStream) CloseAsync() {
	n.connMut.Lock()
	if n.natsConn != nil {
		n.natsConn.Close()
		n.natsConn = nil
	}
	n.connMut.Unlock()
}

// WaitForClose blocks until the NATS output has closed down.
func (n *NATSStream) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
