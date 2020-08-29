package writer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

//------------------------------------------------------------------------------

// MQTTConfig contains configuration fields for the MQTT output type.
type MQTTConfig struct {
	URLs        []string `json:"urls" yaml:"urls"`
	QoS         uint8    `json:"qos" yaml:"qos"`
	Topic       string   `json:"topic" yaml:"topic"`
	ClientID    string   `json:"client_id" yaml:"client_id"`
	User        string   `json:"user" yaml:"user"`
	Password    string   `json:"password" yaml:"password"`
	MaxInFlight int      `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewMQTTConfig creates a new MQTTConfig with default values.
func NewMQTTConfig() MQTTConfig {
	return MQTTConfig{
		URLs:        []string{"tcp://localhost:1883"},
		QoS:         1,
		Topic:       "benthos_topic",
		ClientID:    "benthos_output",
		User:        "",
		Password:    "",
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// MQTT is an output type that serves MQTT messages.
type MQTT struct {
	log   log.Modular
	stats metrics.Type

	urls  []string
	conf  MQTTConfig
	topic field.Expression

	client  mqtt.Client
	connMut sync.RWMutex
}

// NewMQTT creates a new MQTT output type.
func NewMQTT(
	conf MQTTConfig,
	log log.Modular,
	stats metrics.Type,
) (*MQTT, error) {
	m := &MQTT{
		log:   log,
		stats: stats,
		conf:  conf,
	}

	var err error
	if m.topic, err = bloblang.NewField(conf.Topic); err != nil {
		return nil, fmt.Errorf("failed to parse topic expression: %v", err)
	}

	for _, u := range conf.URLs {
		for _, splitURL := range strings.Split(u, ",") {
			if len(splitURL) > 0 {
				m.urls = append(m.urls, splitURL)
			}
		}
	}

	return m, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an MQTT server.
func (m *MQTT) ConnectWithContext(ctx context.Context) error {
	return m.Connect()
}

// Connect establishes a connection to an MQTT server.
func (m *MQTT) Connect() error {
	m.connMut.Lock()
	defer m.connMut.Unlock()

	if m.client != nil {
		return nil
	}

	conf := mqtt.NewClientOptions().
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			client.Disconnect(0)
			m.log.Errorf("Connection lost due to: %v\n", reason)
		}).
		SetConnectTimeout(time.Second).
		SetWriteTimeout(time.Second).
		SetClientID(m.conf.ClientID)

	for _, u := range m.urls {
		conf = conf.AddBroker(u)
	}

	if m.conf.User != "" {
		conf.SetUsername(m.conf.User)
	}

	if m.conf.Password != "" {
		conf.SetPassword(m.conf.Password)
	}

	client := mqtt.NewClient(conf)

	tok := client.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		return err
	}

	m.client = client
	return nil
}

//------------------------------------------------------------------------------

// WriteWithContext attempts to write a message by pushing it to an MQTT broker.
func (m *MQTT) WriteWithContext(ctx context.Context, msg types.Message) error {
	return m.Write(msg)
}

// Write attempts to write a message by pushing it to an MQTT broker.
func (m *MQTT) Write(msg types.Message) error {
	m.connMut.RLock()
	client := m.client
	m.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		mtok := client.Publish(m.topic.String(i, msg), byte(m.conf.QoS), false, p.Get())
		mtok.Wait()
		sendErr := mtok.Error()
		if sendErr != nil && strings.Contains(sendErr.Error(), "Not Connected") {
			m.connMut.RLock()
			m.client = nil
			m.connMut.RUnlock()
			sendErr = types.ErrNotConnected
		}
		return sendErr
	})
}

// CloseAsync shuts down the MQTT output and stops processing messages.
func (m *MQTT) CloseAsync() {
	go func() {
		m.connMut.Lock()
		if m.client != nil {
			m.client.Disconnect(0)
			m.client = nil
		}
		m.connMut.Unlock()
	}()
}

// WaitForClose blocks until the MQTT output has closed down.
func (m *MQTT) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
