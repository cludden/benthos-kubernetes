package reader

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

//------------------------------------------------------------------------------

// MQTTConfig contains configuration fields for the MQTT input type.
type MQTTConfig struct {
	URLs                   []string `json:"urls" yaml:"urls"`
	QoS                    uint8    `json:"qos" yaml:"qos"`
	Topics                 []string `json:"topics" yaml:"topics"`
	ClientID               string   `json:"client_id" yaml:"client_id"`
	CleanSession           bool     `json:"clean_session" yaml:"clean_session"`
	User                   string   `json:"user" yaml:"user"`
	Password               string   `json:"password" yaml:"password"`
	StaleConnectionTimeout string   `json:"stale_connection_timeout" yaml:"stale_connection_timeout"`
}

// NewMQTTConfig creates a new MQTTConfig with default values.
func NewMQTTConfig() MQTTConfig {
	return MQTTConfig{
		URLs:                   []string{"tcp://localhost:1883"},
		QoS:                    1,
		Topics:                 []string{"benthos_topic"},
		ClientID:               "benthos_input",
		CleanSession:           true,
		User:                   "",
		Password:               "",
		StaleConnectionTimeout: "",
	}
}

//------------------------------------------------------------------------------

// MQTT is an input type that reads MQTT Pub/Sub messages.
type MQTT struct {
	client  mqtt.Client
	msgChan chan mqtt.Message
	cMut    sync.Mutex

	staleConnectionTimeout time.Duration

	conf MQTTConfig

	interruptChan chan struct{}

	urls []string

	stats metrics.Type
	log   log.Modular
}

// NewMQTT creates a new MQTT input type.
func NewMQTT(
	conf MQTTConfig, log log.Modular, stats metrics.Type,
) (*MQTT, error) {
	m := &MQTT{
		conf:          conf,
		interruptChan: make(chan struct{}),
		stats:         stats,
		log:           log,
	}

	if len(conf.StaleConnectionTimeout) > 0 {
		var err error
		if m.staleConnectionTimeout, err = time.ParseDuration(conf.StaleConnectionTimeout); err != nil {
			return nil, fmt.Errorf("unable to parse stale connection timeout duration string: %w", err)
		}
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

// Connect establishes a connection to an MQTT server.
func (m *MQTT) Connect() error {
	return m.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a connection to an MQTT server.
func (m *MQTT) ConnectWithContext(ctx context.Context) error {
	m.cMut.Lock()
	defer m.cMut.Unlock()

	if m.client != nil {
		return nil
	}

	var msgMut sync.Mutex
	msgChan := make(chan mqtt.Message)

	closeMsgChan := func() bool {
		msgMut.Lock()
		chanOpen := msgChan != nil
		if chanOpen {
			close(msgChan)
			msgChan = nil
		}
		msgMut.Unlock()
		return chanOpen
	}

	conf := mqtt.NewClientOptions().
		SetAutoReconnect(false).
		SetClientID(m.conf.ClientID).
		SetCleanSession(m.conf.CleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			client.Disconnect(0)
			closeMsgChan()
			m.log.Errorf("Connection lost due to: %v\n", reason)
		}).
		SetOnConnectHandler(func(c mqtt.Client) {
			for _, topic := range m.conf.Topics {
				tok := c.Subscribe(topic, byte(m.conf.QoS), func(c mqtt.Client, msg mqtt.Message) {
					msgMut.Lock()
					if msgChan != nil {
						select {
						case msgChan <- msg:
						case <-m.interruptChan:
						}
					}
					msgMut.Unlock()
				})
				tok.Wait()
				if err := tok.Error(); err != nil {
					m.log.Errorf("Failed to subscribe to topic '%v': %v\n", topic, err)
					m.log.Errorln("Shutting connection down.")
					closeMsgChan()
				}
			}
		})

	if m.conf.User != "" {
		conf.SetUsername(m.conf.User)
	}

	if m.conf.Password != "" {
		conf.SetPassword(m.conf.Password)
	}

	for _, u := range m.urls {
		conf = conf.AddBroker(u)
	}

	client := mqtt.NewClient(conf)

	tok := client.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		return err
	}

	m.log.Infof("Receiving MQTT messages from topics: %v\n", m.conf.Topics)

	if m.staleConnectionTimeout == 0 {
		go func() {
			for {
				select {
				case <-time.After(time.Second):
					if !client.IsConnected() {
						if closeMsgChan() {
							m.log.Errorln("Connection lost for unknown reasons.")
						}
						return
					}
				case <-m.interruptChan:
					return
				}
			}
		}()
	}

	m.client = client
	m.msgChan = msgChan
	return nil
}

// ReadWithContext attempts to read a new message from an MQTT broker.
func (m *MQTT) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	m.cMut.Lock()
	msgChan := m.msgChan
	m.cMut.Unlock()

	if msgChan == nil {
		return nil, nil, types.ErrNotConnected
	}

	var staleTimer *time.Timer
	var staleChan <-chan time.Time
	if m.staleConnectionTimeout > 0 {
		staleTimer = time.NewTimer(m.staleConnectionTimeout)
		staleChan = staleTimer.C
		defer staleTimer.Stop()
	}

	select {
	case <-staleChan:
		m.log.Errorln("Stale connection timeout triggered, re-establishing connection to broker.")
		m.cMut.Lock()
		m.client.Disconnect(0)
		m.msgChan = nil
		m.client = nil
		m.cMut.Unlock()
		return nil, nil, types.ErrNotConnected
	case msg, open := <-msgChan:
		if !open {
			m.cMut.Lock()
			m.msgChan = nil
			m.client = nil
			m.cMut.Unlock()
			return nil, nil, types.ErrNotConnected
		}

		message := message.New([][]byte{[]byte(msg.Payload())})

		meta := message.Get(0).Metadata()
		meta.Set("mqtt_duplicate", strconv.FormatBool(bool(msg.Duplicate())))
		meta.Set("mqtt_qos", strconv.Itoa(int(msg.Qos())))
		meta.Set("mqtt_retained", strconv.FormatBool(bool(msg.Retained())))
		meta.Set("mqtt_topic", string(msg.Topic()))
		meta.Set("mqtt_message_id", strconv.Itoa(int(msg.MessageID())))

		return message, func(ctx context.Context, res types.Response) error {
			if res.Error() == nil {
				msg.Ack()
			}
			return nil
		}, nil
	case <-ctx.Done():
	case <-m.interruptChan:
		return nil, nil, types.ErrTypeClosed
	}
	return nil, nil, types.ErrTimeout
}

// Read attempts to read a new message from an MQTT broker.
func (m *MQTT) Read() (types.Message, error) {
	msg, _, err := m.ReadWithContext(context.Background())
	return msg, err
}

// Acknowledge instructs whether messages have been successfully propagated.
func (m *MQTT) Acknowledge(err error) error {
	return nil
}

// CloseAsync shuts down the MQTT input and stops processing requests.
func (m *MQTT) CloseAsync() {
	m.cMut.Lock()
	if m.client != nil {
		m.client.Disconnect(0)
		m.client = nil
		close(m.interruptChan)
	}
	m.cMut.Unlock()
}

// WaitForClose blocks until the MQTT input has closed down.
func (m *MQTT) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
