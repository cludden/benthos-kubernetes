package writer

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

// RedisPubSubConfig contains configuration fields for the RedisPubSub output
// type.
type RedisPubSubConfig struct {
	URL         string `json:"url" yaml:"url"`
	Channel     string `json:"channel" yaml:"channel"`
	MaxInFlight int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewRedisPubSubConfig creates a new RedisPubSubConfig with default values.
func NewRedisPubSubConfig() RedisPubSubConfig {
	return RedisPubSubConfig{
		URL:         "tcp://localhost:6379",
		Channel:     "benthos_chan",
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// RedisPubSub is an output type that serves RedisPubSub messages.
type RedisPubSub struct {
	log   log.Modular
	stats metrics.Type

	url        *url.URL
	conf       RedisPubSubConfig
	channelStr field.Expression

	client  *redis.Client
	connMut sync.RWMutex
}

// NewRedisPubSub creates a new RedisPubSub output type.
func NewRedisPubSub(
	conf RedisPubSubConfig,
	log log.Modular,
	stats metrics.Type,
) (*RedisPubSub, error) {
	r := &RedisPubSub{
		log:   log,
		stats: stats,
		conf:  conf,
	}
	var err error
	if r.channelStr, err = bloblang.NewField(conf.Channel); err != nil {
		return nil, fmt.Errorf("failed to parse channel expression: %v", err)
	}
	r.url, err = url.Parse(conf.URL)
	if err != nil {
		return nil, err
	}
	return r, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an RedisPubSub server.
func (r *RedisPubSub) ConnectWithContext(ctx context.Context) error {
	return r.Connect()
}

// Connect establishes a connection to an RedisPubSub server.
func (r *RedisPubSub) Connect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	var pass string
	if r.url.User != nil {
		pass, _ = r.url.User.Password()
	}
	client := redis.NewClient(&redis.Options{
		Addr:     r.url.Host,
		Network:  r.url.Scheme,
		Password: pass,
	})

	if _, err := client.Ping().Result(); err != nil {
		return err
	}

	r.log.Infof("Pushing messages to Redis channel: %v\n", r.conf.Channel)

	r.client = client
	return nil
}

//------------------------------------------------------------------------------

// WriteWithContext attempts to write a message by pushing it to a Redis pub/sub
// topic.
func (r *RedisPubSub) WriteWithContext(ctx context.Context, msg types.Message) error {
	return r.Write(msg)
}

// Write attempts to write a message by pushing it to a Redis pub/sub topic.
func (r *RedisPubSub) Write(msg types.Message) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		channel := r.channelStr.String(i, msg)
		if err := client.Publish(channel, p.Get()).Err(); err != nil {
			r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return types.ErrNotConnected
		}
		return nil
	})
}

// disconnect safely closes a connection to an RedisPubSub server.
func (r *RedisPubSub) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

// CloseAsync shuts down the RedisPubSub output and stops processing messages.
func (r *RedisPubSub) CloseAsync() {
	r.disconnect()
}

// WaitForClose blocks until the RedisPubSub output has closed down.
func (r *RedisPubSub) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
