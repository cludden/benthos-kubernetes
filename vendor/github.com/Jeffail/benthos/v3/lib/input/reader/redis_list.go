package reader

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

// RedisListConfig contains configuration fields for the RedisList input type.
type RedisListConfig struct {
	URL     string `json:"url" yaml:"url"`
	Key     string `json:"key" yaml:"key"`
	Timeout string `json:"timeout" yaml:"timeout"`
}

// NewRedisListConfig creates a new RedisListConfig with default values.
func NewRedisListConfig() RedisListConfig {
	return RedisListConfig{
		URL:     "tcp://localhost:6379",
		Key:     "benthos_list",
		Timeout: "5s",
	}
}

//------------------------------------------------------------------------------

// RedisList is an input type that reads Redis List messages.
type RedisList struct {
	client *redis.Client
	cMut   sync.Mutex

	url     *url.URL
	conf    RedisListConfig
	timeout time.Duration

	stats metrics.Type
	log   log.Modular
}

// NewRedisList creates a new RedisList input type.
func NewRedisList(
	conf RedisListConfig, log log.Modular, stats metrics.Type,
) (*RedisList, error) {
	r := &RedisList{
		conf:  conf,
		stats: stats,
		log:   log,
	}

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if r.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	var err error
	r.url, err = url.Parse(r.conf.URL)
	if err != nil {
		return nil, err
	}

	return r, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to a Redis server.
func (r *RedisList) Connect() error {
	return r.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a connection to a Redis server.
func (r *RedisList) ConnectWithContext(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	if r.client != nil {
		return nil
	}

	var pass string
	if r.url.User != nil {
		pass, _ = r.url.User.Password()
	}

	// We default to Redis DB 0 for backward compatibilitiy, but if it's
	// specified in the URL, we'll use the specified one instead.
	var redisDB int
	if len(r.url.Path) > 1 {
		var err error
		// We'll strip the leading '/'
		redisDB, err = strconv.Atoi(r.url.Path[1:])
		if err != nil {
			return fmt.Errorf("invalid Redis DB, can't parse '%s'", r.url.Path)
		}
	}

	client := redis.NewClient(&redis.Options{
		Addr:     r.url.Host,
		Network:  r.url.Scheme,
		DB:       redisDB,
		Password: pass,
	})

	if _, err := client.Ping().Result(); err != nil {
		return err
	}

	r.log.Infof("Receiving messages from Redis list: %v\n", r.conf.Key)

	r.client = client
	return nil
}

// Read attempts to pop a message from a Redis list.
func (r *RedisList) Read() (types.Message, error) {
	msg, _, err := r.ReadWithContext(context.Background())
	return msg, err
}

// ReadWithContext attempts to pop a message from a Redis list.
func (r *RedisList) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	var client *redis.Client

	r.cMut.Lock()
	client = r.client
	r.cMut.Unlock()

	if client == nil {
		return nil, nil, types.ErrNotConnected
	}

	res, err := client.BLPop(r.timeout, r.conf.Key).Result()

	if err != nil && err != redis.Nil {
		r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return nil, nil, types.ErrNotConnected
	}

	if len(res) < 2 {
		return nil, nil, types.ErrTimeout
	}

	return message.New([][]byte{[]byte(res[1])}), noopAsyncAckFn, nil
}

// Acknowledge is a noop since Redis Lists do not support acknowledgements.
func (r *RedisList) Acknowledge(err error) error {
	return nil
}

// disconnect safely closes a connection to an RedisList server.
func (r *RedisList) disconnect() error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	var err error
	if r.client != nil {
		err = r.client.Close()
		r.client = nil
	}
	return err
}

// CloseAsync shuts down the RedisList input and stops processing requests.
func (r *RedisList) CloseAsync() {
	r.disconnect()
}

// WaitForClose blocks until the RedisList input has closed down.
func (r *RedisList) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
