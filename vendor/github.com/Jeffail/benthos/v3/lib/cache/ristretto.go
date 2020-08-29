package cache

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/cenkalti/backoff/v4"
	"github.com/dgraph-io/ristretto"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRistretto] = TypeSpec{
		constructor: NewRistretto,
		Beta:        true,
		Summary: `
Stores key/value pairs in a map held in the memory-bound
[Ristretto cache](https://github.com/dgraph-io/ristretto). This cache is more
efficient and appropriate for high-volume use cases than the standard memory
cache.

The add command is non-atomic, and therefore this cache is not suitable for
deduplication.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"ttl",
				"The TTL of each item as a duration string. After this period an item will be eligible for removal during the next compaction.",
				"60s", "5m", "36h",
			),
		}.Merge(retries.FieldSpecs()),
	}
}

//------------------------------------------------------------------------------

// RistrettoConfig contains config fields for the Ristretto cache type.
type RistrettoConfig struct {
	TTL            string `json:"ttl" yaml:"ttl"`
	retries.Config `json:",inline" yaml:",inline"`
}

// NewRistrettoConfig creates a RistrettoConfig populated with default values.
func NewRistrettoConfig() RistrettoConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"
	return RistrettoConfig{
		TTL:    "",
		Config: rConf,
	}
}

//------------------------------------------------------------------------------

// Ristretto is a memory based cache implementation.
type Ristretto struct {
	ttl   time.Duration
	cache *ristretto.Cache

	backoffCtor func() backoff.BackOff
	boffPool    sync.Pool
}

// NewRistretto creates a new Ristretto cache type.
func NewRistretto(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	var ttl time.Duration
	var err error

	if len(conf.Ristretto.TTL) > 0 {
		if ttl, err = time.ParseDuration(conf.Ristretto.TTL); err != nil {
			return nil, fmt.Errorf("failed to parse ttl duration: %w", err)
		}
	}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		return nil, err
	}
	r := &Ristretto{
		ttl:   ttl,
		cache: cache,
	}

	if r.backoffCtor, err = conf.Ristretto.Config.GetCtor(); err != nil {
		return nil, err
	}
	r.boffPool = sync.Pool{
		New: func() interface{} {
			return r.backoffCtor()
		},
	}

	return r, nil
}

//------------------------------------------------------------------------------

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist.
func (r *Ristretto) Get(key string) ([]byte, error) {
	boff := r.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		r.boffPool.Put(boff)
	}()

	var res interface{}
	var ok bool
	for !ok {
		if res, ok = r.cache.Get(key); !ok {
			wait := boff.NextBackOff()
			if wait == backoff.Stop {
				break
			}
			time.Sleep(wait)
		}
	}

	if !ok {
		return nil, types.ErrKeyNotFound
	}
	return res.([]byte), nil
}

// Set attempts to set the value of a key.
func (r *Ristretto) Set(key string, value []byte) error {
	if !r.cache.SetWithTTL(key, value, 1, r.ttl) {
		return errors.New("set operation was dropped")
	}
	return nil
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (r *Ristretto) SetMulti(items map[string][]byte) error {
	for key, value := range items {
		if !r.cache.SetWithTTL(key, value, 1, r.ttl) {
			return errors.New("set operation was dropped")
		}
	}
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists.
func (r *Ristretto) Add(key string, value []byte) error {
	return r.Add(key, value)
}

// Delete attempts to remove a key.
func (r *Ristretto) Delete(key string) error {
	r.cache.Del(key)
	return nil
}

// CloseAsync shuts down the cache.
func (r *Ristretto) CloseAsync() {
	r.cache.Close()
}

// WaitForClose blocks until the cache has closed down.
func (r *Ristretto) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
