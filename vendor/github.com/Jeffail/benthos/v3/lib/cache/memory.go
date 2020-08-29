package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/OneOfOne/xxhash"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMemory] = TypeSpec{
		constructor: NewMemory,
		Summary: `
Stores key/value pairs in a map held in memory. This cache is therefore reset
every time the service restarts. Each item in the cache has a TTL set from the
moment it was last edited, after which it will be removed during the next
compaction.`,
		Description: `
A compaction only occurs during a write where the time since the last compaction
is above the compaction interval. It is therefore possible to obtain values of
keys that have expired between compactions.

The field ` + "`init_values`" + ` can be used to prepopulate the memory cache
with any number of key/value pairs which are exempt from TTLs:

` + "```yaml" + `
memory:
  ttl: 60
  init_values:
    foo: bar
` + "```" + `

These values can be overridden during execution, at which point the configured
TTL is respected as usual.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("ttl", "The TTL of each item in seconds. After this period an item will be eligible for removal during the next compaction."),
			docs.FieldCommon("compaction_interval", "The period of time to wait before each compaction, at which point expired items are removed."),
			docs.FieldAdvanced("shards", "A number of logical shards to spread keys across, increasing the shards can have a performance benefit when processing a large number of keys."),
			docs.FieldCommon(
				"init_values", "A table of key/value pairs that should be present in the cache on initialization. This can be used to create static lookup tables.",
				map[string]string{
					"Nickelback":       "1995",
					"Spice Girls":      "1994",
					"The Human League": "1977",
				},
			),
		},
	}
}

//------------------------------------------------------------------------------

// MemoryConfig contains config fields for the Memory cache type.
type MemoryConfig struct {
	TTL                int               `json:"ttl" yaml:"ttl"`
	CompactionInterval string            `json:"compaction_interval" yaml:"compaction_interval"`
	InitValues         map[string]string `json:"init_values" yaml:"init_values"`
	Shards             int               `json:"shards" yaml:"shards"`
}

// NewMemoryConfig creates a MemoryConfig populated with default values.
func NewMemoryConfig() MemoryConfig {
	return MemoryConfig{
		TTL:                300, // 5 Mins
		CompactionInterval: "60s",
		InitValues:         map[string]string{},
		Shards:             1,
	}
}

//------------------------------------------------------------------------------

type item struct {
	value []byte
	ts    time.Time
}

type shard struct {
	items map[string]item
	ttl   time.Duration

	compInterval   time.Duration
	lastCompaction time.Time

	mKeys        metrics.StatGauge
	mCompactions metrics.StatCounter

	sync.RWMutex
}

func (s *shard) compaction() {
	if s.compInterval == 0 {
		return
	}
	if time.Since(s.lastCompaction) < s.compInterval {
		return
	}
	s.mCompactions.Incr(1)
	for k, v := range s.items {
		if v.ts.IsZero() {
			continue
		}
		if time.Since(v.ts) >= s.ttl {
			delete(s.items, k)
		}
	}
	s.lastCompaction = time.Now()
}

//------------------------------------------------------------------------------

// Memory is a memory based cache implementation.
type Memory struct {
	shards []*shard
}

// NewMemory creates a new Memory cache type.
func NewMemory(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	var interval time.Duration
	if tout := conf.Memory.CompactionInterval; len(tout) > 0 {
		var err error
		if interval, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse compaction interval string: %v", err)
		}
	}

	m := &Memory{}
	if conf.Memory.Shards <= 0 {
		return nil, fmt.Errorf("expected >=1 shards, found: %v", conf.Memory.Shards)
	}
	if conf.Memory.Shards == 1 {
		m.shards = []*shard{
			{
				items: map[string]item{},
				ttl:   time.Second * time.Duration(conf.Memory.TTL),

				compInterval:   interval,
				lastCompaction: time.Now(),

				mKeys:        stats.GetGauge("keys"),
				mCompactions: stats.GetCounter("compaction"),
			},
		}
	} else {
		for i := 0; i < conf.Memory.Shards; i++ {
			m.shards = append(m.shards, &shard{
				items: map[string]item{},
				ttl:   time.Second * time.Duration(conf.Memory.TTL),

				compInterval:   interval,
				lastCompaction: time.Now(),

				mKeys:        stats.GetGauge(fmt.Sprintf("shard.%v.keys", i)),
				mCompactions: stats.GetCounter(fmt.Sprintf("shard.%v.compaction", i)),
			})
		}
	}

	for k, v := range conf.Memory.InitValues {
		m.getShard(k).items[k] = item{
			value: []byte(v),
			ts:    time.Time{},
		}
	}

	return m, nil
}

//------------------------------------------------------------------------------

func (m *Memory) getShard(key string) *shard {
	if len(m.shards) == 1 {
		return m.shards[0]
	}
	h := xxhash.New64()
	h.WriteString(key)
	return m.shards[h.Sum64()%uint64(len(m.shards))]
}

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist.
func (m *Memory) Get(key string) ([]byte, error) {
	shard := m.getShard(key)
	shard.RLock()
	k, exists := shard.items[key]
	shard.RUnlock()
	if !exists {
		return nil, types.ErrKeyNotFound
	}
	return k.value, nil
}

// Set attempts to set the value of a key.
func (m *Memory) Set(key string, value []byte) error {
	shard := m.getShard(key)
	shard.Lock()
	shard.compaction()
	shard.items[key] = item{value: value, ts: time.Now()}
	shard.mKeys.Set(int64(len(shard.items)))
	shard.Unlock()
	return nil
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (m *Memory) SetMulti(items map[string][]byte) error {
	for k, v := range items {
		if err := m.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists.
func (m *Memory) Add(key string, value []byte) error {
	shard := m.getShard(key)
	shard.Lock()
	if _, exists := shard.items[key]; exists {
		shard.Unlock()
		return types.ErrKeyAlreadyExists
	}
	shard.compaction()
	shard.items[key] = item{value: value, ts: time.Now()}
	shard.mKeys.Set(int64(len(shard.items)))
	shard.Unlock()
	return nil
}

// Delete attempts to remove a key.
func (m *Memory) Delete(key string) error {
	shard := m.getShard(key)
	shard.Lock()
	shard.compaction()
	delete(shard.items, key)
	shard.mKeys.Set(int64(len(shard.items)))
	shard.Unlock()
	return nil
}

// CloseAsync shuts down the cache.
func (m *Memory) CloseAsync() {
}

// WaitForClose blocks until the cache has closed down.
func (m *Memory) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
