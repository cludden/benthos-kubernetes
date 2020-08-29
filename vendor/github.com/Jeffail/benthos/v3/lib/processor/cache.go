package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCache] = TypeSpec{
		constructor: NewCache,
		Summary: `
Performs operations against a [cache resource](/docs/components/caches/about)
for each message, allowing you to store or retrieve data within message payloads.`,
		Description: `
This processor will interpolate functions within the ` + "`key` and `value`" + `
fields individually for each message. This allows you to specify dynamic keys
and values based on the contents of the message payloads and metadata. You can
find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).

## Operators

### ` + "`set`" + `

Set a key in the cache to a value. If the key already exists the contents are
overridden.

### ` + "`add`" + `

Set a key in the cache to a value. If the key already exists the action fails
with a 'key already exists' error, which can be detected with
[processor error handling](/docs/configuration/error_handling).

### ` + "`get`" + `

Retrieve the contents of a cached key and replace the original message payload
with the result. If the key does not exist the action fails with an error, which
can be detected with [processor error handling](/docs/configuration/error_handling).

### ` + "`delete`" + `

Delete a key and its contents from the cache.  If the key does not exist the
action is a no-op and will not fail with an error.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("resource", "The [`cache` resource](/docs/components/caches/about) to target with this processor."),
			docs.FieldDeprecated("cache"),
			docs.FieldCommon("operator", "The [operation](#operators) to perform with the cache.").HasOptions("set", "add", "get", "delete"),
			docs.FieldCommon("key", "A key to use with the cache.").SupportsInterpolation(false),
			docs.FieldCommon("value", "A value to use with the cache (when applicable).").SupportsInterpolation(false),
			partsFieldSpec,
		},
		Footnotes: `
## Examples

The ` + "`cache`" + ` processor can be used in combination with other processors
in order to solve a variety of data stream problems.

### Deduplication

Deduplication can be done using the add operator with a key extracted from the
message payload, since it fails when a key already exists we can remove the
duplicates using a
[` + "`bloblang` processor" + `](/docs/components/processors/bloblang):

` + "``` yaml" + `
- cache:
    resource: TODO
    operator: add
    key: '${! json("message.id") }'
    value: "storeme"
- bloblang: root = if errored() { deleted() }
` + "```" + `

### Hydration

It's possible to enrich payloads with content previously stored in a cache by
using the [` + "`process_map`" + `](/docs/components/processors/process_map) processor:

` + "``` yaml" + `
- process_map:
    processors:
    - cache:
        resource: TODO
        operator: get
        key: '${! json("message.document_id") }'
    postmap:
      message.document: .
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// CacheConfig contains configuration fields for the Cache processor.
type CacheConfig struct {
	Cache    string `json:"cache" yaml:"cache"`
	Resource string `json:"resource" yaml:"resource"`
	Parts    []int  `json:"parts" yaml:"parts"`
	Operator string `json:"operator" yaml:"operator"`
	Key      string `json:"key" yaml:"key"`
	Value    string `json:"value" yaml:"value"`
}

// NewCacheConfig returns a CacheConfig with default values.
func NewCacheConfig() CacheConfig {
	return CacheConfig{
		Cache:    "",
		Resource: "",
		Parts:    []int{},
		Operator: "set",
		Key:      "",
		Value:    "",
	}
}

//------------------------------------------------------------------------------

// Cache is a processor that stores or retrieves data from a cache for each
// message of a batch via an interpolated key.
type Cache struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	parts []int

	key   field.Expression
	value field.Expression

	cache    types.Cache
	operator cacheOperator

	mCount            metrics.StatCounter
	mErr              metrics.StatCounter
	mKeyAlreadyExists metrics.StatCounter
	mSent             metrics.StatCounter
	mBatchSent        metrics.StatCounter
}

// NewCache returns a Cache processor.
func NewCache(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var c types.Cache
	var err error
	if len(conf.Cache.Resource) > 0 {
		c, err = mgr.GetCache(conf.Cache.Resource)
	} else {
		c, err = mgr.GetCache(conf.Cache.Cache)
	}
	if err != nil {
		return nil, err
	}

	op, err := cacheOperatorFromString(conf.Cache.Operator, c)
	if err != nil {
		return nil, err
	}

	key, err := bloblang.NewField(conf.Cache.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	value, err := bloblang.NewField(conf.Cache.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}

	return &Cache{
		conf:  conf,
		log:   log,
		stats: stats,

		parts: conf.Cache.Parts,

		key:   key,
		value: value,

		cache:    c,
		operator: op,

		mCount:            stats.GetCounter("count"),
		mErr:              stats.GetCounter("error"),
		mKeyAlreadyExists: stats.GetCounter("key_already_exists"),
		mSent:             stats.GetCounter("sent"),
		mBatchSent:        stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

type cacheOperator func(key string, value []byte) ([]byte, bool, error)

func newCacheSetOperator(cache types.Cache) cacheOperator {
	return func(key string, value []byte) ([]byte, bool, error) {
		err := cache.Set(key, value)
		return nil, false, err
	}
}

func newCacheAddOperator(cache types.Cache) cacheOperator {
	return func(key string, value []byte) ([]byte, bool, error) {
		err := cache.Add(key, value)
		return nil, false, err
	}
}

func newCacheGetOperator(cache types.Cache) cacheOperator {
	return func(key string, _ []byte) ([]byte, bool, error) {
		result, err := cache.Get(key)
		return result, true, err
	}
}

func newCacheDeleteOperator(cache types.Cache) cacheOperator {
	return func(key string, _ []byte) ([]byte, bool, error) {
		err := cache.Delete(key)
		return nil, false, err
	}
}

func cacheOperatorFromString(operator string, cache types.Cache) (cacheOperator, error) {
	switch operator {
	case "set":
		return newCacheSetOperator(cache), nil
	case "add":
		return newCacheAddOperator(cache), nil
	case "get":
		return newCacheGetOperator(cache), nil
	case "delete":
		return newCacheDeleteOperator(cache), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", operator)
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Cache) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		key := c.key.String(index, newMsg)
		value := c.value.Bytes(index, newMsg)

		result, useResult, err := c.operator(key, value)
		if err != nil {
			if err != types.ErrKeyAlreadyExists {
				c.mErr.Incr(1)
				c.log.Debugf("Operator failed for key '%s': %v\n", key, err)
			} else {
				c.mKeyAlreadyExists.Incr(1)
				c.log.Debugf("Key already exists: %v\n", key)
			}
			return err
		}

		if useResult {
			part.Set(result)
		}
		return nil
	}

	IteratePartsWithSpan(TypeCache, c.parts, newMsg, proc)

	c.mBatchSent.Incr(1)
	c.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Cache) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Cache) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
