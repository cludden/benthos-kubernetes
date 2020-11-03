package processor

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/client"
	"github.com/google/go-cmp/cmp"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHTTP] = TypeSpec{
		constructor: NewHTTP,
		Categories: []Category{
			CategoryIntegration,
		},
		Summary: `
Performs an HTTP request using a message batch as the request body, and replaces
the original message parts with the body of the response.`,
		Description: `
If a processed message batch contains more than one message they will be sent in
a single request as a [multipart message](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html).
Alternatively, message batches can be sent in parallel by setting the field
` + "`parallel` to `true`" + `.

The ` + "`rate_limit`" + ` field can be used to specify a rate limit
[resource](/docs/components/rate_limits/about) to cap the rate of requests
across all parallel components service wide.

The URL and header values of this type can be dynamically set using function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

In order to map or encode the payload to a specific request body, and map the
response back into the original payload instead of replacing it entirely, you
can use the ` + "[`branch` processor](/docs/components/processors/branch)" + `.

## Response Codes

Benthos considers any response code between 200 and 299 inclusive to indicate a
successful response, you can add more success status codes with the field
` + "`successful_on`" + `.

When a request returns a response code within the ` + "`backoff_on`" + ` field
it will be retried after increasing intervals.

When a request returns a response code within the ` + "`drop_on`" + ` field it
will not be reattempted and is immediately considered a failed request.

## Adding Metadata

If the request returns an error response code this processor sets a metadata
field ` + "`http_status_code`" + ` on the resulting message.

If the field ` + "`copy_response_headers` is set to `true`" + ` then any headers
in the response will also be set in the resulting message as metadata.
 
## Error Handling

When all retry attempts for a message are exhausted the processor cancels the
attempt. These failed messages will continue through the pipeline unchanged, but
can be dropped or placed in a dead letter queue according to your config, you
can read about these patterns [here](/docs/configuration/error_handling).`,
		FieldSpecs: append(docs.FieldSpecs{
			docs.FieldCommon("parallel", "When processing batched messages, whether to send messages of the batch in parallel, otherwise they are sent within a single request."),
			docs.FieldDeprecated("max_parallel"),
			docs.FieldDeprecated("request"),
		}, client.FieldSpecs()...),
		Examples: []docs.AnnotatedExample{
			{
				Title: "Branched Request",
				Summary: `
This example uses a ` + "[`branch` processor](/docs/components/processors/branch/)" + ` to strip the request message into an empty body, grab an HTTP payload, and place the result back into the original message at the path ` + "`repo.status`" + `:`,
				Config: `
pipeline:
  processors:
    - branch:
        request_map: 'root = ""'
        processors:
          - http:
              url: https://hub.docker.com/v2/repositories/jeffail/benthos
              verb: GET
        result_map: 'root.repo.status = this'
`,
			},
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			cBytes, err := json.Marshal(conf.HTTP)
			if err != nil {
				return nil, err
			}

			hashMap := map[string]interface{}{}
			if err = json.Unmarshal(cBytes, &hashMap); err != nil {
				return nil, err
			}

			if cmp.Equal(conf.HTTP.Client, client.NewConfig()) {
				delete(hashMap, "request")
			}

			return hashMap, nil
		},
	}
}

//------------------------------------------------------------------------------

// HTTPConfig contains configuration fields for the HTTP processor.
type HTTPConfig struct {
	Parallel      bool          `json:"parallel" yaml:"parallel"`
	MaxParallel   int           `json:"max_parallel" yaml:"max_parallel"`
	Client        client.Config `json:"request" yaml:"request"`
	client.Config `json:",inline" yaml:",inline"`
}

// NewHTTPConfig returns a HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Client:      client.NewConfig(),
		Parallel:    false,
		MaxParallel: 0,
		Config:      client.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// HTTP is a processor that performs an HTTP request using the message as the
// request body, and returns the response.
type HTTP struct {
	client *client.Type

	parallel bool
	max      int

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrHTTP   metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewHTTP returns a HTTP processor.
func NewHTTP(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	if !cmp.Equal(conf.HTTP.Client, client.NewConfig()) {
		if !cmp.Equal(conf.HTTP.Config, client.NewConfig()) {
			return nil, fmt.Errorf("detected a mix of both deprecated http.request and standard http config fields")
		}
		log.Warnln("Using deprecated http.request fields. All fields under the path http.request should now be written directly within http.")
		conf.HTTP.Config = conf.HTTP.Client
	}
	g := &HTTP{
		conf:  conf,
		log:   log,
		stats: stats,

		parallel: conf.HTTP.Parallel,
		max:      conf.HTTP.MaxParallel,

		mCount:     stats.GetCounter("count"),
		mErrHTTP:   stats.GetCounter("error.http"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	var err error
	if g.client, err = client.New(
		conf.HTTP.Config,
		client.OptSetLogger(g.log),
		client.OptSetStats(metrics.Namespaced(g.stats, "client")),
		client.OptSetManager(mgr),
	); err != nil {
		return nil, err
	}
	return g, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (h *HTTP) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	h.mCount.Incr(1)
	var responseMsg types.Message

	if !h.parallel || msg.Len() == 1 {
		// Easy, just do a single request.
		resultMsg, err := h.client.Send(msg)
		if err != nil {
			var codeStr string
			if hErr, ok := err.(types.ErrUnexpectedHTTPRes); ok {
				codeStr = strconv.Itoa(hErr.Code)
			}
			h.mErr.Incr(1)
			h.mErrHTTP.Incr(1)
			h.log.Errorf("HTTP request to '%v' failed: %v\n", h.conf.HTTP.URL, err)
			responseMsg = msg.Copy()
			responseMsg.Iter(func(i int, p types.Part) error {
				if len(codeStr) > 0 {
					p.Metadata().Set("http_status_code", codeStr)
				}
				FlagErr(p, err)
				return nil
			})
		} else {
			parts := make([]types.Part, resultMsg.Len())
			resultMsg.Iter(func(i int, p types.Part) error {
				if i < msg.Len() {
					parts[i] = msg.Get(i).Copy()
				} else {
					parts[i] = msg.Get(0).Copy()
				}
				parts[i].Set(p.Get())
				p.Metadata().Iter(func(k, v string) error {
					parts[i].Metadata().Set(k, v)
					return nil
				})
				return nil
			})
			responseMsg = message.New(nil)
			responseMsg.Append(parts...)
		}
	} else {
		// Hard, need to do parallel requests limited by max parallelism.
		results := make([]types.Part, msg.Len())
		msg.Iter(func(i int, p types.Part) error {
			results[i] = p.Copy()
			return nil
		})
		reqChan, resChan := make(chan int), make(chan error)

		max := h.max
		if max == 0 || msg.Len() < max {
			max = msg.Len()
		}

		for i := 0; i < max; i++ {
			go func() {
				for index := range reqChan {
					result, err := h.client.Send(message.Lock(msg, index))
					if err == nil && result.Len() != 1 {
						err = fmt.Errorf("unexpected response size: %v", result.Len())
					}
					if err == nil {
						results[index].Set(result.Get(0).Get())
						result.Get(0).Metadata().Iter(func(k, v string) error {
							results[index].Metadata().Set(k, v)
							return nil
						})
					} else {
						if hErr, ok := err.(types.ErrUnexpectedHTTPRes); ok {
							results[index].Metadata().Set("http_status_code", strconv.Itoa(hErr.Code))
						}
						FlagErr(results[index], err)
					}
					resChan <- err
				}
			}()
		}
		go func() {
			for i := 0; i < msg.Len(); i++ {
				reqChan <- i
			}
		}()
		for i := 0; i < msg.Len(); i++ {
			if err := <-resChan; err != nil {
				h.mErr.Incr(1)
				h.mErrHTTP.Incr(1)
				h.log.Errorf("HTTP parallel request to '%v' failed: %v\n", h.conf.HTTP.URL, err)
			}
		}

		close(reqChan)
		responseMsg = message.New(nil)
		responseMsg.Append(results...)
	}

	if responseMsg.Len() < 1 {
		return nil, response.NewError(fmt.Errorf(
			"HTTP response from '%v' was empty", h.conf.HTTP.URL,
		))
	}

	msgs := [1]types.Message{responseMsg}

	h.mBatchSent.Incr(1)
	h.mSent.Incr(int64(responseMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (h *HTTP) CloseAsync() {
	h.client.CloseAsync()
}

// WaitForClose blocks until the processor has closed down.
func (h *HTTP) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
