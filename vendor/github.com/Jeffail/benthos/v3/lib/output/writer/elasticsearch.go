package writer

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/http/auth"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/cenkalti/backoff/v4"
	"github.com/olivere/elastic/v7"
	aws "github.com/olivere/elastic/v7/aws/v4"
)

//------------------------------------------------------------------------------

// OptionalAWSConfig contains config fields for AWS authentication with an
// enable flag.
type OptionalAWSConfig struct {
	Enabled     bool `json:"enabled" yaml:"enabled"`
	sess.Config `json:",inline" yaml:",inline"`
}

//------------------------------------------------------------------------------

// ElasticsearchConfig contains configuration fields for the Elasticsearch
// output type.
type ElasticsearchConfig struct {
	URLs           []string             `json:"urls" yaml:"urls"`
	Sniff          bool                 `json:"sniff" yaml:"sniff"`
	Healthcheck    bool                 `json:"healthcheck" yaml:"healthcheck"`
	ID             string               `json:"id" yaml:"id"`
	Index          string               `json:"index" yaml:"index"`
	Pipeline       string               `json:"pipeline" yaml:"pipeline"`
	Type           string               `json:"type" yaml:"type"`
	Timeout        string               `json:"timeout" yaml:"timeout"`
	TLS            btls.Config          `json:"tls" yaml:"tls"`
	Auth           auth.BasicAuthConfig `json:"basic_auth" yaml:"basic_auth"`
	AWS            OptionalAWSConfig    `json:"aws" yaml:"aws"`
	MaxInFlight    int                  `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config `json:",inline" yaml:",inline"`
	Batching       batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewElasticsearchConfig creates a new ElasticsearchConfig with default values.
func NewElasticsearchConfig() ElasticsearchConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	batching := batch.NewPolicyConfig()
	batching.Count = 1

	return ElasticsearchConfig{
		URLs:        []string{"http://localhost:9200"},
		Sniff:       true,
		Healthcheck: true,
		ID:          `${!count("elastic_ids")}-${!timestamp_unix()}`,
		Index:       "benthos_index",
		Pipeline:    "",
		Type:        "doc",
		Timeout:     "5s",
		TLS:         btls.NewConfig(),
		Auth:        auth.NewBasicAuthConfig(),
		AWS: OptionalAWSConfig{
			Enabled: false,
			Config:  sess.NewConfig(),
		},
		MaxInFlight: 1,
		Config:      rConf,
		Batching:    batching,
	}
}

//------------------------------------------------------------------------------

// Elasticsearch is a writer type that writes messages into elasticsearch.
type Elasticsearch struct {
	log   log.Modular
	stats metrics.Type

	urls        []string
	sniff       bool
	healthcheck bool
	conf        ElasticsearchConfig

	backoffCtor func() backoff.BackOff
	timeout     time.Duration
	tlsConf     *tls.Config

	idStr             field.Expression
	indexStr          field.Expression
	pipelineStr       field.Expression
	interpolatedIndex bool

	eJSONErr metrics.StatCounter

	client *elastic.Client
}

// NewElasticsearch creates a new Elasticsearch writer type.
func NewElasticsearch(conf ElasticsearchConfig, log log.Modular, stats metrics.Type) (*Elasticsearch, error) {
	e := Elasticsearch{
		log:         log,
		stats:       stats,
		conf:        conf,
		sniff:       conf.Sniff,
		healthcheck: conf.Healthcheck,
		eJSONErr:    stats.GetCounter("error.json"),
	}

	var err error
	if e.idStr, err = bloblang.NewField(conf.ID); err != nil {
		return nil, fmt.Errorf("failed to parse id expression: %v", err)
	}
	if e.indexStr, err = bloblang.NewField(conf.Index); err != nil {
		return nil, fmt.Errorf("failed to parse index expression: %v", err)
	}
	if e.pipelineStr, err = bloblang.NewField(conf.Pipeline); err != nil {
		return nil, fmt.Errorf("failed to parse pipeline expression: %v", err)
	}

	for _, u := range conf.URLs {
		for _, splitURL := range strings.Split(u, ",") {
			if len(splitURL) > 0 {
				e.urls = append(e.urls, splitURL)
			}
		}
	}

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if e.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	if e.backoffCtor, err = conf.Config.GetCtor(); err != nil {
		return nil, err
	}

	if conf.TLS.Enabled {
		var err error
		if e.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	return &e, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to a Elasticsearch
// broker.
func (e *Elasticsearch) ConnectWithContext(ctx context.Context) error {
	return e.Connect()
}

// Connect attempts to establish a connection to a Elasticsearch broker.
func (e *Elasticsearch) Connect() error {
	if e.client != nil {
		return nil
	}

	opts := []elastic.ClientOptionFunc{
		elastic.SetURL(e.urls...),
		elastic.SetSniff(e.sniff),
		elastic.SetHealthcheck(e.healthcheck),
	}

	if e.conf.Auth.Enabled {
		opts = append(opts, elastic.SetBasicAuth(
			e.conf.Auth.Username, e.conf.Auth.Password,
		))
	}

	if e.conf.TLS.Enabled {
		opts = append(opts, elastic.SetHttpClient(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: e.tlsConf,
			},
			Timeout: e.timeout,
		}))

	} else {
		opts = append(opts, elastic.SetHttpClient(&http.Client{
			Timeout: e.timeout,
		}))
	}

	if e.conf.AWS.Enabled {
		tsess, err := e.conf.AWS.GetSession()
		if err != nil {
			return err
		}
		signingClient := aws.NewV4SigningClient(tsess.Config.Credentials, e.conf.AWS.Region)
		opts = append(opts, elastic.SetHttpClient(signingClient))
	}

	client, err := elastic.NewClient(opts...)
	if err != nil {
		return err
	}

	e.client = client
	e.log.Infof("Sending messages to Elasticsearch index at urls: %s\n", e.urls)
	return nil
}

func shouldRetry(s int) bool {
	if s >= 500 && s <= 599 {
		return true
	}
	return false
}

type pendingBulkIndex struct {
	Index    string
	Pipeline string
	Type     string
	Doc      interface{}
}

// WriteWithContext will attempt to write a message to Elasticsearch, wait for
// acknowledgement, and returns an error if applicable.
func (e *Elasticsearch) WriteWithContext(ctx context.Context, msg types.Message) error {
	return e.Write(msg)
}

// Write will attempt to write a message to Elasticsearch, wait for
// acknowledgement, and returns an error if applicable.
func (e *Elasticsearch) Write(msg types.Message) error {
	if e.client == nil {
		return types.ErrNotConnected
	}

	boff := e.backoffCtor()

	if msg.Len() == 1 {
		index := e.indexStr.String(0, msg)
		_, err := e.client.Index().
			Index(index).
			Pipeline(e.pipelineStr.String(0, msg)).
			Type(e.conf.Type).
			Id(e.idStr.String(0, msg)).
			BodyString(string(msg.Get(0).Get())).
			Do(context.Background())
		if err == nil {
			// Flush to make sure the document got written.
			_, err = e.client.Flush().Index(index).Do(context.Background())
		}
		return err
	}

	requests := map[string]*pendingBulkIndex{}
	msg.Iter(func(i int, part types.Part) error {
		jObj, ierr := part.JSON()
		if ierr != nil {
			e.eJSONErr.Incr(1)
			e.log.Errorf("Failed to marshal message into JSON document: %v\n", ierr)
			return nil
		}
		requests[e.idStr.String(i, msg)] = &pendingBulkIndex{
			Index:    e.indexStr.String(i, msg),
			Pipeline: e.pipelineStr.String(i, msg),
			Type:     e.conf.Type,
			Doc:      jObj,
		}
		return nil
	})

	b := e.client.Bulk()
	for k, v := range requests {
		b.Add(
			elastic.NewBulkIndexRequest().
				Index(v.Index).
				Pipeline(v.Pipeline).
				Type(v.Type).
				Id(k).
				Doc(v.Doc),
		)
	}

	for b.NumberOfActions() != 0 {
		result, err := b.Do(context.Background())
		if err != nil {
			return err
		}

		failed := result.Failed()
		if len(failed) == 0 {
			continue
		}

		wait := boff.NextBackOff()
		for i := 0; i < len(failed); i++ {
			if !shouldRetry(failed[i].Status) {
				e.log.Errorf("Elasticsearch message '%v' rejected with code [%s]: %v\n", failed[i].Id, failed[i].Status, failed[i].Error.Reason)
				return fmt.Errorf("failed to send %v parts from message: %v", len(failed), failed[0].Error.Reason)
			}
			e.log.Errorf("Elasticsearch message '%v' failed with code [%s]: %v\n", failed[i].Id, failed[i].Status, failed[i].Error.Reason)
			id := failed[i].Id
			req := requests[id]
			b.Add(
				elastic.NewBulkIndexRequest().
					Index(req.Index).
					Pipeline(req.Pipeline).
					Type(req.Type).
					Id(id).
					Doc(req.Doc),
			)
		}
		if wait == backoff.Stop {
			return fmt.Errorf("failed to send %v parts from message: %v", len(failed), failed[0].Error.Reason)
		}
		time.Sleep(wait)
	}

	return nil
}

// CloseAsync shuts down the Elasticsearch writer and stops processing messages.
func (e *Elasticsearch) CloseAsync() {
}

// WaitForClose blocks until the Elasticsearch writer has closed down.
func (e *Elasticsearch) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
