package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/gorilla/mux"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// Config contains the configuration fields for the Benthos API.
type Config struct {
	Address        string `json:"address" yaml:"address"`
	Enabled        bool   `json:"enabled" yaml:"enabled"`
	ReadTimeout    string `json:"read_timeout" yaml:"read_timeout"`
	RootPath       string `json:"root_path" yaml:"root_path"`
	DebugEndpoints bool   `json:"debug_endpoints" yaml:"debug_endpoints"`
}

// NewConfig creates a new API config with default values.
func NewConfig() Config {
	return Config{
		Address:        "0.0.0.0:4195",
		Enabled:        true,
		ReadTimeout:    "5s",
		RootPath:       "/benthos",
		DebugEndpoints: false,
	}
}

//------------------------------------------------------------------------------

// Type implements the Benthos HTTP API.
type Type struct {
	conf         Config
	endpoints    map[string]string
	endpointsMut sync.Mutex

	ctx    context.Context
	cancel func()

	handlers    map[string]http.HandlerFunc
	handlersMut sync.RWMutex

	mux    *mux.Router
	server *http.Server
}

// New creates a new Benthos HTTP API.
func New(
	version string,
	dateBuilt string,
	conf Config,
	wholeConf interface{},
	log log.Modular,
	stats metrics.Type,
) (*Type, error) {
	handler := mux.NewRouter()
	server := &http.Server{
		Addr:    conf.Address,
		Handler: handler,
	}

	if tout := conf.ReadTimeout; len(tout) > 0 {
		var err error
		if server.ReadTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse read timeout string: %v", err)
		}
	}

	t := &Type{
		conf:      conf,
		endpoints: map[string]string{},
		handlers:  map[string]http.HandlerFunc{},
		mux:       handler,
		server:    server,
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())

	handlePing := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	}

	handleStackTrace := func(w http.ResponseWriter, r *http.Request) {
		stackSlice := make([]byte, 1024*100)
		s := runtime.Stack(stackSlice, true)
		w.Write(stackSlice[:s])
	}

	handlePrintJSONConfig := func(w http.ResponseWriter, r *http.Request) {
		resBytes, err := json.Marshal(wholeConf)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Write(resBytes)
	}

	handlePrintYAMLConfig := func(w http.ResponseWriter, r *http.Request) {
		resBytes, err := yaml.Marshal(wholeConf)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Write(resBytes)
	}

	handleVersion := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("{\"version\":\"%v\", \"built\":\"%v\"}", version, dateBuilt)))
	}

	handleEndpoints := func(w http.ResponseWriter, r *http.Request) {
		t.endpointsMut.Lock()
		defer t.endpointsMut.Unlock()

		resBytes, err := json.Marshal(t.endpoints)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
		} else {
			w.Write(resBytes)
		}
	}

	if t.conf.DebugEndpoints {
		t.RegisterEndpoint(
			"/debug/config/json", "DEBUG: Returns the loaded config as JSON.",
			handlePrintJSONConfig,
		)
		t.RegisterEndpoint(
			"/debug/config/yaml", "DEBUG: Returns the loaded config as YAML.",
			handlePrintYAMLConfig,
		)
		t.RegisterEndpoint(
			"/debug/stack", "DEBUG: Returns a snapshot of the current service stack trace.",
			handleStackTrace,
		)
		t.RegisterEndpoint(
			"/debug/pprof/profile", "DEBUG: Responds with a pprof-formatted cpu profile.",
			pprof.Profile,
		)
		t.RegisterEndpoint(
			"/debug/pprof/heap", "DEBUG: Responds with a pprof-formatted heap profile.",
			pprof.Index,
		)
		t.RegisterEndpoint(
			"/debug/pprof/block", "DEBUG: Responds with a pprof-formatted block profile.",
			pprof.Index,
		)
		t.RegisterEndpoint(
			"/debug/pprof/mutex", "DEBUG: Responds with a pprof-formatted mutex profile.",
			pprof.Index,
		)
		t.RegisterEndpoint(
			"/debug/pprof/symbol", "DEBUG: looks up the program counters listed"+
				" in the request, responding with a table mapping program"+
				" counters to function names.",
			pprof.Symbol,
		)
		t.RegisterEndpoint(
			"/debug/pprof/trace",
			"DEBUG: Responds with the execution trace in binary form."+
				" Tracing lasts for duration specified in seconds GET"+
				" parameter, or for 1 second if not specified.",
			pprof.Trace,
		)
	}

	t.RegisterEndpoint("/ping", "Ping me.", handlePing)
	t.RegisterEndpoint("/version", "Returns the service version.", handleVersion)
	t.RegisterEndpoint("/endpoints", "Returns this map of endpoints.", handleEndpoints)

	// If we want to expose a JSON stats endpoint we register the endpoints.
	if wHandlerFunc, ok := stats.(metrics.WithHandlerFunc); ok {
		t.RegisterEndpoint(
			"/stats", "Returns a JSON object of service metrics.",
			wHandlerFunc.HandlerFunc(),
		)
		t.RegisterEndpoint(
			"/metrics", "Returns a JSON object of service metrics.",
			wHandlerFunc.HandlerFunc(),
		)
	}

	return t, nil
}

// RegisterEndpoint registers a http.HandlerFunc under a path with a
// description that will be displayed under the /endpoints path.
func (t *Type) RegisterEndpoint(path, desc string, handler http.HandlerFunc) {
	t.endpointsMut.Lock()
	defer t.endpointsMut.Unlock()

	t.endpoints[path] = desc

	t.handlersMut.Lock()
	defer t.handlersMut.Unlock()

	if _, exists := t.handlers[path]; !exists {
		wrapHandler := func(w http.ResponseWriter, r *http.Request) {
			t.handlersMut.RLock()
			h := t.handlers[path]
			t.handlersMut.RUnlock()
			h(w, r)
		}
		t.mux.HandleFunc(path, wrapHandler)
		t.mux.HandleFunc(t.conf.RootPath+path, wrapHandler)
	}
	t.handlers[path] = handler
}

// ListenAndServe launches the API and blocks until the server closes or fails.
func (t *Type) ListenAndServe() error {
	if !t.conf.Enabled {
		<-t.ctx.Done()
		return nil
	}
	return t.server.ListenAndServe()
}

// Shutdown attempts to close the http server.
func (t *Type) Shutdown(ctx context.Context) error {
	t.cancel()
	return t.server.Shutdown(ctx)
}

//------------------------------------------------------------------------------
