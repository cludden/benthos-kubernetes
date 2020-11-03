package input

import (
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/broker"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDynamic] = TypeSpec{
		brokerConstructor: NewDynamic,
		Summary: `
A special broker type where the inputs are identified by unique labels and can
be created, changed and removed during runtime via a REST HTTP interface.`,
		Description: `
To GET a JSON map of input identifiers with their current uptimes use the
` + "`/inputs`" + ` endpoint.

To perform CRUD actions on the inputs themselves use POST, DELETE, and GET
methods on the ` + "`/inputs/{input_id}`" + ` endpoint. When using POST the body
of the request should be a YAML configuration for the input, if the input
already exists it will be changed.`,
		Categories: []Category{
			CategoryUtility,
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			nestedInputs := conf.Dynamic.Inputs
			inMap := map[string]interface{}{}
			for k, input := range nestedInputs {
				sanInput, err := SanitiseConfig(input)
				if err != nil {
					return nil, err
				}
				inMap[k] = sanInput
			}
			return map[string]interface{}{
				"inputs":  inMap,
				"prefix":  conf.Dynamic.Prefix,
				"timeout": conf.Dynamic.Timeout,
			}, nil
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("inputs", "A map of inputs to statically create."),
			docs.FieldCommon("prefix", "A path prefix for HTTP endpoints that are registered."),
			docs.FieldCommon("timeout", "The server side timeout of HTTP requests."),
		},
	}
}

//------------------------------------------------------------------------------

// DynamicConfig contains configuration for the Dynamic input type.
type DynamicConfig struct {
	Inputs  map[string]Config `json:"inputs" yaml:"inputs"`
	Prefix  string            `json:"prefix" yaml:"prefix"`
	Timeout string            `json:"timeout" yaml:"timeout"`
}

// NewDynamicConfig creates a new DynamicConfig with default values.
func NewDynamicConfig() DynamicConfig {
	return DynamicConfig{
		Inputs:  map[string]Config{},
		Prefix:  "",
		Timeout: "5s",
	}
}

//------------------------------------------------------------------------------

// NewDynamic creates a new Dynamic input type.
func NewDynamic(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	dynAPI := api.NewDynamic()

	inputs := map[string]broker.DynamicInput{}
	for k, v := range conf.Dynamic.Inputs {
		newInput, err := New(v, mgr, log, stats, pipelines...)
		if err != nil {
			return nil, err
		}
		inputs[k] = newInput
	}

	var timeout time.Duration
	if tout := conf.Dynamic.Timeout; len(tout) > 0 {
		var err error
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	inputConfigs := conf.Dynamic.Inputs
	inputConfigsMut := sync.RWMutex{}

	fanIn, err := broker.NewDynamicFanIn(
		inputs, log, stats,
		broker.OptDynamicFanInSetOnAdd(func(l string) {
			inputConfigsMut.Lock()
			defer inputConfigsMut.Unlock()

			uConf, exists := inputConfigs[l]
			if !exists {
				return
			}
			sConf, bErr := SanitiseConfig(uConf)
			if bErr != nil {
				log.Errorf("Failed to sanitise config: %v\n", bErr)
			}

			confBytes, _ := json.Marshal(sConf)
			dynAPI.Started(l, confBytes)
			delete(inputConfigs, l)
		}),
		broker.OptDynamicFanInSetOnRemove(func(l string) {
			dynAPI.Stopped(l)
		}),
	)
	if err != nil {
		return nil, err
	}

	dynAPI.OnUpdate(func(id string, c []byte) error {
		newConf := NewConfig()
		if err := yaml.Unmarshal(c, &newConf); err != nil {
			return err
		}
		ns := fmt.Sprintf("dynamic.inputs.%v", id)
		newInput, err := New(
			Config(newConf), mgr,
			log.NewModule("."+ns),
			metrics.Combine(stats, metrics.Namespaced(stats, ns)),
			pipelines...,
		)
		if err != nil {
			return err
		}
		inputConfigsMut.Lock()
		inputConfigs[id] = Config(newConf)
		inputConfigsMut.Unlock()
		if err = fanIn.SetInput(id, newInput, timeout); err != nil {
			log.Errorf("Failed to set input '%v': %v", id, err)
			inputConfigsMut.Lock()
			delete(inputConfigs, id)
			inputConfigsMut.Unlock()
		}
		return err
	})
	dynAPI.OnDelete(func(id string) error {
		err := fanIn.SetInput(id, nil, timeout)
		if err != nil {
			log.Errorf("Failed to close input '%v': %v", id, err)
		}
		return err
	})

	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/inputs/{id}"),
		"Perform CRUD operations on the configuration of dynamic inputs. For"+
			" more information read the `dynamic` input type documentation.",
		dynAPI.HandleCRUD,
	)
	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/inputs"),
		"Get a map of running input identifiers with their current uptimes.",
		dynAPI.HandleList,
	)

	return fanIn, nil
}

//------------------------------------------------------------------------------
