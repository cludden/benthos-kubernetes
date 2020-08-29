package tracer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// Errors for the tracer package.
var (
	ErrInvalidTracerType = errors.New("invalid tracer type")
)

//------------------------------------------------------------------------------

// TypeSpec is a constructor and a usage description for each tracer type.
type TypeSpec struct {
	constructor        func(conf Config, opts ...func(Type)) (Type, error)
	sanitiseConfigFunc func(conf Config) (interface{}, error)

	Summary     string
	Description string
	Footnotes   string
	FieldSpecs  docs.FieldSpecs
	Beta        bool
	Deprecated  bool
}

// Constructors is a map of all tracer types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each tracer type.
const (
	TypeJaeger = "jaeger"
	TypeNone   = "none"
)

//------------------------------------------------------------------------------

// Type is an interface implemented by all tracer types.
type Type interface {
	// Close stops and cleans up the tracers resources.
	Close() error
}

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all tracer types.
type Config struct {
	Type   string       `json:"type" yaml:"type"`
	Jaeger JaegerConfig `json:"jaeger" yaml:"jaeger"`
	None   struct{}     `json:"none" yaml:"none"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:   TypeNone,
		Jaeger: NewJaegerConfig(),
		None:   struct{}{},
	}
}

// SanitiseConfig returns a sanitised version of the Config, meaning sections
// that aren't relevant to behaviour are removed.
func SanitiseConfig(conf Config) (interface{}, error) {
	return conf.Sanitised(false)
}

// Sanitised returns a sanitised version of the config, meaning sections that
// aren't relevant to behaviour are removed. Also optionally removes deprecated
// fields.
func (conf Config) Sanitised(removeDeprecated bool) (interface{}, error) {
	cBytes, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = json.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	outputMap := config.Sanitised{}

	t := conf.Type
	outputMap["type"] = t
	if sfunc := Constructors[t].sanitiseConfigFunc; sfunc != nil {
		if outputMap[t], err = sfunc(conf); err != nil {
			return nil, err
		}
	} else {
		outputMap[t] = hashMap[t]
	}

	def := Constructors[t]

	if removeDeprecated {
		if m, ok := outputMap[t].(map[string]interface{}); ok {
			for _, spec := range def.FieldSpecs {
				if spec.Deprecated {
					delete(m, spec.Name)
				}
			}
		}
	}

	return outputMap, nil
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := value.Decode(&aliased); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	var raw interface{}
	if err := value.Decode(&raw); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}
	if typeCandidates := config.GetInferenceCandidates(raw); len(typeCandidates) > 0 {
		var inferredType string
		for _, tc := range typeCandidates {
			if _, exists := Constructors[tc]; exists {
				if len(inferredType) > 0 {
					return fmt.Errorf("line %v: unable to infer type, multiple candidates '%v' and '%v'", value.Line, inferredType, tc)
				}
				inferredType = tc
			}
		}
		if len(inferredType) == 0 {
			return fmt.Errorf("line %v: unable to infer type, candidates were: %v", value.Line, typeCandidates)
		}
		aliased.Type = inferredType
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-tracers`" + `

A tracer type represents a destination for Benthos to send opentracing events to
such as [Jaeger](https://www.jaegertracing.io/).

When a tracer is configured all messages will be allocated a root span during
ingestion that represents their journey through a Benthos pipeline. Many Benthos
processors create spans, and so opentracing is a great way to analyse the
pathways of individual messages as they progress through a Benthos instance.

Some inputs, such as ` + "`http_server` and `http_client`" + `, are capable of
extracting a root span from the source of the message (HTTP headers). This is
a work in progress and should eventually expand so that all inputs have a way of
doing so.

A tracer config section looks like this:

` + "``` yaml" + `
tracer:
  jaeger:
    agent_address: localhost:6831
    sampler_param: 1
    sampler_type: const
    service_name: benthos
` + "```" + `

WARNING: Although the configuration spec of this component is stable the format
of spans, tags and logs created by Benthos is subject to change as it is tuned
for improvement.`

// Descriptions returns a formatted string of collated descriptions of each
// type.
func Descriptions() string {
	// Order our types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Tracer Types\n")
	buf.WriteString(strings.Repeat("=", 12))
	buf.WriteString("\n\n")
	buf.WriteString(header)
	buf.WriteString("\n\n")

	// Append each description
	for i, name := range names {
		var confBytes []byte

		conf := NewConfig()
		conf.Type = name
		if confSanit, err := SanitiseConfig(conf); err == nil {
			confBytes, _ = config.MarshalYAML(confSanit)
		}

		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		if confBytes != nil {
			buf.WriteString("\n``` yaml\n")
			buf.Write(confBytes)
			buf.WriteString("```\n")
		}
		buf.WriteString(Constructors[name].Description)
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n---\n")
		}
	}
	return buf.String()
}

// New creates a tracer type based on a configuration.
func New(conf Config, opts ...func(Type)) (Type, error) {
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, opts...)
	}
	return nil, ErrInvalidTracerType
}

//------------------------------------------------------------------------------
