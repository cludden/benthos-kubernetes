package tracer

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeJaeger] = TypeSpec{
		constructor: NewJaeger,
		Summary: `
Send spans to a [Jaeger](https://www.jaegertracing.io/) agent.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("agent_address", "The address of a Jaeger agent to send tracing events to."),
			docs.FieldCommon("service_name", "A name to provide for this service."),
			docs.FieldCommon("sampler_type", "The sampler type to use.").HasOptions(
				"const", "probabilistic", "ratelimiting", "remote",
			),
			docs.FieldAdvanced("sampler_manager_address", "An optional address of a sampler manager."),
			docs.FieldAdvanced("sampler_param", "A parameter to use for sampling. This field is unused for some sampling types."),
			docs.FieldAdvanced("tags", "A map of tags to add to tracing spans."),
			docs.FieldCommon("flush_interval", "The period of time between each flush of tracing spans."),
		},
	}
}

//------------------------------------------------------------------------------

// JaegerConfig is config for the Jaeger metrics type.
type JaegerConfig struct {
	AgentAddress          string            `json:"agent_address" yaml:"agent_address"`
	ServiceName           string            `json:"service_name" yaml:"service_name"`
	SamplerType           string            `json:"sampler_type" yaml:"sampler_type"`
	SamplerManagerAddress string            `json:"sampler_manager_address" yaml:"sampler_manager_address"`
	SamplerParam          float64           `json:"sampler_param" yaml:"sampler_param"`
	Tags                  map[string]string `json:"tags" yaml:"tags"`
	FlushInterval         string            `json:"flush_interval" yaml:"flush_interval"`
}

// NewJaegerConfig creates an JaegerConfig struct with default values.
func NewJaegerConfig() JaegerConfig {
	return JaegerConfig{
		AgentAddress:          "localhost:6831",
		ServiceName:           "benthos",
		SamplerType:           "const",
		SamplerManagerAddress: "",
		SamplerParam:          1.0,
		Tags:                  map[string]string{},
		FlushInterval:         "",
	}
}

//------------------------------------------------------------------------------

// Jaeger is a tracer with the capability to push spans to a Jaeger instance.
type Jaeger struct {
	closer io.Closer
}

// NewJaeger creates and returns a new Jaeger object.
func NewJaeger(config Config, opts ...func(Type)) (Type, error) {
	j := &Jaeger{}

	for _, opt := range opts {
		opt(j)
	}

	var sampler *jaegercfg.SamplerConfig
	if sType := config.Jaeger.SamplerType; len(sType) > 0 {
		sampler = &jaegercfg.SamplerConfig{
			Param:             config.Jaeger.SamplerParam,
			SamplingServerURL: config.Jaeger.SamplerManagerAddress,
		}
		switch strings.ToLower(sType) {
		case "const":
			sampler.Type = jaeger.SamplerTypeConst
		case "probabilistic":
			sampler.Type = jaeger.SamplerTypeProbabilistic
		case "ratelimiting":
			sampler.Type = jaeger.SamplerTypeRateLimiting
		case "remote":
			sampler.Type = jaeger.SamplerTypeRemote
		default:
			return nil, fmt.Errorf("unrecognised sampler type: %v", sType)
		}
	}

	cfg := jaegercfg.Configuration{
		ServiceName: config.Jaeger.ServiceName,
		Sampler:     sampler,
	}

	if tags := config.Jaeger.Tags; len(tags) > 0 {
		var jTags []opentracing.Tag
		for k, v := range config.Jaeger.Tags {
			jTags = append(jTags, opentracing.Tag{
				Key:   k,
				Value: v,
			})
		}
		cfg.Tags = jTags
	}

	reporterConf := &jaegercfg.ReporterConfig{}

	if i := config.Jaeger.FlushInterval; len(i) > 0 {
		flushInterval, err := time.ParseDuration(i)
		if err != nil {
			return nil, fmt.Errorf("failed to parse flush interval '%s': %v", i, err)
		}
		reporterConf.BufferFlushInterval = flushInterval
		cfg.Reporter = reporterConf
	}

	if i := config.Jaeger.AgentAddress; len(i) > 0 {
		reporterConf.LocalAgentHostPort = i
		cfg.Reporter = reporterConf
	}

	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		return nil, err
	}
	opentracing.SetGlobalTracer(tracer)
	j.closer = closer

	return j, nil
}

//------------------------------------------------------------------------------

// Close stops the tracer.
func (j *Jaeger) Close() error {
	if j.closer != nil {
		j.closer.Close()
		j.closer = nil
	}
	return nil
}

//------------------------------------------------------------------------------
