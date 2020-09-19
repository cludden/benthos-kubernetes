package processor

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMetric] = TypeSpec{
		constructor: NewMetric,
		Categories: []Category{
			CategoryUtility,
		},
		Summary: "Emit custom metrics by extracting values from messages.",
		Description: `
This processor works by evaluating an [interpolated field ` + "`value`" + `](/docs/configuration/interpolation#bloblang-queries) for each message and updating a emitted metric according to the [type](#types).

Custom metrics such as these are emitted along with Benthos internal metrics, where you can customize where metrics are sent, which metric names are emitted and rename them as/when appropriate. For more information check out the [metrics docs here](/docs/components/metrics/about).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("type", "The metric [type](#types) to create.").HasOptions(
				"counter",
				"counter_by",
				"gauge",
				"timing",
			),
			docs.FieldDeprecated("path"),
			docs.FieldCommon("name", "The name of the metric to create, this must be unique across all Benthos components otherwise it will overwrite those other metrics."),
			docs.FieldCommon(
				"labels", "A map of label names and values that can be used to enrich metrics. Labels are not supported by some metric destinations, in which case the metrics series are combined.",
				map[string]string{
					"type":  "${! json(\"doc.type\") }",
					"topic": "${! meta(\"kafka_topic\") }",
				},
			).SupportsInterpolation(true),
			docs.FieldCommon("value", "For some metric types specifies a value to set, increment.").SupportsInterpolation(true),
			partsFieldSpec,
		},
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Counter",
				Summary: "In this example we emit a counter metric called `Foos`, which increments for every message processed, and we label the metric with some metadata about where the message came from and a field from the document that states what type it is. We also configure our metrics to emit to CloudWatch, and explicitly only allow our custom metric and some internal Benthos metrics to emit.",
				Config: `
pipeline:
  processors:
    - metric:
        name: Foos
        type: counter
        labels:
          topic: ${! meta("kafka_topic") }
          parition: ${! meta("kafka_partition") }
          type: ${! json("document.type").or("unknown") }

metrics:
  cloudwatch:
    namespace: ProdConsumer
    region: eu-west-1
    path_mapping: |
      root = if ![
        "Foos",
        "input.received",
        "output.sent"
      ].contains(this) { deleted() }
`,
			},
			{
				Title:   "Gauge",
				Summary: "In this example we emit a gauge metric called `FooSize`, which is given a value extracted from JSON messages at the path `foo.size`. We then also configure our Prometheus metric exporter to only emit this custom metric and nothing else. We also label the metric with some metadata.",
				Config: `
pipeline:
  processors:
    - metric:
        name: FooSize
        type: gauge
        labels:
          topic: ${! meta("kafka_topic") }
        value: ${! json("foo.size") }

metrics:
  prometheus:
    path_mapping: 'if this != "FooSize" { deleted() }'
`,
			},
		},
		Footnotes: `
## Types

### ` + "`counter`" + `

Increments a counter by exactly 1, the contents of ` + "`value`" + ` are ignored
by this type.

### ` + "`counter_by`" + `

If the contents of ` + "`value`" + ` can be parsed as a positive integer value
then the counter is incremented by this value.

For example, the following configuration will increment the value of the
` + "`count.custom.field` metric by the contents of `field.some.value`" + `:

` + "```yaml" + `
metric:
  type: counter_by
  name: CountCustomField
  value: ${!json("field.some.value")}
` + "```" + `

### ` + "`gauge`" + `

If the contents of ` + "`value`" + ` can be parsed as a positive integer value
then the gauge is set to this value.

For example, the following configuration will set the value of the
` + "`gauge.custom.field` metric to the contents of `field.some.value`" + `:

` + "```yaml" + `
metric:
  type: gauge
  path: GaugeCustomField
  value: ${!json("field.some.value")}
` + "```" + `

### ` + "`timing`" + `

Equivalent to ` + "`gauge`" + ` where instead the metric is a timing.`,
	}
}

//------------------------------------------------------------------------------

// MetricConfig contains configuration fields for the Metric processor.
type MetricConfig struct {
	Parts  []int             `json:"parts" yaml:"parts"`
	Type   string            `json:"type" yaml:"type"`
	Path   string            `json:"path" yaml:"path"`
	Name   string            `json:"name" yaml:"name"`
	Labels map[string]string `json:"labels" yaml:"labels"`
	Value  string            `json:"value" yaml:"value"`
}

// NewMetricConfig returns a MetricConfig with default values.
func NewMetricConfig() MetricConfig {
	return MetricConfig{
		Parts:  []int{},
		Type:   "counter",
		Path:   "",
		Name:   "",
		Labels: map[string]string{},
		Value:  "",
	}
}

//------------------------------------------------------------------------------

// Metric is a processor that creates a metric from extracted values from a message part.
type Metric struct {
	parts      []int
	deprecated bool

	conf  Config
	log   log.Modular
	stats metrics.Type

	value  field.Expression
	labels labels

	mCounter metrics.StatCounter
	mGauge   metrics.StatGauge
	mTimer   metrics.StatTimer

	mCounterVec metrics.StatCounterVec
	mGaugeVec   metrics.StatGaugeVec
	mTimerVec   metrics.StatTimerVec

	handler func(string, int, types.Message) error
}

type labels []label
type label struct {
	name  string
	value field.Expression
}

func (l *label) val(index int, msg types.Message) string {
	return l.value.String(index, msg)
}

func (l labels) names() []string {
	var names []string
	for i := range l {
		names = append(names, l[i].name)
	}
	return names
}

func (l labels) values(index int, msg types.Message) []string {
	var values []string
	for i := range l {
		values = append(values, l[i].val(index, msg))
	}
	return values
}

func unwrapMetric(t metrics.Type) metrics.Type {
	for {
		u, ok := t.(interface {
			Unwrap() metrics.Type
		})
		if ok {
			t = u.Unwrap()
		} else {
			break
		}
	}
	return t
}

// NewMetric returns a Metric processor.
func NewMetric(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	value, err := bloblang.NewField(conf.Metric.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}

	m := &Metric{
		parts: conf.Metric.Parts,
		conf:  conf,
		log:   log,
		stats: stats,
		value: value,
	}

	name := conf.Metric.Name
	if len(conf.Metric.Path) > 0 {
		if len(conf.Metric.Name) > 0 {
			return nil, errors.New("cannot combine deprecated path field with name field")
		}
		if len(conf.Metric.Parts) > 0 {
			return nil, errors.New("cannot combine deprecated path field with parts field")
		}
		m.deprecated = true
		name = conf.Metric.Path
	}
	if len(name) == 0 {
		return nil, errors.New("metric name must not be empty")
	}
	if !m.deprecated {
		// Remove any namespaces from the metric type.
		stats = unwrapMetric(stats)
	}

	labelNames := make([]string, 0, len(conf.Metric.Labels))
	for n := range conf.Metric.Labels {
		labelNames = append(labelNames, n)
	}
	sort.Strings(labelNames)

	for _, n := range labelNames {
		v, err := bloblang.NewField(conf.Metric.Labels[n])
		if err != nil {
			return nil, fmt.Errorf("failed to parse label '%v' expression: %v", n, err)
		}
		m.labels = append(m.labels, label{
			name:  n,
			value: v,
		})
	}

	switch strings.ToLower(conf.Metric.Type) {
	case "counter":
		if len(m.labels) > 0 {
			m.mCounterVec = stats.GetCounterVec(name, m.labels.names())
		} else {
			m.mCounter = stats.GetCounter(name)
		}
		m.handler = m.handleCounter
	case "counter_parts":
		if len(m.labels) > 0 {
			m.mCounterVec = stats.GetCounterVec(name, m.labels.names())
		} else {
			m.mCounter = stats.GetCounter(name)
		}
		m.handler = m.handleCounterParts
	case "counter_by":
		if len(m.labels) > 0 {
			m.mCounterVec = stats.GetCounterVec(name, m.labels.names())
		} else {
			m.mCounter = stats.GetCounter(name)
		}
		m.handler = m.handleCounterBy
	case "gauge":
		if len(m.labels) > 0 {
			m.mGaugeVec = stats.GetGaugeVec(name, m.labels.names())
		} else {
			m.mGauge = stats.GetGauge(name)
		}
		m.handler = m.handleGauge
	case "timing":
		if len(m.labels) > 0 {
			m.mTimerVec = stats.GetTimerVec(name, m.labels.names())
		} else {
			m.mTimer = stats.GetTimer(name)
		}
		m.handler = m.handleTimer
	default:
		return nil, fmt.Errorf("metric type unrecognised: %v", conf.Metric.Type)
	}

	return m, nil
}

func (m *Metric) handleCounter(val string, index int, msg types.Message) error {
	if len(m.labels) > 0 {
		m.mCounterVec.With(m.labels.values(index, msg)...).Incr(1)
	} else {
		m.mCounter.Incr(1)
	}
	return nil
}

// TODO: V4 Remove this
func (m *Metric) handleCounterParts(val string, index int, msg types.Message) error {
	if msg.Len() == 0 {
		return nil
	}
	if len(m.labels) > 0 {
		m.mCounterVec.With(m.labels.values(index, msg)...).Incr(int64(msg.Len()))
	} else {
		m.mCounter.Incr(int64(msg.Len()))
	}
	return nil
}

func (m *Metric) handleCounterBy(val string, index int, msg types.Message) error {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	if i < 0 {
		return errors.New("value is negative")
	}
	if len(m.labels) > 0 {
		m.mCounterVec.With(m.labels.values(index, msg)...).Incr(i)
	} else {
		m.mCounter.Incr(i)
	}
	return nil
}

func (m *Metric) handleGauge(val string, index int, msg types.Message) error {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	if i < 0 {
		return errors.New("value is negative")
	}
	if len(m.labels) > 0 {
		m.mGaugeVec.With(m.labels.values(index, msg)...).Set(i)
	} else {
		m.mGauge.Set(i)
	}
	return nil
}

func (m *Metric) handleTimer(val string, index int, msg types.Message) error {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	if i < 0 {
		return errors.New("value is negative")
	}
	if len(m.labels) > 0 {
		m.mTimerVec.With(m.labels.values(index, msg)...).Timing(i)
	} else {
		m.mTimer.Timing(i)
	}
	return nil
}

// ProcessMessage applies the processor to a message
func (m *Metric) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	if m.deprecated {
		value := m.value.String(0, msg)
		if err := m.handler(value, 0, msg); err != nil {
			m.log.Errorf("Handler error: %v\n", err)
		}
		return []types.Message{msg}, nil
	}
	iterateParts(m.parts, msg, func(index int, p types.Part) error {
		value := m.value.String(index, msg)
		if err := m.handler(value, index, msg); err != nil {
			m.log.Errorf("Handler error: %v\n", err)
		}
		return nil
	})
	return []types.Message{msg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (m *Metric) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (m *Metric) WaitForClose(timeout time.Duration) error {
	return nil
}
