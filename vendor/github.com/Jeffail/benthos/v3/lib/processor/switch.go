package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/docs"
	imessage "github.com/Jeffail/benthos/v3/internal/message"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSwitch] = TypeSpec{
		constructor: NewSwitch,
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
Conditionally processes messages based on their contents.`,
		Description: `
For each switch case a [Bloblang query](/docs/guides/bloblang/about/) is checked and, if the result is true (or the check is empty) the child processors are executed on the message.`,
		Footnotes: `
## Batching

When a switch processor executes on a [batch of messages](/docs/configuration/batching/) they are checked individually and can be matched independently against cases. During processing the messages matched against a case are processed as a batch, although the ordering of messages during case processing cannot be guaranteed to match the order as received.

At the end of switch processing the resulting batch will follow the same ordering as the batch was received. If any child processors have split or otherwise grouped messages this grouping will be lost as the result of a switch is always a single batch. In order to perform conditional grouping and/or splitting use the [` + "`group_by`" + ` processor](/docs/components/processors/group_by/).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether a message should have the processors of this case executed on it. If left empty the case always passes.",
				`this.type == "foo"`,
				`this.contents.urls.contains("https://benthos.dev/")`,
			).HasDefault(""),
			docs.FieldCommon(
				"processors",
				"A list of [processors](/docs/components/processors/about/) to execute on a message.",
			).HasDefault([]interface{}{}),
			docs.FieldAdvanced(
				"fallthrough",
				"Indicates whether, if this case passes for a message, the next case should also be executed.",
			).HasDefault(false),
		},
		Examples: []docs.AnnotatedExample{
			{
				Title: "I Hate George",
				Summary: `
We have a system where we're counting a metric for all messages that pass through our system. However, occasionally we get messages from George where he's rambling about dumb stuff we don't care about.

For Georges messages we want to instead emit a metric that gauges how angry he is about being ignored and then we drop it.`,
				Config: `
pipeline:
  processors:
    - switch:
        - check: this.user.name.first != "George"
          processors:
            - metric:
                type: counter
                name: MessagesWeCareAbout

        - processors:
            - metric:
                type: gauge
                name: GeorgesAnger
                value: ${! json("user.anger") }
            - bloblang: root = deleted()
`,
			},
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			switchSlice := []interface{}{}
			deprecated := false
			for _, switchCase := range conf.Switch {
				if !isDefaultCaseCond(switchCase.Condition) {
					deprecated = true
					break
				}
			}
			for _, switchCase := range conf.Switch {
				var sanProcs []interface{}
				for _, proc := range switchCase.Processors {
					sanProc, err := SanitiseConfig(proc)
					if err != nil {
						return nil, err
					}
					sanProcs = append(sanProcs, sanProc)
				}
				sanit := map[string]interface{}{
					"check":       switchCase.Check,
					"processors":  sanProcs,
					"fallthrough": switchCase.Fallthrough,
				}
				if deprecated {
					sanCond, err := condition.SanitiseConfig(switchCase.Condition)
					if err != nil {
						return nil, err
					}
					sanit["condition"] = sanCond
				}
				switchSlice = append(switchSlice, sanit)
			}
			return switchSlice, nil
		},
	}
}

//------------------------------------------------------------------------------

// SwitchCaseConfig contains a condition, processors and other fields for an
// individual case in the Switch processor.
type SwitchCaseConfig struct {
	Condition   condition.Config `json:"condition" yaml:"condition"`
	Check       string           `json:"check" yaml:"check"`
	Processors  []Config         `json:"processors" yaml:"processors"`
	Fallthrough bool             `json:"fallthrough" yaml:"fallthrough"`
}

// NewSwitchCaseConfig returns a new SwitchCaseConfig with default values.
func NewSwitchCaseConfig() SwitchCaseConfig {
	cond := condition.NewConfig()
	cond.Type = condition.TypeStatic
	cond.Static = true
	return SwitchCaseConfig{
		Condition:   cond,
		Check:       "",
		Processors:  []Config{},
		Fallthrough: false,
	}
}

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchCaseConfig) UnmarshalJSON(bytes []byte) error {
	type confAlias SwitchCaseConfig
	aliased := confAlias(NewSwitchCaseConfig())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*s = SwitchCaseConfig(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchCaseConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias SwitchCaseConfig
	aliased := confAlias(NewSwitchCaseConfig())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	*s = SwitchCaseConfig(aliased)
	return nil
}

//------------------------------------------------------------------------------

// SwitchConfig is a config struct containing fields for the Switch processor.
type SwitchConfig []SwitchCaseConfig

// NewSwitchConfig returns a default SwitchConfig.
func NewSwitchConfig() SwitchConfig {
	return SwitchConfig{}
}

//------------------------------------------------------------------------------

// switchCase contains a condition, processors and other fields for an
// individual case in the Switch processor.
type switchCase struct {
	check       *mapping.Executor
	processors  []types.Processor
	fallThrough bool
}

// Switch is a processor that only applies child processors under a certain
// condition.
type Switch struct {
	cases []switchCase
	log   log.Modular

	mCount metrics.StatCounter
	mSent  metrics.StatCounter
}

func isDefaultCaseCond(cond condition.Config) bool {
	return cond.Type == condition.TypeStatic && cond.Static
}

// NewSwitch returns a Switch processor.
func NewSwitch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	deprecated := false
	for _, caseConf := range conf.Switch {
		if deprecated || !isDefaultCaseCond(caseConf.Condition) {
			deprecated = true
		}
		if deprecated {
			if len(caseConf.Check) > 0 {
				return nil, errors.New("cannot use both deprecated condition field in combination with field check")
			}
		}
	}
	if deprecated {
		return newSwitchDeprecated(conf, mgr, log, stats)
	}

	var cases []switchCase
	for i, caseConf := range conf.Switch {
		prefix := strconv.Itoa(i)

		var err error
		var check *mapping.Executor
		var procs []types.Processor

		if len(caseConf.Check) > 0 {
			if check, err = bloblang.NewMapping("", caseConf.Check); err != nil {
				return nil, fmt.Errorf("failed to parse case %v check: %w", i, err)
			}
		}

		for j, procConf := range caseConf.Processors {
			procPrefix := prefix + "." + strconv.Itoa(j)
			var proc types.Processor
			if proc, err = New(
				procConf, mgr,
				log.NewModule("."+procPrefix),
				metrics.Namespaced(stats, procPrefix),
			); err != nil {
				return nil, fmt.Errorf("case [%v] processor [%v]: %w", i, j, err)
			}
			procs = append(procs, proc)
		}

		cases = append(cases, switchCase{
			check:       check,
			processors:  procs,
			fallThrough: caseConf.Fallthrough,
		})
	}
	return &Switch{
		cases: cases,
		log:   log,

		mCount: stats.GetCounter("count"),
		mSent:  stats.GetCounter("sent"),
	}, nil
}

//------------------------------------------------------------------------------

func reorderFromTags(tags []*imessage.Tag, parts []types.Part) {
	sort.Slice(parts, func(i, j int) bool {
		iFound, jFound := false, false
		for _, t := range tags {
			if !iFound && imessage.HasTag(t, parts[i]) {
				iFound = true
				i = t.Index
			}
			if !jFound && imessage.HasTag(t, parts[j]) {
				jFound = true
				j = t.Index
			}
			if iFound && jFound {
				break
			}
		}
		return i < j
	})
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *Switch) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	s.mCount.Incr(1)

	var result []types.Part

	var tags []*imessage.Tag
	var remaining []types.Part
	var carryOver []types.Part

	tags = make([]*imessage.Tag, msg.Len())
	remaining = make([]types.Part, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		tag := imessage.NewTag(i)
		tags[i] = tag
		remaining[i] = imessage.WithTag(tag, p)
		return nil
	})

	for i, switchCase := range s.cases {
		passed, failed := carryOver, []types.Part{}

		// Form a message to test against, consisting of fallen through messages
		// from prior cases plus remaining messages that haven't passed a case
		// yet.
		testMsg := message.New(nil)
		testMsg.Append(remaining...)

		for j, p := range remaining {
			test := switchCase.check == nil
			if !test {
				var err error
				if test, err = switchCase.check.QueryPart(j, testMsg); err != nil {
					test = false
					s.log.Errorf("Failed to test case %v: %v\n", i, err)
				}
			}
			if test {
				passed = append(passed, p)
			} else {
				failed = append(failed, p)
			}
		}

		carryOver = nil
		remaining = failed

		if len(passed) > 0 {
			execMsg := message.New(nil)
			execMsg.SetAll(passed)

			msgs, res := ExecuteAll(switchCase.processors, execMsg)
			if res != nil && res.Error() != nil {
				return nil, res
			}

			for _, m := range msgs {
				m.Iter(func(_ int, p types.Part) error {
					if switchCase.fallThrough {
						carryOver = append(carryOver, p)
					} else {
						result = append(result, p)
					}
					return nil
				})
			}
		}
	}

	result = append(result, remaining...)
	if len(result) > 1 {
		reorderFromTags(tags, result)
	}

	resMsg := message.New(nil)
	resMsg.SetAll(result)

	if resMsg.Len() == 0 {
		return nil, response.NewAck()
	}

	s.mSent.Incr(int64(resMsg.Len()))
	return []types.Message{resMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *Switch) CloseAsync() {
	for _, s := range s.cases {
		for _, proc := range s.processors {
			proc.CloseAsync()
		}
	}
}

// WaitForClose blocks until the processor has closed down.
func (s *Switch) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, s := range s.cases {
		for _, proc := range s.processors {
			if err := proc.WaitForClose(time.Until(stopBy)); err != nil {
				return err
			}
		}
	}
	return nil
}

//------------------------------------------------------------------------------
