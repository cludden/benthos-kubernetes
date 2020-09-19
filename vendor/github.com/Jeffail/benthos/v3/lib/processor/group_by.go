package processor

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/google/go-cmp/cmp"
	olog "github.com/opentracing/opentracing-go/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGroupBy] = TypeSpec{
		constructor: NewGroupBy,
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
Splits a [batch of messages](/docs/configuration/batching/) into N batches, where each resulting batch contains a group of messages determined by a [Bloblang query](/docs/guides/bloblang/about/).`,
		Description: `
Once the groups are established a list of processors are applied to their respective grouped batch, which can be used to label the batch as per their grouping. Messages that do not pass the check of any specified group are placed in their own group.`,
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Grouped Processing",
				Summary: "Imagine we have a batch of messages that we wish to split into a group of foos and everything else, which should be sent to different output destinations based on those groupings. We also need to send the foos as a tar gzip archive. For this purpose we can use the `group_by` processor with a [`switch`](/docs/components/outputs/switch) output:",
				Config: `
pipeline:
  processors:
    - group_by:
      - check: content().contains("this is a foo")
        processors:
          - archive:
              format: tar
          - compress:
              algorithm: gzip
          - bloblang: 'meta grouping = "foo"'

output:
  switch:
    cases:
      - check: meta("grouping") == "foo"
        output:
          gcp_pubsub:
            project: foo_prod
            topic: only_the_foos
      - output:
          gcp_pubsub:
            project: somewhere_else
            topic: no_foos_here
`,
			},
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether a message belongs to a given group.",
				`this.type == "foo"`,
				`this.contents.urls.contains("https://benthos.dev/")`,
				`true`,
			).HasDefault(""),
			docs.FieldDeprecated("condition"),
			docs.FieldCommon(
				"processors",
				"A list of [processors](/docs/components/processors/about/) to execute on the newly formed group.",
			).HasDefault([]interface{}{}),
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			groups := []interface{}{}
			for _, g := range conf.GroupBy {
				procsSanit := []interface{}{}
				for _, p := range g.Processors {
					procSanit, err := SanitiseConfig(p)
					if err != nil {
						return nil, err
					}
					procsSanit = append(procsSanit, procSanit)
				}
				groupSanit := map[string]interface{}{
					"processors": procsSanit,
					"check":      g.Check,
				}
				if !isDefaultGroupCond(g.Condition) {
					condSanit, err := condition.SanitiseConfig(g.Condition)
					if err != nil {
						return nil, err
					}
					groupSanit["condition"] = condSanit
				}
				groups = append(groups, groupSanit)
			}
			return groups, nil
		},
		UsesBatches: true,
	}
}

//------------------------------------------------------------------------------

func isDefaultGroupCond(cond condition.Config) bool {
	if cond.Type == "" {
		return true
	}
	return cmp.Equal(cond, condition.NewConfig())
}

// GroupByElement represents a group determined by a condition and a list of
// group specific processors.
type GroupByElement struct {
	Condition  condition.Config `json:"condition" yaml:"condition"`
	Check      string           `json:"check" yaml:"check"`
	Processors []Config         `json:"processors" yaml:"processors"`
}

//------------------------------------------------------------------------------

// GroupByConfig is a configuration struct containing fields for the GroupBy
// processor, which breaks message batches down into N batches of a smaller size
// according to conditions.
type GroupByConfig []GroupByElement

// NewGroupByConfig returns a GroupByConfig with default values.
func NewGroupByConfig() GroupByConfig {
	return GroupByConfig{}
}

//------------------------------------------------------------------------------

type group struct {
	Condition  condition.Type
	Check      *mapping.Executor
	Processors []types.Processor
}

// GroupBy is a processor that group_bys messages into a message per part.
type GroupBy struct {
	log   log.Modular
	stats metrics.Type

	groups     []group
	mGroupPass []metrics.StatCounter

	mCount        metrics.StatCounter
	mGroupDefault metrics.StatCounter
	mSent         metrics.StatCounter
	mBatchSent    metrics.StatCounter
}

// NewGroupBy returns a GroupBy processor.
func NewGroupBy(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var err error
	groups := make([]group, len(conf.GroupBy))
	groupCtrs := make([]metrics.StatCounter, len(conf.GroupBy))

	for i, gConf := range conf.GroupBy {
		groupPrefix := fmt.Sprintf("groups.%v", i)
		nsLog := log.NewModule("." + groupPrefix)
		nsStats := metrics.Namespaced(stats, groupPrefix)

		if !isDefaultGroupCond(gConf.Condition) {
			if groups[i].Condition, err = condition.New(
				gConf.Condition, mgr,
				nsLog.NewModule(".condition"), metrics.Namespaced(nsStats, "condition"),
			); err != nil {
				return nil, fmt.Errorf("failed to create condition for group '%v': %v", i, err)
			}
		}

		if len(gConf.Check) > 0 {
			if groups[i].Check, err = bloblang.NewMapping("", gConf.Check); err != nil {
				return nil, fmt.Errorf("failed to parse check for group '%v': %v", i, err)
			}
		}

		if groups[i].Check == nil && groups[i].Condition == nil {
			return nil, errors.New("a group definition must have a check query")
		}

		if groups[i].Check != nil && groups[i].Condition != nil {
			return nil, errors.New("cannot specify both a condition and a check in a group")
		}

		for j, pConf := range gConf.Processors {
			prefix := fmt.Sprintf("processor.%v", j)
			var proc Type
			if proc, err = New(
				pConf, mgr,
				nsLog.NewModule("."+prefix), metrics.Namespaced(nsStats, prefix),
			); err != nil {
				return nil, fmt.Errorf("failed to create processor '%v' for group '%v': %v", j, i, err)
			}
			groups[i].Processors = append(groups[i].Processors, proc)
		}

		groupCtrs[i] = stats.GetCounter(groupPrefix + ".passed")
	}

	return &GroupBy{
		log:   log,
		stats: stats,

		groups:     groups,
		mGroupPass: groupCtrs,

		mCount:        stats.GetCounter("count"),
		mGroupDefault: stats.GetCounter("groups.default.passed"),
		mSent:         stats.GetCounter("sent"),
		mBatchSent:    stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (g *GroupBy) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	g.mCount.Incr(1)

	if msg.Len() == 0 {
		return nil, response.NewAck()
	}

	groups := make([]types.Message, len(g.groups))
	for i := range groups {
		groups[i] = message.New(nil)
	}
	groupless := message.New(nil)

	spans := tracing.CreateChildSpans(TypeGroupBy, msg)

	msg.Iter(func(i int, p types.Part) error {
		for j, group := range g.groups {
			if group.Condition != nil {
				if group.Condition.Check(message.Lock(msg, i)) {
					groupStr := strconv.Itoa(j)
					spans[i].LogFields(
						olog.String("event", "grouped"),
						olog.String("type", groupStr),
					)
					spans[i].SetTag("group", groupStr)
					groups[j].Append(p.Copy())
					g.mGroupPass[j].Incr(1)
					return nil
				}
			} else if group.Check != nil {
				res, err := group.Check.QueryPart(i, msg)
				if err != nil {
					res = false
					g.log.Errorf("Failed to test group %v: %v\n", j, err)
				}
				if res {
					groupStr := strconv.Itoa(j)
					spans[i].LogFields(
						olog.String("event", "grouped"),
						olog.String("type", groupStr),
					)
					spans[i].SetTag("group", groupStr)
					groups[j].Append(p.Copy())
					g.mGroupPass[j].Incr(1)
					return nil
				}
			}
		}

		spans[i].LogFields(
			olog.String("event", "grouped"),
			olog.String("type", "default"),
		)
		spans[i].SetTag("group", "default")
		groupless.Append(p.Copy())
		g.mGroupDefault.Incr(1)
		return nil
	})

	for _, s := range spans {
		s.Finish()
	}

	msgs := []types.Message{}
	for i, gmsg := range groups {
		if gmsg.Len() == 0 {
			continue
		}

		resultMsgs, res := ExecuteAll(g.groups[i].Processors, gmsg)
		if len(resultMsgs) > 0 {
			msgs = append(msgs, resultMsgs...)
		}
		if res != nil {
			if err := res.Error(); err != nil {
				g.log.Errorf("Processor error: %v\n", err)
			}
		}
	}

	if groupless.Len() > 0 {
		msgs = append(msgs, groupless)
	}

	if len(msgs) == 0 {
		return nil, response.NewAck()
	}

	g.mBatchSent.Incr(int64(len(msgs)))
	for _, m := range msgs {
		g.mSent.Incr(int64(m.Len()))
	}
	return msgs, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (g *GroupBy) CloseAsync() {
	for _, group := range g.groups {
		for _, p := range group.Processors {
			p.CloseAsync()
		}
	}
}

// WaitForClose blocks until the processor has closed down.
func (g *GroupBy) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, group := range g.groups {
		for _, p := range group.Processors {
			if err := p.WaitForClose(time.Until(stopBy)); err != nil {
				return err
			}
		}
	}
	return nil
}

//------------------------------------------------------------------------------
