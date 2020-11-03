package processor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeProcessField] = TypeSpec{
		constructor: NewProcessField,
		Summary: `
A processor that extracts the value of a field [dot path](/docs/configuration/field_paths)
within payloads according to a specified [codec](#codec), applies a list of
processors to the extracted value and finally sets the field within the original
payloads to the processed result.`,
		Status: docs.StatusDeprecated,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("codec", "A [codec](#codec) to use in order to extract (and set) the target field.").HasOptions("json", "metadata"),
			docs.FieldCommon("path", "A [dot path](/docs/configuration/field_paths) pointing to the target field."),
			docs.FieldCommon(
				"result_type", "The final data type to marshal the processing result into. The `discard` type is a special case that discards the result of the processing steps entirely.",
			).HasOptions("string", "int", "float", "bool", "object", "discard"),
			docs.FieldCommon("processors", "A list of child processors to execute on the extracted value."),
			partsFieldSpec,
		},
		Description: `
The result can be marshalled into a specific data type with the field
[` + "`result_type`" + `](#result_type).

It's therefore possible to use this codec without any child processors as a way
of casting string values into other types. For example, with an input JSON
document ` + "`{\"foo\":\"10\"}`" + ` it's possible to cast the value of the
field foo to an integer type with:

` + "```yaml" + `
process_field:
  path: foo
  result_type: int
` + "```" + `

## Codecs

### ` + "`json`" + `

Parses the payload as a JSON document, extracts and sets the field using a dot
notation path.

### ` + "`metadata`" + `

Extracts and sets a metadata value identified by the path field.`,
		Footnotes: `
## Alternatives

The ` + "[`branch` processor](/docs/components/processors/branch)" + ` offers a
more flexible and robust way to perform the actions of this processor.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			procConfs := make([]interface{}, len(conf.ProcessField.Processors))
			for i, pConf := range conf.ProcessField.Processors {
				if procConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			return map[string]interface{}{
				"parts":       conf.ProcessField.Parts,
				"codec":       conf.ProcessField.Codec,
				"path":        conf.ProcessField.Path,
				"result_type": conf.ProcessField.ResultType,
				"processors":  procConfs,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// ProcessFieldConfig is a config struct containing fields for the ProcessField
// processor.
type ProcessFieldConfig struct {
	Parts      []int    `json:"parts" yaml:"parts"`
	Codec      string   `json:"codec" yaml:"codec"`
	Path       string   `json:"path" yaml:"path"`
	ResultType string   `json:"result_type" yaml:"result_type"`
	Processors []Config `json:"processors" yaml:"processors"`
}

// NewProcessFieldConfig returns a default ProcessFieldConfig.
func NewProcessFieldConfig() ProcessFieldConfig {
	return ProcessFieldConfig{
		Parts:      []int{},
		Codec:      "json",
		Path:       "",
		ResultType: "string",
		Processors: []Config{},
	}
}

//------------------------------------------------------------------------------

type processFieldCodec interface {
	CreateRequest(types.Part) (types.Part, error)
	ExtractResult(from, to types.Part) error
	Discard() bool
}

// ProcessField is a processor that applies a list of child processors to a
// field extracted from the original payload.
type ProcessField struct {
	parts    []int
	path     []string
	children []types.Processor

	codec processFieldCodec

	log log.Modular

	mCount              metrics.StatCounter
	mErr                metrics.StatCounter
	mErrParse           metrics.StatCounter
	mErrMisaligned      metrics.StatCounter
	mErrMisalignedBatch metrics.StatCounter
	mSent               metrics.StatCounter
	mBatchSent          metrics.StatCounter
}

// NewProcessField returns a ProcessField processor.
func NewProcessField(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var children []types.Processor
	for i, pconf := range conf.ProcessField.Processors {
		prefix := fmt.Sprintf("%v", i)
		proc, err := New(pconf, mgr, log.NewModule("."+prefix), metrics.Namespaced(stats, prefix))
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	codec, err := stringToProcessFieldCodec(conf.ProcessField.Path, conf.ProcessField.Codec, conf.ProcessField.ResultType)
	if err != nil {
		return nil, err
	}
	return &ProcessField{
		parts:    conf.ProcessField.Parts,
		path:     strings.Split(conf.ProcessField.Path, "."),
		children: children,
		codec:    codec,

		log: log,

		mCount:              stats.GetCounter("count"),
		mErr:                stats.GetCounter("error"),
		mErrParse:           stats.GetCounter("error.parse"),
		mErrMisaligned:      stats.GetCounter("error.misaligned"),
		mErrMisalignedBatch: stats.GetCounter("error.misaligned_messages"),
		mSent:               stats.GetCounter("sent"),
		mBatchSent:          stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

type processFieldJSONCodec struct {
	path             []string
	resultMarshaller func(p types.Part) (interface{}, error)
}

func newProcessFieldJSONCodec(path, resultStr string) (*processFieldJSONCodec, error) {
	var resultMarshaller func(p types.Part) (interface{}, error)
	switch resultStr {
	case "string":
		resultMarshaller = processFieldJSONResultStringMarshaller
	case "int":
		resultMarshaller = processFieldJSONResultIntMarshaller
	case "float":
		resultMarshaller = processFieldJSONResultFloatMarshaller
	case "bool":
		resultMarshaller = processFieldJSONResultBoolMarshaller
	case "object":
		resultMarshaller = processFieldJSONResultObjectMarshaller
	case "array":
		resultMarshaller = processFieldJSONResultArrayMarshaller
	case "discard":
		resultMarshaller = nil
	default:
		return nil, fmt.Errorf("unrecognised json codec result_type: %v", resultStr)
	}
	return &processFieldJSONCodec{
		path:             strings.Split(path, "."),
		resultMarshaller: resultMarshaller,
	}, nil
}

func (p *processFieldJSONCodec) CreateRequest(source types.Part) (types.Part, error) {
	reqPart := source.Copy()
	jObj, err := reqPart.JSON()
	if err != nil {
		return nil, err
	}
	gObj := gabs.Wrap(jObj)
	gTarget := gObj.S(p.path...)
	switch t := gTarget.Data().(type) {
	case string:
		reqPart.Set([]byte(t))
	default:
		reqPart.SetJSON(gTarget.Data())
	}
	return reqPart, nil
}

func (p *processFieldJSONCodec) ExtractResult(from, to types.Part) error {
	resVal, err := p.resultMarshaller(from)
	if err != nil {
		return err
	}
	jObj, err := to.JSON()
	if err == nil {
		jObj, err = message.CopyJSON(jObj)
	}
	if err != nil {
		return err
	}
	gObj := gabs.Wrap(jObj)
	gObj.Set(resVal, p.path...)
	return to.SetJSON(gObj.Data())
}

func (p *processFieldJSONCodec) Discard() bool {
	return p.resultMarshaller == nil
}

func processFieldJSONResultStringMarshaller(p types.Part) (interface{}, error) {
	return string(p.Get()), nil
}

func processFieldJSONResultIntMarshaller(p types.Part) (interface{}, error) {
	return strconv.Atoi(string(p.Get()))
}

func processFieldJSONResultFloatMarshaller(p types.Part) (interface{}, error) {
	return strconv.ParseFloat(string(p.Get()), 64)
}

func processFieldJSONResultBoolMarshaller(p types.Part) (interface{}, error) {
	str := string(p.Get())
	if str == "true" {
		return true, nil
	}
	if str == "false" {
		return false, nil
	}
	return nil, fmt.Errorf("value '%v' could not be parsed as bool", str)
}

func processFieldJSONResultObjectMarshaller(p types.Part) (interface{}, error) {
	jVal, err := p.JSON()
	if err != nil {
		return nil, err
	}
	// We consider null as an object
	if jVal == nil {
		return nil, nil
	}
	if jObj, ok := jVal.(map[string]interface{}); ok {
		return jObj, nil
	}
	return nil, fmt.Errorf("failed to parse JSON type '%T' into object", jVal)
}

func processFieldJSONResultArrayMarshaller(p types.Part) (interface{}, error) {
	jVal, err := p.JSON()
	if err != nil {
		return nil, err
	}
	if jArray, ok := jVal.([]interface{}); ok {
		return jArray, nil
	}
	return nil, fmt.Errorf("failed to parse JSON type '%T' into array", jVal)
}

//------------------------------------------------------------------------------

type processFieldMetadataCodec struct {
	key     string
	discard bool
}

func newProcessFieldMetadataCodec(path, resultStr string) (*processFieldMetadataCodec, error) {
	return &processFieldMetadataCodec{
		key:     path,
		discard: resultStr == "discard",
	}, nil
}

func (p *processFieldMetadataCodec) CreateRequest(source types.Part) (types.Part, error) {
	reqPart := source.Copy()
	reqPart.Set([]byte(reqPart.Metadata().Get(p.key)))
	return reqPart, nil
}

func (p *processFieldMetadataCodec) ExtractResult(from, to types.Part) error {
	to.Metadata().Set(p.key, string(from.Get()))
	return nil
}

func (p *processFieldMetadataCodec) Discard() bool {
	return p.discard
}

//------------------------------------------------------------------------------

func stringToProcessFieldCodec(path, codecStr, resultStr string) (processFieldCodec, error) {
	switch codecStr {
	case "json":
		return newProcessFieldJSONCodec(path, resultStr)
	case "metadata":
		return newProcessFieldMetadataCodec(path, resultStr)
	}
	return nil, fmt.Errorf("unrecognised codec: %v", codecStr)
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *ProcessField) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	p.mCount.Incr(1)
	payload := msg.Copy()
	resMsgs := [1]types.Message{payload}
	msgs = resMsgs[:]

	targetParts := p.parts
	if len(targetParts) == 0 {
		targetParts = make([]int, payload.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

	reqMsg := message.New(nil)
	for _, index := range targetParts {
		reqPart, err := p.codec.CreateRequest(payload.Get(index))
		if err != nil {
			p.mErrParse.Incr(1)
			p.mErr.Incr(1)
			p.log.Errorf("Failed to decode part: %v\n", err)
			reqPart = payload.Get(index).Copy()
			reqPart.Set(nil)
			FlagErr(reqPart, err)
		}
		reqMsg.Append(reqPart)
	}

	propMsg, _ := tracing.WithChildSpans(TypeProcessField, reqMsg)
	resultMsgs, _ := ExecuteAll(p.children, propMsg)
	resMsg := message.New(nil)
	for _, rMsg := range resultMsgs {
		rMsg.Iter(func(i int, p types.Part) error {
			resMsg.Append(p.Copy())
			return nil
		})
	}
	defer tracing.FinishSpans(propMsg)

	if p.codec.Discard() {
		// With no result codec, if our results are inline with our original
		// batch we copy the metadata only.
		if len(targetParts) == resMsg.Len() {
			for i, index := range targetParts {
				tPart := payload.Get(index)
				tPartMeta := tPart.Metadata()
				resMsg.Get(i).Metadata().Iter(func(k, v string) error {
					tPartMeta.Set(k, v)
					return nil
				})
			}
		}
		p.mBatchSent.Incr(1)
		p.mSent.Incr(int64(payload.Len()))
		return
	}

	if exp, act := len(targetParts), resMsg.Len(); exp != act {
		p.mBatchSent.Incr(1)
		p.mSent.Incr(int64(payload.Len()))
		p.mErr.Incr(1)
		p.mErrMisalignedBatch.Incr(1)
		p.log.Errorf("Misaligned processor result batch. Expected %v messages, received %v\n", exp, act)
		partsErr := fmt.Errorf("mismatched processor result, expected %v, received %v messages", exp, act)
		payload.Iter(func(i int, p types.Part) error {
			FlagErr(p, partsErr)
			return nil
		})
		return
	}

	for i, index := range targetParts {
		tPart := payload.Get(index)
		tPartMeta := tPart.Metadata()
		resMsg.Get(i).Metadata().Iter(func(k, v string) error {
			tPartMeta.Set(k, v)
			return nil
		})
		rErr := p.codec.ExtractResult(resMsg.Get(i), tPart)
		if rErr != nil {
			p.log.Errorf("Failed to marshal result: %v\n", rErr)
			FlagErr(tPart, rErr)
			continue
		}
	}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(payload.Len()))
	return
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *ProcessField) CloseAsync() {
	for _, c := range p.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (p *ProcessField) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
