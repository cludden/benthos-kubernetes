package processor

import (
	"encoding/xml"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/clbanning/mxj"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	dec := xml.NewDecoder(nil)
	dec.Strict = false
	mxj.CustomDecoder = dec

	Constructors[TypeXML] = TypeSpec{
		constructor: NewXML,
		Beta:        true,
		Summary: `
Parses messages as an XML document, performs a mutation on the data, and then
overwrites the previous contents with the new value.`,
		Description: `
## Operators

### ` + "`to_json`" + `

Converts an XML document into a JSON structure, where elements appear as keys of
an object according to the following rules:

- If an element contains attributes they are parsed by prefixing a hyphen,
  ` + "`-`" + `, to the attribute label.
- If the element is a simple element and has attributes, the element value
  is given the key ` + "`#text`" + `.
- XML comments, directives, and process instructions are ignored.
- When elements are repeated the resulting JSON value is an array.

For example, given the following XML:

` + "```xml" + `
<root>
  <title>This is a title</title>
  <description tone="boring">This is a description</description>
  <elements id="1">foo1</elements>
  <elements id="2">foo2</elements>
  <elements>foo3</elements>
</root>
` + "```" + `

The resulting JSON structure would look like this:

` + "```json" + `
{
  "root":{
    "title":"This is a title",
    "description":{
      "#text":"This is a description",
      "-tone":"boring"
    },
    "elements":[
      {"#text":"foo1","-id":"1"},
      {"#text":"foo2","-id":"2"},
      "foo3"
    ]
  }
}
` + "```" + ``,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("operator", "An XML [operation](#operators) to apply to messages.").HasOptions("to_json"),
			partsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// XMLConfig contains configuration fields for the XML processor.
type XMLConfig struct {
	Parts    []int  `json:"parts" yaml:"parts"`
	Operator string `json:"operator" yaml:"operator"`
}

// NewXMLConfig returns a XMLConfig with default values.
func NewXMLConfig() XMLConfig {
	return XMLConfig{
		Parts:    []int{},
		Operator: "to_json",
	}
}

//------------------------------------------------------------------------------

// XML is a processor that performs an operation on a XML payload.
type XML struct {
	parts []int

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewXML returns a XML processor.
func NewXML(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	if conf.XML.Operator != "to_json" {
		return nil, fmt.Errorf("operator not recognised: %v", conf.XML.Operator)
	}

	j := &XML{
		parts: conf.XML.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	return j, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *XML) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		root, err := mxj.NewMapXml(part.Get())
		if err != nil {
			p.mErr.Incr(1)
			p.log.Debugf("Failed to parse part as XML: %v\n", err)
			return err
		}
		if err = part.SetJSON(map[string]interface{}(root)); err != nil {
			p.mErr.Incr(1)
			p.log.Debugf("Failed to marshal XML as JSON: %v\n", err)
			return err
		}
		return nil
	}

	IteratePartsWithSpan(TypeXML, p.parts, newMsg, proc)

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *XML) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *XML) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
