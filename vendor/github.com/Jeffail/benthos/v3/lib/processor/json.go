package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeJSON] = TypeSpec{
		constructor: NewJSON,
		Deprecated:  true,
		Footnotes: `
## Alternatives

All functionality of this processor has been superseded by the
[bloblang](/docs/components/processors/bloblang) processor.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("operator", "The [operator](#operators) to apply to messages.").HasOptions(
				"append", "clean", "copy", "delete", "explode", "flatten", "flatten_array", "fold_number_array",
				"fold_string_array", "move", "select", "set", "split",
			),
			docs.FieldCommon("path", "A [dot path](/docs/configuration/field_paths) specifying the target within the document to the apply the chosen operator to.", "foo.bar", ".", "some_array.0.id"),
			docs.FieldCommon(
				"value",
				"A value to use with the chosen operator (sometimes not applicable). This is a generic field that can be any type.",
				"foo", "${!metadata:kafka_key}", false, 10,
				map[string]interface{}{"topic": "${!metadata:kafka_topic}", "key": "${!metadata:kafka_key}"},
			),
			partsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

type rawJSONValue []byte

func (r *rawJSONValue) UnmarshalJSON(bytes []byte) error {
	*r = append((*r)[0:0], bytes...)
	return nil
}

func (r rawJSONValue) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	return r, nil
}

func (r *rawJSONValue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var yamlObj interface{}
	if err := unmarshal(&yamlObj); err != nil {
		return err
	}

	var convertMap func(m map[interface{}]interface{}) map[string]interface{}
	var convertArray func(a []interface{})
	convertMap = func(m map[interface{}]interface{}) map[string]interface{} {
		newMap := map[string]interface{}{}
		for k, v := range m {
			keyStr, ok := k.(string)
			if !ok {
				continue
			}
			newVal := v
			switch t := v.(type) {
			case []interface{}:
				convertArray(t)
			case map[interface{}]interface{}:
				newVal = convertMap(t)
			}
			newMap[keyStr] = newVal
		}
		return newMap
	}
	convertArray = func(a []interface{}) {
		for i, v := range a {
			newVal := v
			switch t := v.(type) {
			case []interface{}:
				convertArray(t)
			case map[interface{}]interface{}:
				newVal = convertMap(t)
			}
			a[i] = newVal
		}
	}
	switch t := yamlObj.(type) {
	case []interface{}:
		convertArray(t)
	case map[interface{}]interface{}:
		yamlObj = convertMap(t)
	}

	rawJSON, err := json.Marshal(yamlObj)
	if err != nil {
		return err
	}

	*r = append((*r)[0:0], rawJSON...)
	return nil
}

func (r rawJSONValue) MarshalYAML() (interface{}, error) {
	if r == nil {
		return nil, nil
	}
	var val interface{}
	if err := json.Unmarshal(r, &val); err != nil {
		return nil, err
	}
	return val, nil
}

//------------------------------------------------------------------------------

// JSONConfig contains configuration fields for the JSON processor.
type JSONConfig struct {
	Parts    []int        `json:"parts" yaml:"parts"`
	Operator string       `json:"operator" yaml:"operator"`
	Path     string       `json:"path" yaml:"path"`
	Value    rawJSONValue `json:"value" yaml:"value"`
}

// NewJSONConfig returns a JSONConfig with default values.
func NewJSONConfig() JSONConfig {
	return JSONConfig{
		Parts:    []int{},
		Operator: "clean",
		Path:     "",
		Value:    rawJSONValue(`""`),
	}
}

//------------------------------------------------------------------------------

type jsonOperator func(body interface{}, value json.RawMessage) (interface{}, error)

func newSetOperator(path []string) jsonOperator {
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		if len(path) == 0 {
			var data interface{}
			if value != nil {
				if err := json.Unmarshal([]byte(value), &data); err != nil {
					return nil, fmt.Errorf("failed to parse value: %v", err)
				}
			}
			return data, nil
		}

		gPart := gabs.Wrap(body)

		var data interface{}
		if value != nil {
			if err := json.Unmarshal([]byte(value), &data); err != nil {
				return nil, fmt.Errorf("failed to parse value: %v", err)
			}
		}

		gPart.Set(data, path...)
		return gPart.Data(), nil
	}
}

func newMoveOperator(srcPath, destPath []string) (jsonOperator, error) {
	if len(srcPath) == 0 && len(destPath) == 0 {
		return nil, errors.New("an empty source and destination path is not valid for the move operator")
	}
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		var gPart *gabs.Container
		var gSrc interface{}
		if len(srcPath) > 0 {
			gPart = gabs.Wrap(body)
			gSrc = gPart.S(srcPath...).Data()
			gPart.Delete(srcPath...)
		} else {
			gPart = gabs.New()
			gSrc = body
		}
		if gSrc == nil {
			return nil, fmt.Errorf("item not found at path '%v'", strings.Join(srcPath, "."))
		}
		if len(destPath) == 0 {
			return gSrc, nil
		}
		if _, err := gPart.Set(gSrc, destPath...); err != nil {
			return nil, fmt.Errorf("failed to set destination path '%v': %v", strings.Join(destPath, "."), err)
		}
		return gPart.Data(), nil
	}, nil
}

func newCopyOperator(srcPath, destPath []string) (jsonOperator, error) {
	if len(srcPath) == 0 {
		return nil, errors.New("an empty source path is not valid for the copy operator")
	}
	if len(destPath) == 0 {
		return nil, errors.New("an empty destination path is not valid for the copy operator")
	}
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		gPart := gabs.Wrap(body)
		gSrc := gPart.S(srcPath...).Data()
		if gSrc == nil {
			return nil, fmt.Errorf("item not found at path '%v'", strings.Join(srcPath, "."))
		}

		if _, err := gPart.Set(gSrc, destPath...); err != nil {
			return nil, fmt.Errorf("failed to set destination path '%v': %v", strings.Join(destPath, "."), err)
		}
		return gPart.Data(), nil
	}, nil
}

func newExplodeOperator(path []string) (jsonOperator, error) {
	if len(path) == 0 {
		return nil, errors.New("explode operator requires a target path")
	}
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		target := gabs.Wrap(body).Search(path...)

		switch t := target.Data().(type) {
		case []interface{}:
			result := make([]interface{}, len(t))
			for i, ele := range t {
				exploded, err := message.CopyJSON(body)
				if err != nil {
					return nil, fmt.Errorf("failed to clone root object to explode: %v", err)
				}

				gExploded := gabs.Wrap(exploded)
				gExploded.Set(ele, path...)
				result[i] = gExploded.Data()
			}
			return result, nil
		case map[string]interface{}:
			result := make(map[string]interface{})
			for key, ele := range t {
				exploded, err := message.CopyJSON(body)
				if err != nil {
					return nil, fmt.Errorf("failed to clone root object to explode: %v", err)
				}

				gExploded := gabs.Wrap(exploded)
				gExploded.Set(ele, path...)
				result[key] = gExploded.Data()
			}
			return result, nil
		}

		return nil, fmt.Errorf("target value was not an array or a map, found: %T", target.Data())
	}, nil
}

func foldStringArray(children []*gabs.Container, value json.RawMessage) (string, error) {
	var delim string
	if value != nil {
		json.Unmarshal(value, &delim)
	}
	var b strings.Builder
	for i, child := range children {
		switch t := child.Data().(type) {
		case string:
			if i > 0 && len(delim) > 0 {
				b.WriteString(delim)
			}
			b.WriteString(t)
		default:
			return "", fmt.Errorf("mismatched types found in array, expected string, found: %T", t)
		}
	}
	return b.String(), nil
}

func foldArrayArray(children []*gabs.Container) ([]interface{}, error) {
	var b []interface{}
	for _, child := range children {
		switch t := child.Data().(type) {
		case []interface{}:
			b = append(b, t...)
		default:
			b = append(b, t)
		}
	}
	return b, nil
}

func foldNumberArray(children []*gabs.Container) (float64, error) {
	var b float64
	for _, child := range children {
		switch t := child.Data().(type) {
		case int:
			b = b + float64(t)
		case int64:
			b = b + float64(t)
		case float64:
			b = b + float64(t)
		case json.Number:
			f, err := t.Float64()
			if err != nil {
				i, _ := t.Int64()
				f = float64(i)
			}
			b = b + f
		default:
			return 0, fmt.Errorf("mismatched types found in array, expected number, found: %T", t)
		}
	}
	return b, nil
}

func newFlattenOperator(path []string) jsonOperator {
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		gPart := gabs.Wrap(body)
		target := gPart
		if len(path) > 0 {
			target = gPart.Search(path...)
		}

		v, err := target.Flatten()
		if err != nil {
			return nil, err
		}

		gPart.Set(v, path...)
		return gPart.Data(), nil
	}
}

func newFlattenArrayOperator(path []string) jsonOperator {
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		gPart := gabs.Wrap(body)
		target := gPart
		if len(path) > 0 {
			target = gPart.Search(path...)
		}

		if _, isArray := target.Data().([]interface{}); !isArray {
			return nil, fmt.Errorf("non-array value found at path: %T", target.Data())
		}

		children := target.Children()
		if len(children) == 0 {
			return body, nil
		}

		v, err := foldArrayArray(children)
		if err != nil {
			return nil, err
		}

		gPart.Set(v, path...)
		return gPart.Data(), nil
	}
}

func newFoldNumberArrayOperator(path []string) jsonOperator {
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		gPart := gabs.Wrap(body)
		target := gPart
		if len(path) > 0 {
			target = gPart.Search(path...)
		}

		if _, isArray := target.Data().([]interface{}); !isArray {
			return nil, fmt.Errorf("non-array value found at path: %T", target.Data())
		}

		var v float64
		var err error

		children := target.Children()
		if len(children) > 0 {
			v, err = foldNumberArray(children)
		}
		if err != nil {
			return nil, err
		}

		gPart.Set(v, path...)
		return gPart.Data(), nil
	}
}

func newFoldStringArrayOperator(path []string) jsonOperator {
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		gPart := gabs.Wrap(body)
		target := gPart
		if len(path) > 0 {
			target = gPart.Search(path...)
		}

		if _, isArray := target.Data().([]interface{}); !isArray {
			return nil, fmt.Errorf("non-array value found at path: %T", target.Data())
		}

		var v string
		var err error

		children := target.Children()
		if len(children) > 0 {
			v, err = foldStringArray(children, value)
		}
		if err != nil {
			return nil, err
		}

		gPart.Set(v, path...)
		return gPart.Data(), nil
	}
}

func newSelectOperator(path []string) jsonOperator {
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		gPart := gabs.Wrap(body)
		target := gPart
		if len(path) > 0 {
			target = gPart.Search(path...)
		}

		switch t := target.Data().(type) {
		case string:
			return rawJSONValue(t), nil
		case json.Number:
			return rawJSONValue(t.String()), nil
		}

		return target.Data(), nil
	}
}

func newDeleteOperator(path []string) jsonOperator {
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		if len(path) == 0 {
			return nil, nil
		}

		gPart := gabs.Wrap(body)
		if err := gPart.Delete(path...); err != nil {
			return nil, err
		}
		return gPart.Data(), nil
	}
}

func newCleanOperator(path []string) jsonOperator {
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		gRoot := gabs.Wrap(body)

		var cleanValueFn func(g interface{}) interface{}
		var cleanArrayFn func(g []interface{}) []interface{}
		var cleanObjectFn func(g map[string]interface{}) map[string]interface{}
		cleanValueFn = func(g interface{}) interface{} {
			if g == nil {
				return nil
			}
			switch t := g.(type) {
			case map[string]interface{}:
				if nv := cleanObjectFn(t); len(nv) > 0 {
					return nv
				}
				return nil
			case []interface{}:
				if na := cleanArrayFn(t); len(na) > 0 {
					return na
				}
				return nil
			case string:
				if len(t) > 0 {
					return t
				}
				return nil
			}
			return g
		}
		cleanArrayFn = func(g []interface{}) []interface{} {
			newArray := []interface{}{}
			for _, v := range g {
				if nv := cleanValueFn(v); nv != nil {
					newArray = append(newArray, nv)
				}
			}
			return newArray
		}
		cleanObjectFn = func(g map[string]interface{}) map[string]interface{} {
			newObject := map[string]interface{}{}
			for k, v := range g {
				if nv := cleanValueFn(v); nv != nil {
					newObject[k] = nv
				}
			}
			return newObject
		}
		if val := cleanValueFn(gRoot.S(path...).Data()); val == nil {
			if len(path) == 0 {
				switch gRoot.Data().(type) {
				case []interface{}:
					return []interface{}{}, nil
				case map[string]interface{}:
					return map[string]interface{}{}, nil
				}
				return nil, nil
			}
			gRoot.Delete(path...)
		} else {
			gRoot.Set(val, path...)
		}

		return gRoot.Data(), nil
	}
}

func newAppendOperator(path []string) jsonOperator {
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		gPart := gabs.Wrap(body)
		var array []interface{}

		var valueParsed interface{}
		if value != nil {
			if err := json.Unmarshal(value, &valueParsed); err != nil {
				return nil, err
			}
		}
		switch t := valueParsed.(type) {
		case []interface{}:
			array = t
		default:
			array = append(array, t)
		}

		if gTarget := gPart.S(path...); gTarget != nil {
			switch t := gTarget.Data().(type) {
			case []interface{}:
				t = append(t, array...)
				array = t
			case nil:
				array = append([]interface{}{t}, array...)
			default:
				array = append([]interface{}{t}, array...)
			}
		}
		gPart.Set(array, path...)

		return gPart.Data(), nil
	}
}

func newSplitOperator(path []string) jsonOperator {
	return func(body interface{}, value json.RawMessage) (interface{}, error) {
		gPart := gabs.Wrap(body)

		var valueParsed string
		if value != nil {
			if err := json.Unmarshal(value, &valueParsed); err != nil {
				return nil, err
			}
		}
		if len(valueParsed) == 0 {
			return nil, errors.New("value field must be a non-empty string")
		}

		targetStr, ok := gPart.S(path...).Data().(string)
		if !ok {
			return nil, errors.New("path value must be a string")
		}

		var values []interface{}
		for _, v := range strings.Split(targetStr, valueParsed) {
			values = append(values, v)
		}

		gPart.Set(values, path...)
		return gPart.Data(), nil
	}
}

func getOperator(opStr string, path []string, value json.RawMessage) (jsonOperator, error) {
	var destPath []string
	if opStr == "move" || opStr == "copy" {
		var destDotPath string
		if err := json.Unmarshal(value, &destDotPath); err != nil {
			return nil, fmt.Errorf("failed to parse destination path from value: %v", err)
		}
		if len(destDotPath) > 0 {
			destPath = gabs.DotPathToSlice(destDotPath)
		}
	}
	switch opStr {
	case "set":
		return newSetOperator(path), nil
	case "flatten":
		return newFlattenOperator(path), nil
	case "flatten_array":
		return newFlattenArrayOperator(path), nil
	case "fold_number_array":
		return newFoldNumberArrayOperator(path), nil
	case "fold_string_array":
		return newFoldStringArrayOperator(path), nil
	case "select":
		return newSelectOperator(path), nil
	case "split":
		return newSplitOperator(path), nil
	case "copy":
		return newCopyOperator(path, destPath)
	case "move":
		return newMoveOperator(path, destPath)
	case "delete":
		return newDeleteOperator(path), nil
	case "append":
		return newAppendOperator(path), nil
	case "clean":
		return newCleanOperator(path), nil
	case "explode":
		return newExplodeOperator(path)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

// JSON is a processor that performs an operation on a JSON payload.
type JSON struct {
	parts []int

	value    field.Expression
	operator jsonOperator

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrJSONP  metrics.StatCounter
	mErrJSONS  metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewJSON returns a JSON processor.
func NewJSON(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	value, err := bloblang.NewField(string(conf.JSON.Value))
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}

	j := &JSON{
		parts: conf.JSON.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		value: value,

		mCount:     stats.GetCounter("count"),
		mErrJSONP:  stats.GetCounter("error.json_parse"),
		mErrJSONS:  stats.GetCounter("error.json_set"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	splitPath := gabs.DotPathToSlice(conf.JSON.Path)
	if len(conf.JSON.Path) == 0 || conf.JSON.Path == "." {
		splitPath = []string{}
	}

	if j.operator, err = getOperator(conf.JSON.Operator, splitPath, json.RawMessage(j.value.Bytes(0, message.New(nil)))); err != nil {
		return nil, err
	}
	return j, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *JSON) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		valueBytes := p.value.BytesEscapedLegacy(index, newMsg)
		jsonPart, err := part.JSON()
		if err == nil {
			jsonPart, err = message.CopyJSON(jsonPart)
		}
		if err != nil {
			p.mErrJSONP.Incr(1)
			p.mErr.Incr(1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			return err
		}

		var data interface{}
		if data, err = p.operator(jsonPart, json.RawMessage(valueBytes)); err != nil {
			p.mErr.Incr(1)
			p.log.Debugf("Failed to apply operator: %v\n", err)
			return err
		}

		switch t := data.(type) {
		case rawJSONValue:
			newMsg.Get(index).Set([]byte(t))
		case []byte:
			newMsg.Get(index).Set(t)
		default:
			if err = newMsg.Get(index).SetJSON(data); err != nil {
				p.mErrJSONS.Incr(1)
				p.mErr.Incr(1)
				p.log.Debugf("Failed to convert json into part: %v\n", err)
				return err
			}
		}
		return nil
	}

	IteratePartsWithSpan(TypeJSON, p.parts, newMsg, proc)

	msgs := [1]types.Message{newMsg}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *JSON) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *JSON) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
