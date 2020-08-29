package query

import (
	"fmt"
)

var _ Function = &mapLiteral{}

type mapLiteral struct {
	keyValues [][2]interface{}
}

// NewMapLiteral creates a map literal from a slice of key/value pairs. If all
// keys and values are static then a static map[string]interface{} value is
// returned. However, if any keys or values are dynamic a Function is returned.
func NewMapLiteral(values [][2]interface{}) (interface{}, error) {
	isDynamic := false
	staticValues := make(map[string]interface{}, len(values))
	for _, kv := range values {
		var key string
		var isStr bool
		switch t := kv[0].(type) {
		case string:
			key = t
			isStr = true
		case *Literal:
			if key, isStr = t.Value.(string); !isStr {
				return nil, fmt.Errorf("object keys must be strings, received: %T", t.Value)
			}
		}
		_, isFn := kv[1].(Function)
		if isStr && !isFn {
			staticValues[key] = kv[1]
		} else {
			isDynamic = true
			break
		}
	}
	if isDynamic {
		return &mapLiteral{keyValues: values}, nil
	}
	return staticValues, nil
}

func (m *mapLiteral) Exec(ctx FunctionContext) (interface{}, error) {
	dynMap := make(map[string]interface{}, len(m.keyValues))
	for _, kv := range m.keyValues {
		var key string
		var value interface{}

		var err error
		switch t := kv[0].(type) {
		case string:
			key = t
		case Function:
			var keyI interface{}
			if keyI, err = t.Exec(ctx); err != nil {
				return nil, fmt.Errorf("failed to resolve key: %w", err)
			}
			switch t2 := keyI.(type) {
			case string:
				key = t2
			case []byte:
				key = string(t2)
			default:
				return nil, fmt.Errorf("mapping returned invalid key type: %T", keyI)
			}
		default:
			return nil, fmt.Errorf("invalid key type: %T", kv[0])
		}

		if fn, isFunction := kv[1].(Function); isFunction {
			if value, err = fn.Exec(ctx); err != nil {
				return nil, fmt.Errorf("failed to resolve '%v' value: %w", key, err)
			}
		} else {
			value = kv[1]
		}

		dynMap[key] = value
	}
	return dynMap, nil
}

func (m *mapLiteral) QueryTargets() []TargetPath {
	var targetPaths []TargetPath
	for _, kv := range m.keyValues {
		if fn, ok := kv[0].(Function); ok {
			targetPaths = append(targetPaths, fn.QueryTargets()...)
		}
		if fn, ok := kv[1].(Function); ok {
			targetPaths = append(targetPaths, fn.QueryTargets()...)
		}
	}
	return targetPaths
}

//------------------------------------------------------------------------------

var _ Function = &arrayLiteral{}

type arrayLiteral struct {
	values []interface{}
}

// NewArrayLiteral creates an array literal from a slice of values. If all
// values are static then a static []interface{} value is returned. However, if
// any values are dynamic a Function is returned.
func NewArrayLiteral(values ...interface{}) interface{} {
	isDynamic := false
	for _, v := range values {
		if _, isFunction := v.(Function); isFunction {
			isDynamic = true
		}
	}
	if !isDynamic {
		return values
	}

	return &arrayLiteral{values}
}

func (a *arrayLiteral) Exec(ctx FunctionContext) (interface{}, error) {
	dynArray := make([]interface{}, len(a.values))
	var err error
	for i, v := range a.values {
		if fn, isFunction := v.(Function); isFunction {
			fnRes, fnErr := fn.Exec(ctx)
			if fnErr != nil {
				if recovered, ok := fnErr.(*ErrRecoverable); ok {
					dynArray[i] = recovered.Recovered
					err = fnErr
				}
				return nil, fnErr
			}
			dynArray[i] = fnRes
		} else {
			dynArray[i] = v
		}
	}
	if err != nil {
		return nil, &ErrRecoverable{
			Recovered: dynArray,
			Err:       err,
		}
	}
	return dynArray, nil
}

func (a *arrayLiteral) QueryTargets() []TargetPath {
	var targetPaths []TargetPath
	for _, v := range a.values {
		if fn, ok := v.(Function); ok {
			targetPaths = append(targetPaths, fn.QueryTargets()...)
		}
	}
	return targetPaths
}
