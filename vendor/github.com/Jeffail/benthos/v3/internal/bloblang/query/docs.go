package query

// ExampleSpec provides a mapping example and some input/output results to
// display.
type ExampleSpec struct {
	Mapping string
	Summary string
	Results [][2]string
}

// NewExampleSpec creates a new example spec.
func NewExampleSpec(summary, mapping string, results ...string) ExampleSpec {
	structuredResults := make([][2]string, 0, len(results)/2)
	for i, res := range results {
		if i%2 == 1 {
			structuredResults = append(structuredResults, [2]string{results[i-1], res})
		}
	}
	return ExampleSpec{
		Mapping: mapping,
		Summary: summary,
		Results: structuredResults,
	}
}

//------------------------------------------------------------------------------

// Status of a function or method.
type Status string

// Component statuses.
var (
	StatusStable     Status = "stable"
	StatusBeta       Status = "beta"
	StatusDeprecated Status = "deprecated"
)

//------------------------------------------------------------------------------

// FunctionCategory is an abstract title for functions of a similar purpose.
type FunctionCategory string

// Function categories.
var (
	FunctionCategoryGeneral     FunctionCategory = "General"
	FunctionCategoryMessage     FunctionCategory = "Message Info"
	FunctionCategoryEnvironment FunctionCategory = "Environment"
)

// FunctionSpec describes a Bloblang function.
type FunctionSpec struct {
	// The release status of the function.
	Status Status

	// A category to place the function within.
	Category FunctionCategory

	// Name of the function (as it appears in config).
	Name string

	// Description of the functions purpose (in markdown).
	Description string

	// Examples shows general usage for the function.
	Examples []ExampleSpec
}

// NewFunctionSpec creates a new function spec.
func NewFunctionSpec(category FunctionCategory, name, description string, examples ...ExampleSpec) FunctionSpec {
	return FunctionSpec{
		Status:      StatusStable,
		Category:    category,
		Name:        name,
		Description: description,
		Examples:    examples,
	}
}

// Beta flags the function as a beta component.
func (s FunctionSpec) Beta() FunctionSpec {
	s.Status = StatusBeta
	return s
}

// NewDeprecatedFunctionSpec creates a new function spec that is deprecated. The
// function will not appear in docs or searches but will still be usable in
// mappings.
func NewDeprecatedFunctionSpec(name string) FunctionSpec {
	return FunctionSpec{
		Name:   name,
		Status: StatusDeprecated,
	}
}

//------------------------------------------------------------------------------

// MethodCategory is an abstract title for methods of a similar purpose.
type MethodCategory string

// Method categories.
var (
	MethodCategoryStrings        MethodCategory = "String Manipulation"
	MethodCategoryTime           MethodCategory = "Timestamp Manipulation"
	MethodCategoryRegexp         MethodCategory = "Regular Expressions"
	MethodCategoryEncoding       MethodCategory = "Encoding and Encryption"
	MethodCategoryCoercion       MethodCategory = "Type Coercion"
	MethodCategoryParsing        MethodCategory = "Parsing"
	MethodCategoryObjectAndArray MethodCategory = "Object & Array Manipulation"
)

// MethodCatSpec describes how a method behaves in the context of a given
// category.
type MethodCatSpec struct {
	Category    MethodCategory
	Description string
	Examples    []ExampleSpec
}

// MethodSpec describes a Bloblang method.
type MethodSpec struct {
	// The release status of the function.
	Status Status

	// Name of the method (as it appears in config).
	Name string

	// Description of the method purpose (in markdown).
	Description string

	// Examples shows general usage for the method.
	Examples []ExampleSpec

	// Categories that this method fits within.
	Categories []MethodCatSpec
}

// NewMethodSpec creates a new method spec.
func NewMethodSpec(name, description string, examples ...ExampleSpec) MethodSpec {
	return MethodSpec{
		Name:        name,
		Status:      StatusStable,
		Description: description,
		Examples:    examples,
	}
}

// NewDeprecatedMethodSpec creates a new method spec that is deprecated. The
// method will not appear in docs or searches but will still be usable in
// mappings.
func NewDeprecatedMethodSpec(name string) MethodSpec {
	return MethodSpec{
		Name:   name,
		Status: StatusDeprecated,
	}
}

// Beta flags the function as a beta component.
func (m MethodSpec) Beta() MethodSpec {
	m.Status = StatusBeta
	return m
}

// InCategory describes the methods behaviour in the context of a given
// category, methods can belong to multiple categories. For example, the
// `contains` method behaves differently in the object and array category versus
// the strings one, but belongs in both.
func (m MethodSpec) InCategory(category MethodCategory, description string, examples ...ExampleSpec) MethodSpec {
	cats := make([]MethodCatSpec, 0, len(m.Categories)+1)
	cats = append(cats, m.Categories...)
	cats = append(cats, MethodCatSpec{
		Category:    category,
		Description: description,
		Examples:    examples,
	})
	m.Categories = cats
	return m
}
