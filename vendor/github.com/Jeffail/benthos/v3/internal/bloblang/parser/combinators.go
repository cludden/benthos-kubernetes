package parser

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

//------------------------------------------------------------------------------

// Result represents the result of a parser given an input.
type Result struct {
	Payload   interface{}
	Err       *Error
	Remaining []rune
}

// Type is a general parser method.
type Type func([]rune) Result

//------------------------------------------------------------------------------

// NotEnd parses zero characters from an input and expects it to not have ended.
// An ExpectedError must be provided which provides the error returned on empty
// input.
func NotEnd(p Type, exp ...string) Type {
	return func(input []rune) Result {
		if len(input) == 0 {
			return Result{
				Payload:   nil,
				Err:       NewError(input, exp...),
				Remaining: input,
			}
		}
		return p(input)
	}
}

// Char parses a single character and expects it to match one candidate.
func Char(c rune) Type {
	exp := string(c)
	return NotEnd(func(input []rune) Result {
		if input[0] != c {
			return Result{
				Payload:   nil,
				Err:       NewError(input, exp),
				Remaining: input,
			}
		}
		return Result{
			Payload:   string(c),
			Err:       nil,
			Remaining: input[1:],
		}
	}, exp)
}

// NotChar parses any number of characters until they match a single candidate.
func NotChar(c rune) Type {
	exp := "not " + string(c)
	return NotEnd(func(input []rune) Result {
		if input[0] == c {
			return Result{
				Payload:   nil,
				Err:       NewError(input, exp),
				Remaining: input,
			}
		}
		i := 0
		for ; i < len(input); i++ {
			if input[i] == c {
				return Result{
					Payload:   string(input[:i]),
					Err:       nil,
					Remaining: input[i:],
				}
			}
		}
		return Result{
			Payload:   string(input),
			Err:       nil,
			Remaining: nil,
		}
	}, exp)
}

// InSet parses any number of characters within a set of runes.
func InSet(set ...rune) Type {
	setMap := make(map[rune]struct{}, len(set))
	for _, r := range set {
		setMap[r] = struct{}{}
	}
	exp := fmt.Sprintf("chars(%v)", string(set))
	return NotEnd(func(input []rune) Result {
		i := 0
		for ; i < len(input); i++ {
			if _, exists := setMap[input[i]]; !exists {
				if i == 0 {
					return Result{
						Err:       NewError(input, exp),
						Remaining: input,
					}
				}
				break
			}
		}

		return Result{
			Payload:   string(input[:i]),
			Err:       nil,
			Remaining: input[i:],
		}
	}, exp)
}

// InRange parses any number of characters between two runes inclusive.
func InRange(lower, upper rune) Type {
	exp := fmt.Sprintf("range(%c - %c)", lower, upper)
	return NotEnd(func(input []rune) Result {
		i := 0
		for ; i < len(input); i++ {
			if input[i] < lower || input[i] > upper {
				if i == 0 {
					return Result{
						Err:       NewError(input, exp),
						Remaining: input,
					}
				}
				break
			}
		}

		return Result{
			Payload:   string(input[:i]),
			Err:       nil,
			Remaining: input[i:],
		}
	}, exp)
}

// SpacesAndTabs parses any number of space or tab characters.
func SpacesAndTabs() Type {
	return Expect(InSet(' ', '\t'), "whitespace")
}

// Term parses a single instance of a string.
func Term(str string) Type {
	exp := str
	return NotEnd(func(input []rune) Result {
		for i, c := range str {
			if len(input) <= i || input[i] != c {
				return Result{
					Payload:   nil,
					Err:       NewError(input, exp),
					Remaining: input,
				}
			}
		}
		return Result{
			Payload:   str,
			Err:       nil,
			Remaining: input[len(str):],
		}
	}, exp)
}

// Number parses any number of numerical characters into either an int64 or, if
// the number contains float characters, a float64.
func Number() Type {
	digitSet := InSet([]rune("0123456789")...)
	dot := Char('.')
	minus := Char('-')
	return func(input []rune) Result {
		var negative bool
		res := minus(input)
		if res.Err == nil {
			negative = true
		}
		res = Expect(digitSet, "number")(res.Remaining)
		if res.Err != nil {
			return res
		}
		resStr := res.Payload.(string)
		if resTest := dot(res.Remaining); resTest.Err == nil {
			if resTest = digitSet(resTest.Remaining); resTest.Err == nil {
				resStr = resStr + "." + resTest.Payload.(string)
				res = resTest
			}
		}
		if strings.Contains(resStr, ".") {
			f, err := strconv.ParseFloat(resStr, 64)
			if err != nil {
				return Result{
					Err: NewFatalError(
						input,
						fmt.Errorf("failed to parse '%v' as float: %v", resStr, err),
					),
					Remaining: input,
				}
			}
			if negative {
				f = -f
			}
			res.Payload = f
		} else {
			i, err := strconv.ParseInt(resStr, 10, 64)
			if err != nil {
				return Result{
					Err: NewFatalError(
						input,
						fmt.Errorf("failed to parse '%v' as integer: %v", resStr, err),
					),
					Remaining: input,
				}
			}
			if negative {
				i = -i
			}
			res.Payload = i
		}
		return res
	}
}

// Boolean parses either 'true' or 'false' into a boolean value.
func Boolean() Type {
	parser := Expect(OneOf(Term("true"), Term("false")), "boolean")
	return func(input []rune) Result {
		res := parser(input)
		if res.Err == nil {
			res.Payload = res.Payload.(string) == "true"
		}
		return res
	}
}

// Null parses a null literal value.
func Null() Type {
	nullMatch := Term("null")
	return func(input []rune) Result {
		res := nullMatch(input)
		if res.Err == nil {
			res.Payload = nil
		}
		return res
	}
}

// Array parses an array literal.
func Array() Type {
	open, comma, close := Char('['), Char(','), Char(']')
	whitespace := DiscardAll(
		OneOf(
			NewlineAllowComment(),
			SpacesAndTabs(),
		),
	)
	return func(input []rune) Result {
		return DelimitedPattern(
			Expect(Sequence(
				open,
				whitespace,
			), "array"),
			LiteralValue(),
			Sequence(
				Discard(SpacesAndTabs()),
				comma,
				whitespace,
			),
			Sequence(
				whitespace,
				close,
			),
			false, false,
		)(input)
	}
}

// Object parses an object literal.
func Object() Type {
	open, comma, close := Char('{'), Char(','), Char('}')
	whitespace := DiscardAll(
		OneOf(
			NewlineAllowComment(),
			SpacesAndTabs(),
		),
	)

	return func(input []rune) Result {
		res := DelimitedPattern(
			Expect(Sequence(
				open,
				whitespace,
			), "object"),
			Sequence(
				QuotedString(),
				Discard(SpacesAndTabs()),
				Char(':'),
				Discard(whitespace),
				LiteralValue(),
			),
			Sequence(
				Discard(SpacesAndTabs()),
				comma,
				whitespace,
			),
			Sequence(
				whitespace,
				close,
			),
			false, false,
		)(input)
		if res.Err != nil {
			return res
		}

		values := map[string]interface{}{}
		for _, sequenceValue := range res.Payload.([]interface{}) {
			slice := sequenceValue.([]interface{})
			values[slice[0].(string)] = slice[4]
		}

		res.Payload = values
		return res
	}
}

// LiteralValue parses a literal bool, number, quoted string, null value, array
// of literal values, or object.
func LiteralValue() Type {
	return OneOf(
		Boolean(),
		Number(),
		TripleQuoteString(),
		QuotedString(),
		Null(),
		Array(),
		Object(),
	)
}

// JoinStringPayloads wraps a parser that returns a []interface{} of exclusively
// string values and returns a result of a joined string of all the elements.
//
// Warning! If the result is not a []interface{}, or if an element is not a
// string, then this parser returns a zero value instead.
func JoinStringPayloads(p Type) Type {
	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		var buf bytes.Buffer
		slice, _ := res.Payload.([]interface{})

		for _, v := range slice {
			str, _ := v.(string)
			buf.WriteString(str)
		}
		res.Payload = buf.String()
		return res
	}
}

// Comment parses a # comment (always followed by a line break).
func Comment() Type {
	p := JoinStringPayloads(
		Sequence(
			Char('#'),
			JoinStringPayloads(
				Optional(UntilFail(NotChar('\n'))),
			),
			Newline(),
		),
	)
	return func(input []rune) Result {
		return p(input)
	}
}

// SnakeCase parses any number of characters of a camel case string. This parser
// is very strict and does not support double underscores, prefix or suffix
// underscores.
func SnakeCase() Type {
	return Expect(JoinStringPayloads(UntilFail(OneOf(
		InRange('a', 'z'),
		InRange('0', '9'),
		Char('_'),
	))), "snake-case")
}

// TripleQuoteString parses a single instance of a triple-quoted multiple line
// string. The result is the inner contents.
func TripleQuoteString() Type {
	exp := "quoted string"
	return NotEnd(func(input []rune) Result {
		if len(input) < 6 ||
			input[0] != '"' ||
			input[1] != '"' ||
			input[2] != '"' {
			return Result{
				Err:       NewError(input, exp),
				Remaining: input,
			}
		}
		for i := 2; i < len(input)-2; i++ {
			if input[i] == '"' &&
				input[i+1] == '"' &&
				input[i+2] == '"' {
				return Result{
					Payload:   string(input[3:i]),
					Remaining: input[i+3:],
				}
			}
		}
		return Result{
			Err:       NewFatalError(input[len(input):], errors.New("required"), "end triple-quote"),
			Remaining: input,
		}
	}, exp)
}

// QuotedString parses a single instance of a quoted string. The result is the
// inner contents unescaped.
func QuotedString() Type {
	exp := "quoted string"
	return NotEnd(func(input []rune) Result {
		if input[0] != '"' {
			return Result{
				Payload:   nil,
				Err:       NewError(input, exp),
				Remaining: input,
			}
		}
		escaped := false
		for i := 1; i < len(input); i++ {
			if input[i] == '"' && !escaped {
				unquoted, err := strconv.Unquote(string(input[:i+1]))
				if err != nil {
					return Result{
						Err:       NewFatalError(input, fmt.Errorf("failed to unescape quoted string contents: %v", err)),
						Remaining: input,
					}
				}
				return Result{
					Payload:   unquoted,
					Remaining: input[i+1:],
				}
			}
			if input[i] == '\\' {
				escaped = !escaped
			} else if escaped {
				escaped = false
			}
		}
		return Result{
			Err:       NewFatalError(input[len(input):], errors.New("required"), "end quote"),
			Remaining: input,
		}
	}, exp)
}

// Newline parses a line break.
func Newline() Type {
	return Expect(Char('\n'), "line break")
}

// NewlineAllowComment parses an optional comment followed by a mandatory line
// break.
func NewlineAllowComment() Type {
	return Expect(OneOf(Comment(), Char('\n')), "line break")
}

// UntilFail applies a parser until it fails, and returns a slice containing all
// results. If the parser does not succeed at least once an error is returned.
func UntilFail(parser Type) Type {
	return func(input []rune) Result {
		res := parser(input)
		if res.Err != nil {
			return res
		}
		results := []interface{}{res.Payload}
		for {
			if res = parser(res.Remaining); res.Err != nil {
				return Result{
					Payload:   results,
					Remaining: res.Remaining,
				}
			}
			results = append(results, res.Payload)
		}
	}
}

// DelimitedPattern attempts to parse zero or more primary parsers in between an
// start and stop parser, where after the first parse a delimiter is expected.
// Parsing is stopped only once an explicit stop parser is successful.
//
// If allowTrailing is set to false and a delimiter is parsed but a subsequent
// primary parse fails then an error is returned.
//
// Only the results of the primary parser are returned, the results of the
// start, delimiter and stop parsers are discarded. If returnDelimiters is set
// to true then two slices are returned, the first element being a slice of
// primary results and the second element being the delimiter results.
func DelimitedPattern(
	start, primary, delimiter, stop Type,
	allowTrailing, returnDelimiters bool,
) Type {
	return func(input []rune) Result {
		res := start(input)
		if res.Err != nil {
			return res
		}

		results := []interface{}{}
		delims := []interface{}{}
		mkRes := func() interface{} {
			if returnDelimiters {
				return []interface{}{
					results, delims,
				}
			}
			return results
		}
		if res = primary(res.Remaining); res.Err != nil {
			if resStop := stop(res.Remaining); resStop.Err == nil {
				resStop.Payload = mkRes()
				return resStop
			}
			return Result{
				Err:       res.Err,
				Remaining: input,
			}
		}
		results = append(results, res.Payload)

		for {
			if res = delimiter(res.Remaining); res.Err != nil {
				if resStop := stop(res.Remaining); resStop.Err == nil {
					resStop.Payload = mkRes()
					return resStop
				}
				return Result{
					Err:       res.Err,
					Remaining: input,
				}
			}
			delims = append(delims, res.Payload)
			if res = primary(res.Remaining); res.Err != nil {
				if allowTrailing {
					if resStop := stop(res.Remaining); resStop.Err == nil {
						resStop.Payload = mkRes()
						return resStop
					}
				}
				return Result{
					Err:       res.Err,
					Remaining: input,
				}
			}
			results = append(results, res.Payload)
		}
	}
}

// Delimited attempts to parse one or more primary parsers, where after the
// first parse a delimiter is expected. Parsing is stopped only once a delimiter
// parse is not successful.
//
// Two slices are returned, the first element being a slice of primary results
// and the second element being the delimiter results.
func Delimited(primary, delimiter Type) Type {
	return func(input []rune) Result {
		results := []interface{}{}
		delims := []interface{}{}

		res := primary(input)
		if res.Err != nil {
			return res
		}
		results = append(results, res.Payload)

		for {
			if res = delimiter(res.Remaining); res.Err != nil {
				return Result{
					Payload: []interface{}{
						results, delims,
					},
					Remaining: res.Remaining,
				}
			}
			delims = append(delims, res.Payload)
			if res = primary(res.Remaining); res.Err != nil {
				return Result{
					Err:       res.Err,
					Remaining: input,
				}
			}
			results = append(results, res.Payload)
		}
	}
}

// Sequence applies a sequence of parsers and returns either a slice of the
// results or an error if any parser fails.
func Sequence(parsers ...Type) Type {
	return func(input []rune) Result {
		results := make([]interface{}, 0, len(parsers))
		res := Result{
			Remaining: input,
		}
		for _, p := range parsers {
			if res = p(res.Remaining); res.Err != nil {
				return Result{
					Err:       res.Err,
					Remaining: input,
				}
			}
			results = append(results, res.Payload)
		}
		return Result{
			Payload:   results,
			Remaining: res.Remaining,
		}
	}
}

// Optional applies a child parser and if it returns an ExpectedError then it is
// cleared and a nil result is returned instead. Any other form of error will be
// returned unchanged.
func Optional(parser Type) Type {
	return func(input []rune) Result {
		res := parser(input)
		if res.Err != nil && !res.Err.IsFatal() {
			res.Err = nil
		}
		return res
	}
}

// Discard the result of a child parser, regardless of the result. This has the
// effect of running the parser and returning only Remaining.
func Discard(parser Type) Type {
	return func(input []rune) Result {
		res := parser(input)
		res.Payload = nil
		res.Err = nil
		return res
	}
}

// DiscardAll the results of a child parser, applied until it fails. This has
// the effect of running the parser and returning only Remaining.
func DiscardAll(parser Type) Type {
	return func(input []rune) Result {
		res := parser(input)
		for res.Err == nil {
			res = parser(res.Remaining)
		}
		res.Payload = nil
		res.Err = nil
		return res
	}
}

// MustBe applies a parser and if the result is a non-fatal error then it is
// upgraded to a fatal one.
func MustBe(parser Type) Type {
	return func(input []rune) Result {
		res := parser(input)
		if res.Err != nil && !res.Err.IsFatal() {
			res.Err.Err = errors.New("required")
		}
		return res
	}
}

// Expect applies a parser and if an error is returned the list of expected candidates is replaced with the given
// strings. This is useful for providing better context to users.
func Expect(parser Type, expected ...string) Type {
	return func(input []rune) Result {
		res := parser(input)
		if res.Err != nil && !res.Err.IsFatal() {
			res.Err.Expected = expected
		}
		return res
	}
}

// OneOf accepts one or more parsers and tries them in order against an input.
// If a parser returns an ExpectedError then the next parser is tried and so
// on. Otherwise, the result is returned.
func OneOf(parsers ...Type) Type {
	return func(input []rune) Result {
		var err *Error
	tryParsers:
		for _, p := range parsers {
			res := p(input)
			if res.Err != nil {
				if res.Err.IsFatal() {
					return res
				}
				if err == nil || len(err.Input) > len(res.Err.Input) {
					err = res.Err
				} else if len(err.Input) == len(res.Err.Input) {
					err.Add(res.Err)
				}
				continue tryParsers
			}
			return res
		}
		return Result{
			Err:       err,
			Remaining: input,
		}
	}
}

func bestMatch(left, right Result) (Result, bool) {
	remainingLeft := len(left.Remaining)
	remainingRight := len(right.Remaining)
	if left.Err != nil {
		if left.Err.IsFatal() {
			return left, false
		}
		remainingLeft = len(left.Err.Input)
	}
	if right.Err != nil {
		if right.Err.IsFatal() {
			return right, false
		}
		remainingRight = len(right.Err.Input)
	}
	if remainingRight == remainingLeft {
		if left.Err == nil {
			return left, true
		}
		if right.Err == nil {
			return right, true
		}
	}
	if remainingRight < remainingLeft {
		return right, true
	}
	return left, true
}

// BestMatch accepts one or more parsers and tries them all against an input.
// If any parser returns a non ExpectedError error then it is returned. If all
// parsers return either a result or an ExpectedError then the parser that got
// further through the input will have its result returned. This means that an
// error may be returned even if a parser was successful.
//
// For example, given two parsers, A searching for 'aa', and B searching for
// 'aaaa', if the input 'aaab' were provided then an error from parser B would
// be returned, as although the input didn't match, it matched more of parser B
// than parser A.
func BestMatch(parsers ...Type) Type {
	if len(parsers) == 1 {
		return parsers[0]
	}
	return func(input []rune) Result {
		res := parsers[0](input)
		for _, p := range parsers[1:] {
			resTmp := p(input)
			var cont bool
			if res, cont = bestMatch(res, resTmp); !cont {
				return res
			}
		}
		return res
	}
}

//------------------------------------------------------------------------------
