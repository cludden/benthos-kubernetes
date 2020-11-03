package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	"github.com/urfave/cli/v2"
)

//------------------------------------------------------------------------------

func listableStatus(s docs.Status) bool {
	switch s {
	case "": // Empty status is the equivalent of stable.
		return true
	case docs.StatusStable:
		return true
	case docs.StatusBeta:
		return true
	}
	return false
}

func listComponents(c *cli.Context) {
	jsonFmt := c.String("format") == "json"

	ofType := c.Args().Slice()
	whitelist := map[string]struct{}{}
	for _, k := range ofType {
		whitelist[k] = struct{}{}
	}

	var buf bytes.Buffer
	obj := map[string]interface{}{}

	components := []string{}
	printAll := func(title string) {
		typeStr := strings.ReplaceAll(strings.ToLower(title), " ", "-")
		if _, exists := whitelist[typeStr]; len(ofType) > 0 && !exists {
			components = nil
			return
		}
		sort.Strings(components)
		if jsonFmt {
			cCopy := make([]string, len(components))
			copy(cCopy, components)
			obj[typeStr] = cCopy
		} else {
			if buf.Len() > 0 {
				fmt.Fprintln(&buf, "")
			}
			fmt.Fprintf(&buf, "%v:\n", title)
			for _, t := range components {
				fmt.Fprintf(&buf, "  - %v\n", t)
			}
		}
		components = nil
	}
	defer func() {
		if jsonFmt {
			b, err := json.Marshal(obj)
			if err != nil {
				panic(err)
			}
			fmt.Println(string(b))
		} else {
			fmt.Print(buf.String())
		}
	}()

	for t, c := range input.Constructors {
		if listableStatus(c.Status) {
			components = append(components, t)
		}
	}
	printAll("Inputs")

	for t, c := range processor.Constructors {
		if listableStatus(c.Status) {
			components = append(components, t)
		}
	}
	printAll("Processors")

	for t := range condition.Constructors {
		components = append(components, t)
	}
	printAll("Conditions")

	for t, c := range output.Constructors {
		if listableStatus(c.Status) {
			components = append(components, t)
		}
	}
	printAll("Outputs")

	for t, c := range cache.Constructors {
		if listableStatus(c.Status) {
			components = append(components, t)
		}
	}
	printAll("Caches")

	for t, c := range ratelimit.Constructors {
		if listableStatus(c.Status) {
			components = append(components, t)
		}
	}
	printAll("Rate Limits")

	for t, c := range buffer.Constructors {
		if listableStatus(c.Status) {
			components = append(components, t)
		}
	}
	printAll("Buffers")

	for t, c := range metrics.Constructors {
		if listableStatus(c.Status) {
			components = append(components, t)
		}
	}
	printAll("Metrics")

	for t, c := range tracer.Constructors {
		if listableStatus(c.Status) {
			components = append(components, t)
		}
	}
	printAll("Tracers")

	components = query.ListFunctions()
	printAll("Bloblang Functions")

	components = query.ListMethods()
	printAll("Bloblang Methods")
}

//------------------------------------------------------------------------------
