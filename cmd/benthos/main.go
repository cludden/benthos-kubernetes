package main

import (
	"github.com/Jeffail/benthos/v3/lib/service"

	_ "github.com/cludden/benthos-kubernetes/input"
	_ "github.com/cludden/benthos-kubernetes/output"
	_ "github.com/cludden/benthos-kubernetes/processor"
)

//------------------------------------------------------------------------------

func main() {
	service.Run()
}
