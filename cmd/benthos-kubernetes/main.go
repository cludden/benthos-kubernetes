package main

import (
	"github.com/Jeffail/benthos/v3/lib/service"

	_ "github.com/cludden/benthos-kubernetes-input/input"
)

//------------------------------------------------------------------------------

func main() {
	service.Run()
}
