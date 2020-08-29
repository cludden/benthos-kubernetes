package broker

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// FanIn is a broker that implements types.Producer, takes an array of inputs
// and routes them through a single message channel.
type FanIn struct {
	stats metrics.Type

	transactions chan types.Transaction

	closables       []types.Closable
	inputClosedChan chan int
	inputMap        map[int]struct{}

	closedChan chan struct{}
}

// NewFanIn creates a new FanIn type by providing inputs.
func NewFanIn(inputs []types.Producer, stats metrics.Type) (*FanIn, error) {
	i := &FanIn{
		stats: stats,

		transactions: make(chan types.Transaction),

		inputClosedChan: make(chan int),
		inputMap:        make(map[int]struct{}),

		closables:  []types.Closable{},
		closedChan: make(chan struct{}),
	}

	for n, input := range inputs {
		if closable, ok := input.(types.Closable); ok {
			i.closables = append(i.closables, closable)
		}

		// Keep track of # open inputs
		i.inputMap[n] = struct{}{}

		// Launch goroutine that async writes input into single channel
		go func(index int) {
			defer func() {
				// If the input closes we need to signal to the broker
				i.inputClosedChan <- index
			}()
			for {
				in, open := <-inputs[index].TransactionChan()
				if !open {
					return
				}
				i.transactions <- in
			}
		}(n)
	}

	go i.loop()
	return i, nil
}

//------------------------------------------------------------------------------

// TransactionChan returns the channel used for consuming transactions from this
// broker.
func (i *FanIn) TransactionChan() <-chan types.Transaction {
	return i.transactions
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (i *FanIn) Connected() bool {
	type connector interface {
		Connected() bool
	}
	for _, in := range i.closables {
		if c, ok := in.(connector); ok {
			if !c.Connected() {
				return false
			}
		}
	}
	return true
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (i *FanIn) loop() {
	defer func() {
		close(i.inputClosedChan)
		close(i.transactions)
		close(i.closedChan)
	}()

	for len(i.inputMap) > 0 {
		index := <-i.inputClosedChan
		delete(i.inputMap, index)
	}
}

// CloseAsync shuts down the FanIn broker and stops processing requests.
func (i *FanIn) CloseAsync() {
	for _, closable := range i.closables {
		closable.CloseAsync()
	}
}

// WaitForClose blocks until the FanIn broker has closed down.
func (i *FanIn) WaitForClose(timeout time.Duration) error {
	select {
	case <-i.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
