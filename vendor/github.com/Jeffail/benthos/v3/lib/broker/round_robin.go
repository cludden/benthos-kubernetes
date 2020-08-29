package broker

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// RoundRobin is a broker that implements types.Consumer and sends each message
// out to a single consumer chosen from an array in round-robin fashion.
// Consumers that apply backpressure will block all consumers.
type RoundRobin struct {
	running int32

	stats metrics.Type

	transactions <-chan types.Transaction

	outputTsChans []chan types.Transaction
	outputs       []types.Output

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewRoundRobin creates a new RoundRobin type by providing consumers.
func NewRoundRobin(outputs []types.Output, stats metrics.Type) (*RoundRobin, error) {
	o := &RoundRobin{
		running:      1,
		stats:        stats,
		transactions: nil,
		outputs:      outputs,
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}
	o.outputTsChans = make([]chan types.Transaction, len(o.outputs))
	for i := range o.outputTsChans {
		o.outputTsChans[i] = make(chan types.Transaction)
		if err := o.outputs[i].Consume(o.outputTsChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

//------------------------------------------------------------------------------

// Consume assigns a new messages channel for the broker to read.
func (o *RoundRobin) Consume(ts <-chan types.Transaction) error {
	if o.transactions != nil {
		return types.ErrAlreadyStarted
	}
	o.transactions = ts

	go o.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (o *RoundRobin) Connected() bool {
	for _, out := range o.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (o *RoundRobin) loop() {
	defer func() {
		for _, c := range o.outputTsChans {
			close(c)
		}
		close(o.closedChan)
	}()

	var (
		mMsgsRcvd = o.stats.GetCounter("messages.received")
	)

	i := 0
	var open bool
	for atomic.LoadInt32(&o.running) == 1 {
		var ts types.Transaction
		select {
		case ts, open = <-o.transactions:
			if !open {
				return
			}
		case <-o.closeChan:
			return
		}
		mMsgsRcvd.Incr(1)
		select {
		case o.outputTsChans[i] <- ts:
		case <-o.closeChan:
			return
		}

		i++
		if i >= len(o.outputTsChans) {
			i = 0
		}
	}
}

// CloseAsync shuts down the RoundRobin broker and stops processing requests.
func (o *RoundRobin) CloseAsync() {
	if atomic.CompareAndSwapInt32(&o.running, 1, 0) {
		close(o.closeChan)
	}
}

// WaitForClose blocks until the RoundRobin broker has closed down.
func (o *RoundRobin) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
