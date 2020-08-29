package output

import (
	"bytes"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// LineWriter is an output type that writes messages to an io.WriterCloser type
// as lines.
type LineWriter struct {
	running int32

	typeStr string
	log     log.Modular
	stats   metrics.Type

	customDelim []byte

	transactions <-chan types.Transaction

	handle      io.WriteCloser
	closeOnExit bool

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewLineWriter creates a new LineWriter output type.
func NewLineWriter(
	handle io.WriteCloser,
	closeOnExit bool,
	customDelimiter []byte,
	typeStr string,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	return &LineWriter{
		running:     1,
		typeStr:     typeStr,
		log:         log,
		stats:       stats,
		customDelim: customDelimiter,
		handle:      handle,
		closeOnExit: closeOnExit,
		closeChan:   make(chan struct{}),
		closedChan:  make(chan struct{}),
	}, nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (w *LineWriter) loop() {
	// Metrics paths
	var (
		mCount     = w.stats.GetCounter("count")
		mPartsSent = w.stats.GetCounter("sent")
		mSent      = w.stats.GetCounter("batch.sent")
		mBytesSent = w.stats.GetCounter("batch.bytes")
		mLatency   = w.stats.GetTimer("batch.latency")
	)

	defer func() {
		if w.closeOnExit {
			w.handle.Close()
		}
		close(w.closedChan)
	}()

	delim := []byte("\n")
	if len(w.customDelim) > 0 {
		delim = w.customDelim
	}

	for atomic.LoadInt32(&w.running) == 1 {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-w.transactions:
			if !open {
				return
			}
			mCount.Incr(1)
		case <-w.closeChan:
			return
		}

		w.log.Tracef("Attempting to write %v messages to '%v'.\n", ts.Payload.Len(), w.typeStr)
		spans := tracing.CreateChildSpans("output_"+w.typeStr, ts.Payload)

		var err error
		t0 := time.Now()
		if ts.Payload.Len() == 1 {
			_, err = fmt.Fprintf(w.handle, "%s%s", ts.Payload.Get(0).Get(), delim)
		} else {
			_, err = fmt.Fprintf(w.handle, "%s%s%s", bytes.Join(message.GetAllBytes(ts.Payload), delim), delim, delim)
		}
		latency := time.Since(t0).Nanoseconds()
		if err == nil {
			mSent.Incr(1)
			mPartsSent.Incr(int64(ts.Payload.Len()))
			mBytesSent.Incr(int64(message.GetAllBytesLen(ts.Payload)))
			w.log.Tracef("Successfully wrote %v messages to '%v'.\n", ts.Payload.Len(), w.typeStr)
			mLatency.Timing(latency)
		}

		for _, s := range spans {
			s.Finish()
		}

		select {
		case ts.ResponseChan <- response.NewError(err):
		case <-w.closeChan:
			return
		}
	}
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (w *LineWriter) Connected() bool {
	return true
}

// Consume assigns a messages channel for the output to read.
func (w *LineWriter) Consume(ts <-chan types.Transaction) error {
	if w.transactions != nil {
		return types.ErrAlreadyStarted
	}
	w.transactions = ts
	go w.loop()
	return nil
}

// CloseAsync shuts down the File output and stops processing messages.
func (w *LineWriter) CloseAsync() {
	if atomic.CompareAndSwapInt32(&w.running, 1, 0) {
		close(w.closeChan)
	}
}

// WaitForClose blocks until the File output has closed down.
func (w *LineWriter) WaitForClose(timeout time.Duration) error {
	select {
	case <-w.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
