package response

import "errors"

//------------------------------------------------------------------------------

// Errors for response types.
var (
	ErrNoAck = errors.New("message failed to reach a target destination")
)

//------------------------------------------------------------------------------

// Error is a response type that wraps an error, this error will be interpreted
// as a failed message signal if the error is non-nil, a nil error indicates the
// message has successfully ended up somewhere and can be acknowledged upstream.
type Error struct {
	err error
}

// Error returns the underlying error.
func (o Error) Error() error {
	return o.err
}

// SkipAck indicates whether a successful message should be acknowledged.
// TODO: V4 Remove this once batch processor is removed.
func (o Error) SkipAck() bool {
	return false
}

// NewError returns a response that wraps an error (nil error signals successful
// receipt).
func NewError(err error) Error {
	return Error{
		err: err,
	}
}

//------------------------------------------------------------------------------

// Ack is a response type that indicates the message has reached a destination
// and can be acknowledged upstream.
type Ack struct{}

// Error returns the underlying error.
func (a Ack) Error() error { return nil }

// SkipAck indicates whether a successful message should be acknowledged.
func (a Ack) SkipAck() bool {
	return false
}

// NewAck returns an Ack response type.
func NewAck() Ack {
	return Ack{}
}

//------------------------------------------------------------------------------

// Noack is a response type that indicates the message has failed to reach a
// destination.
type Noack struct{}

// Error returns the underlying error.
func (a Noack) Error() error { return ErrNoAck }

// SkipAck indicates whether a successful message should be acknowledged.
func (a Noack) SkipAck() bool {
	return false
}

// NewNoack returns an Ack response type.
func NewNoack() Noack {
	return Noack{}
}

//------------------------------------------------------------------------------

// Unack is a response type that indicates the message has reached a destination
// but should not yet be acknowledged, if possible. Some input types will be
// able to continue sending messages, and once an acknowledgement has been
// received will propagate the acknowledgement for all unacknowledged messages.
//
// It is considered correct for certain inputs to acknowledge based on an Unack
// response if they are locked in a request/response loop. For example, the HTTP
// input might return a 200 code from this response.
//
// TODO: V4 Remove this once batch processor is removed.
type Unack struct{}

// Error returns the underlying error.
func (u Unack) Error() error { return nil }

// SkipAck indicates whether a successful message should be acknowledged.
func (u Unack) SkipAck() bool {
	return true
}

// NewUnack returns an Unack response type.
func NewUnack() Unack {
	return Unack{}
}

//------------------------------------------------------------------------------
