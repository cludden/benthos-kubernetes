package message

import (
	"context"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// GetContext either returns a context attached to the message part, or
// context.Background() if one hasn't been previously attached.
func GetContext(p types.Part) context.Context {
	if ctxProvider, ok := p.(interface {
		GetContext() context.Context
	}); ok {
		return ctxProvider.GetContext()
	}
	return context.Background()
}

// WithContext returns the same message part wrapped with a context, this
// context can subsequently be received with GetContext.
func WithContext(ctx context.Context, p types.Part) types.Part {
	if ctxProvider, ok := p.(interface {
		WithContext(context.Context) types.Part
	}); ok {
		return ctxProvider.WithContext(ctx)
	}
	return &partWithContext{
		p:   p,
		ctx: ctx,
	}
}

//------------------------------------------------------------------------------

// partWithContext wraps a types.Part with a context.
type partWithContext struct {
	p   types.Part
	ctx context.Context
}

//------------------------------------------------------------------------------

// GetContext returns the underlying context attached to this message part.
func (p *partWithContext) GetContext() context.Context {
	return p.ctx
}

// WithContext returns the underlying message part with a different context
// attached.
func (p *partWithContext) WithContext(ctx context.Context) types.Part {
	return &partWithContext{
		p:   p.p,
		ctx: ctx,
	}
}

//------------------------------------------------------------------------------

// Copy creates a shallow copy of the message part.
func (p *partWithContext) Copy() types.Part {
	return &partWithContext{
		p:   p.p.Copy(),
		ctx: p.ctx,
	}
}

// DeepCopy creates a new deep copy of the message part.
func (p *partWithContext) DeepCopy() types.Part {
	return &partWithContext{
		p:   p.p.DeepCopy(),
		ctx: p.ctx,
	}
}

//------------------------------------------------------------------------------

// Get returns the body of the message part.
func (p *partWithContext) Get() []byte {
	return p.p.Get()
}

// Metadata returns the metadata of the message part.
func (p *partWithContext) Metadata() types.Metadata {
	return p.p.Metadata()
}

// JSON attempts to parse the message part as a JSON document and returns the
// result.
func (p *partWithContext) JSON() (interface{}, error) {
	return p.p.JSON()
}

// Set the value of the message part.
func (p *partWithContext) Set(data []byte) types.Part {
	p.p.Set(data)
	return p
}

// SetMetadata sets the metadata of a message part.
func (p *partWithContext) SetMetadata(meta types.Metadata) types.Part {
	p.p.SetMetadata(meta)
	return p
}

// SetJSON attempts to marshal a JSON document into a byte slice and stores the
// result as the contents of the message part.
func (p *partWithContext) SetJSON(jObj interface{}) error {
	return p.p.SetJSON(jObj)
}

//------------------------------------------------------------------------------

// IsEmpty returns true if the message part is empty.
func (p *partWithContext) IsEmpty() bool {
	return p.p.IsEmpty()
}

//------------------------------------------------------------------------------
