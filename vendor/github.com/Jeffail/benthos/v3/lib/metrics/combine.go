package metrics

import "github.com/Jeffail/benthos/v3/lib/log"

//------------------------------------------------------------------------------

// combinedWrapper wraps two existing Type implementations. Both implementations
// are fed all metrics.
type combinedWrapper struct {
	t1 Type
	t2 Type
}

// Combine returns a Type implementation that feeds metrics into two underlying
// Type implementations.
func Combine(t1, t2 Type) Type {
	return &combinedWrapper{
		t1: t1,
		t2: t2,
	}
}

//------------------------------------------------------------------------------

type combinedCounter struct {
	c1 StatCounter
	c2 StatCounter
}

func (c *combinedCounter) Incr(count int64) error {
	if err := c.c1.Incr(count); err != nil {
		return err
	}
	return c.c2.Incr(count)
}

type combinedTimer struct {
	c1 StatTimer
	c2 StatTimer
}

func (c *combinedTimer) Timing(delta int64) error {
	if err := c.c1.Timing(delta); err != nil {
		return err
	}
	return c.c2.Timing(delta)
}

type combinedGauge struct {
	c1 StatGauge
	c2 StatGauge
}

func (c *combinedGauge) Set(value int64) error {
	if err := c.c1.Set(value); err != nil {
		return err
	}
	return c.c2.Set(value)
}

func (c *combinedGauge) Incr(count int64) error {
	if err := c.c1.Incr(count); err != nil {
		return err
	}
	return c.c2.Incr(count)
}

func (c *combinedGauge) Decr(count int64) error {
	if err := c.c1.Decr(count); err != nil {
		return err
	}
	return c.c2.Decr(count)
}

//------------------------------------------------------------------------------

type combinedCounterVec struct {
	c1 StatCounterVec
	c2 StatCounterVec
}

func (c *combinedCounterVec) With(labelValues ...string) StatCounter {
	return &combinedCounter{
		c1: c.c1.With(labelValues...),
		c2: c.c2.With(labelValues...),
	}
}

type combinedTimerVec struct {
	c1 StatTimerVec
	c2 StatTimerVec
}

func (c *combinedTimerVec) With(labelValues ...string) StatTimer {
	return &combinedTimer{
		c1: c.c1.With(labelValues...),
		c2: c.c2.With(labelValues...),
	}
}

type combinedGaugeVec struct {
	c1 StatGaugeVec
	c2 StatGaugeVec
}

func (c *combinedGaugeVec) With(labelValues ...string) StatGauge {
	return &combinedGauge{
		c1: c.c1.With(labelValues...),
		c2: c.c2.With(labelValues...),
	}
}

//------------------------------------------------------------------------------

func (c *combinedWrapper) GetCounter(path string) StatCounter {
	return &combinedCounter{
		c1: c.t1.GetCounter(path),
		c2: c.t2.GetCounter(path),
	}
}

func (c *combinedWrapper) GetCounterVec(path string, n []string) StatCounterVec {
	return &combinedCounterVec{
		c1: c.t1.GetCounterVec(path, n),
		c2: c.t2.GetCounterVec(path, n),
	}
}

func (c *combinedWrapper) GetTimer(path string) StatTimer {
	return &combinedTimer{
		c1: c.t1.GetTimer(path),
		c2: c.t2.GetTimer(path),
	}
}

func (c *combinedWrapper) GetTimerVec(path string, n []string) StatTimerVec {
	return &combinedTimerVec{
		c1: c.t1.GetTimerVec(path, n),
		c2: c.t2.GetTimerVec(path, n),
	}
}

func (c *combinedWrapper) GetGauge(path string) StatGauge {
	return &combinedGauge{
		c1: c.t1.GetGauge(path),
		c2: c.t2.GetGauge(path),
	}
}

func (c *combinedWrapper) GetGaugeVec(path string, n []string) StatGaugeVec {
	return &combinedGaugeVec{
		c1: c.t1.GetGaugeVec(path, n),
		c2: c.t2.GetGaugeVec(path, n),
	}
}

func (c *combinedWrapper) SetLogger(log log.Modular) {
	c.t1.SetLogger(log)
	c.t2.SetLogger(log)
}

func (c *combinedWrapper) Close() error {
	c.t1.Close()
	c.t2.Close()
	return nil
}

//------------------------------------------------------------------------------
