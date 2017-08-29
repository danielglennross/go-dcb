package main

import (
	"fmt"
	"math"
	"time"
)

const maxInt64 = float64(math.MaxInt64 - 512)

// Backoff interface
type Backoff interface {
	Duration() time.Duration
}

// Exponential backoff
type Exponential struct {
	*ExponentialOptions
	attempt int
}

// ExponentialOptions options
type ExponentialOptions struct {
	Min, Max time.Duration
	Factor   float64
}

// NewExponential Exponential ctor
func NewExponential(options *ExponentialOptions) (*Exponential, error) {
	e := &Exponential{
		ExponentialOptions: &ExponentialOptions{
			Min:    100 * time.Millisecond,
			Max:    10 * 1000 * time.Millisecond,
			Factor: 2,
		},
		attempt: 0,
	}
	if options.Min != 0 {
		e.Min = options.Min
	}
	if options.Max != 0 {
		e.Max = options.Max
	}
	if options.Factor != 0 {
		e.Factor = options.Factor
	}
	if e.Min > e.Max {
		return nil, fmt.Errorf("Min: %d cannot be greater than Max: %d", e.Min, e.Max)
	}
	return e, nil
}

// Duration exponential
func (b *Exponential) Duration() time.Duration {
	minf := float64(b.Min)
	durf := minf * math.Pow(b.Factor, float64(b.attempt))

	b.attempt++

	// ensure float64 wont overflow int64
	if durf > maxInt64 {
		return b.Max
	}

	dur := time.Duration(durf)

	// keep within bounds
	if dur < b.Min {
		return b.Min
	}

	if dur > b.Max {
		return b.Max
	}

	return dur
}

// Fixed default
type Fixed struct {
	WaitDuration time.Duration
}

// Duration Fixed
func (b *Fixed) Duration() time.Duration {
	if b.WaitDuration == 0 {
		return 300 * time.Millisecond
	}

	return b.WaitDuration
}
