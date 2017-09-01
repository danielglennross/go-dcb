package policies

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
	min, max time.Duration
	factor   float64
	attempt  int
}

type exponentialOption func(*Exponential)

// Min duration in milliseconds
func Min(min time.Duration) exponentialOption {
	return func(e *Exponential) {
		e.min = min
	}
}

// Max duration in milliseconds
func Max(max time.Duration) exponentialOption {
	return func(e *Exponential) {
		e.max = max
	}
}

// Factor exponent
func Factor(factor float64) exponentialOption {
	return func(e *Exponential) {
		e.factor = factor
	}
}

// NewExponential Exponential ctor
func NewExponential(options ...exponentialOption) (*Exponential, error) {
	e := &Exponential{
		min:     100 * time.Millisecond,
		max:     10 * 1000 * time.Millisecond,
		factor:  2,
		attempt: 0,
	}

	for _, opt := range options {
		opt(e)
	}

	if e.min > e.max {
		return nil, fmt.Errorf("Min: %dms cannot be greater than Max: %dms",
			int64(e.min/time.Millisecond),
			int64(e.max/time.Millisecond),
		)
	}
	return e, nil
}

// Duration exponential
func (b *Exponential) Duration() time.Duration {
	minf := float64(b.min)
	durf := minf * math.Pow(b.factor, float64(b.attempt))

	b.attempt++

	// ensure float64 wont overflow int64
	if durf > maxInt64 {
		return b.max
	}

	dur := time.Duration(durf)

	// keep within bounds
	if dur < b.min {
		return b.min
	}

	if dur > b.max {
		return b.max
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
