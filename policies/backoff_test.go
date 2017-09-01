package policies

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFixedDurationReturnsDefault(t *testing.T) {
	f := &Fixed{}

	v := f.Duration()
	require.Equal(t, v, 300*time.Millisecond)
}

func TestFixedDurationReturnsUnchangedValue(t *testing.T) {
	d := 100 * time.Millisecond
	f := &Fixed{
		WaitDuration: d,
	}

	v := f.Duration()
	require.Equal(t, v, d)
}

func TestFixedDurationReturnsConsistentValue(t *testing.T) {
	d := 100 * time.Millisecond
	f := &Fixed{
		WaitDuration: d,
	}

	v := f.Duration()
	require.Equal(t, v, d)

	v = f.Duration()
	require.Equal(t, v, d)
}

func TestExponentialReturnsErrorIfMinGreaterThanMax(t *testing.T) {
	_, err := NewExponential(&ExponentialOptions{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Millisecond,
		Factor: 2,
	})

	require.EqualError(t, err, "Min: 100ms cannot be greater than Max: 10ms")
}

func TestExponentialReturnsDefaultValue(t *testing.T) {
	defaultMin := 100 * time.Millisecond

	e, _ := NewExponential(nil)

	v := e.Duration()
	require.Equal(t, v, defaultMin)
}

func TestExponentialReturnsMinValue(t *testing.T) {
	min := 200 * time.Millisecond

	e, _ := NewExponential(&ExponentialOptions{
		Min:    min,
		Max:    10 * time.Second,
		Factor: 2,
	})

	v := e.Duration()
	require.Equal(t, v, min)
}

func TestExponentialReturnsIncreasingValue(t *testing.T) {
	e, _ := NewExponential(&ExponentialOptions{
		Min:    200 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
	})

	v := e.Duration()
	require.Equal(t, v, 200*time.Millisecond)

	v = e.Duration()
	require.Equal(t, v, 400*time.Millisecond)

	v = e.Duration()
	require.Equal(t, v, 800*time.Millisecond)

	v = e.Duration()
	require.Equal(t, v, 1600*time.Millisecond)
}

func TestExponentialDoesNotExceedMax(t *testing.T) {
	e, _ := NewExponential(&ExponentialOptions{
		Min:    200 * time.Millisecond,
		Max:    400 * time.Millisecond,
		Factor: 2,
	})

	v := e.Duration()
	require.Equal(t, v, 200*time.Millisecond)

	v = e.Duration()
	require.Equal(t, v, 400*time.Millisecond)

	v = e.Duration()
	require.Equal(t, v, 400*time.Millisecond)
}
