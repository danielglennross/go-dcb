package main

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
