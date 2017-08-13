package main

import "time"
import "fmt"

const (
	closed State = iota
	open
	halfOpen
)

// State circuit state
type State int

// Circuit distributed circuit
type Circuit struct {
	State    State
	Failures int
	OpenedAt time.Time
}

// Fire circuit breaker contract
type Fire interface {
	Fire(ID string, args ...interface{}) (interface{}, error)
}

type handler func() (interface{}, error)

type fnResult struct {
	res interface{}
	err error
}

// CircuitBreaker circuit breaker
type CircuitBreaker struct {
	*Options
	fn                                               Fn
	ClosedChan, OpenChan, HalfOpenChan, FallbackChan chan string
}

// Fn function to wrap with breaker
type Fn func(args ...interface{}) (interface{}, error)

// Log delegate to log event
type Log func(message string, context interface{})

// Options circuit breaker options
type Options struct {
	GracePeriodMs time.Duration
	Threshold     int

	TimeoutMs time.Duration

	BackoffMs time.Duration
	Retry     int

	LogError Log
	LogInfo  Log
}

// NewCircuitBreaker create a new circuit breaker
func NewCircuitBreaker(fn Fn, options *Options) *CircuitBreaker {
	cb := new(CircuitBreaker)
	cb.Options = options
	cb.fn = fn
	return cb
}

// Destroy disposes of the circuit breaker
func (breaker *CircuitBreaker) Destroy() {
	close(breaker.ClosedChan)
	close(breaker.OpenChan)
	close(breaker.HalfOpenChan)
	close(breaker.FallbackChan)
}

// Fire the breaker
func (breaker *CircuitBreaker) Fire(ID string, args ...interface{}) (interface{}, error) {
	circuit, err := getOrSetState()
	if err != nil {
		return nil, err
	}

	if circuit.State == closed || circuit.State == halfOpen {
		return breaker.trigger()
	}

	reset, err := breaker.tryReset()
	if err != nil {
		return nil, err
	}

	if reset {
		return breaker.trigger()
	}

	breaker.OpenChan <- ID
	return nil, fmt.Errorf("circuit open for ID: %s", ID)
}

func getOrSetState() (*Circuit, error) {

}

func (breaker *CircuitBreaker) trigger() (interface{}, error) {
	getTimout := func() <-chan time.Time {
		return time.After(time.Millisecond * breaker.TimeoutMs)
	}

	var handler handler
	var timeout <-chan time.Time
	var result chan fnResult

	var start <-chan time.Time
	tryCounter := 0

	for tryCounter < breaker.Retry {
		if result == nil {
			if tryCounter == 0 {
				start = time.After(0)
			} else {
				start = time.After(time.Millisecond * breaker.BackoffMs)
			}
		}

		select {
		case <-timeout:
			timeout = nil
			result = nil

			tryCounter++
			handler = handleFail(fmt.Errorf("A timeout occurred"))
		case <-start:
			timeout = getTimout()
			result = make(chan fnResult, 1)

			go func() {
				res, err := breaker.fn()
				result <- fnResult{res, err}
			}()
		case value := <-result:
			handler = handleSuccess(value)
			goto HANDLE
		}
	}

HANDLE:
	return handler()
}

func handleFail(err error) func() (interface{}, error) {
	return func() (interface{}, error) {
		return nil, nil
	}
}

func handleSuccess(value fnResult) func() (interface{}, error) {
	return func() (interface{}, error) {
		return nil, nil
	}
}

func (breaker *CircuitBreaker) tryReset() (bool, error)
