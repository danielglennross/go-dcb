package main

import (
	"fmt"
	"reflect"
	"time"
)

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
	Fire(ID string, fn interface{}) (interface{}, error)
}

// Cache cache contract
type Cache interface {
	Get(ID string) (*Circuit, error)
	Set(ID string, circuit *Circuit) error
}

// DistLock distributed lock
type DistLock interface {
	RunCritical(ID string, fn func() (interface{}, error)) (interface{}, error)
}

type handler func(ID string, breaker *CircuitBreaker) (interface{}, error)

type fnResult struct {
	res interface{}
	err error
}

// CircuitBreaker circuit breaker
type CircuitBreaker struct {
	*Options
	ClosedChan, OpenChan, HalfOpenChan, FallbackChan chan string
	cache                                            Cache
	lock                                             DistLock
	fn                                               interface{}
}

// Log delegate to log event
type Log func(message string, context interface{})

// Options circuit breaker options
type Options struct {
	GracePeriodMs int64
	Threshold     int

	TimeoutMs int64

	BackoffMs int64
	Retry     int

	LogError Log
	LogInfo  Log
}

// NewCircuitBreaker create a new circuit breaker
func NewCircuitBreaker(fn interface{}, cache Cache, lock DistLock, options *Options) (*CircuitBreaker, error) {
	fnType := reflect.TypeOf(fn)

	if fnType.NumOut() != 2 {
		return nil, fmt.Errorf("Invalid function")
	}

	if _, ok := fnType.Out(0).(interface{}); !ok {
		return nil, fmt.Errorf("Invalid function")
	}

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if fnType.Out(1) != errorType {
		return nil, fmt.Errorf("Invalid function")
	}

	cb := new(CircuitBreaker)
	cb.fn = fn
	cb.Options = options
	cb.cache = cache
	cb.lock = lock
	cb.ClosedChan = make(chan string)
	cb.OpenChan = make(chan string)
	cb.HalfOpenChan = make(chan string)
	cb.FallbackChan = make(chan string)
	return cb, nil
}

// Destroy disposes of the circuit breaker
func (breaker *CircuitBreaker) Destroy() {
	breaker.ClosedChan = nil
	breaker.OpenChan = nil
	breaker.HalfOpenChan = nil
	breaker.FallbackChan = nil
}

// Fire the breaker
func (breaker *CircuitBreaker) Fire(ID string, args ...interface{}) (interface{}, error) {
	circuit, err := breaker.getOrSetState(ID)
	if err != nil {
		return nil, err
	}

	if circuit.State == closed || circuit.State == halfOpen {
		return breaker.trigger(ID, args)
	}

	reset, err := breaker.tryReset(ID)
	if err != nil {
		return nil, err
	}

	if reset {
		return breaker.trigger(ID, args)
	}

	breaker.FallbackChan <- ID
	return nil, fmt.Errorf("circuit open for ID: %s", ID)
}

func (breaker *CircuitBreaker) getOrSetState(ID string) (*Circuit, error) {
	res, err := breaker.lock.RunCritical(ID, func() (interface{}, error) {
		circuit, err := breaker.cache.Get(ID)
		if err != nil {
			return nil, err
		}
		if circuit == nil {
			circuit = &Circuit{
				State:    closed,
				OpenedAt: time.Time{},
				Failures: 0,
			}
			err := breaker.cache.Set(ID, circuit)
			if err != nil {
				return nil, err
			}
		}
		return circuit, nil
	})
	return res.(*Circuit), err
}

func (breaker *CircuitBreaker) trigger(ID string, args []interface{}) (interface{}, error) {
	getTimout := func() <-chan time.Time {
		return time.After(time.Millisecond * time.Duration(breaker.TimeoutMs))
	}

	makeFn := func() (interface{}, error) {
		var arr []reflect.Value
		for _, v := range args {
			arr = append(arr, reflect.ValueOf(v))
		}

		result := reflect.ValueOf(breaker.fn).Call(arr)
		r := result[0].Interface()
		if r != nil {
			return r, nil
		}

		e := result[1].Interface()
		if e != nil {
			return nil, e.(error)
		}

		return nil, fmt.Errorf("Could not execute fn with args %v", args)
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
				start = time.After(time.Millisecond * time.Duration(breaker.BackoffMs))
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
				res, err := makeFn()
				result <- fnResult{res, err}
			}()
		case value := <-result:
			handler = handleSuccess(value)
			goto HANDLE
		}
	}

HANDLE:
	return handler(ID, breaker)
}

func handleFail(err error) func(string, *CircuitBreaker) (interface{}, error) {
	return func(ID string, breaker *CircuitBreaker) (interface{}, error) {
		return breaker.lock.RunCritical(ID, func() (interface{}, error) {
			circuit, cacheErr := breaker.cache.Get(ID)
			if cacheErr != nil {
				return nil, cacheErr
			}
			if circuit == nil {
				return nil, err
			}
			if circuit.State == open {
				return nil, err
			}

			circuit.Failures++

			if circuit.Failures > breaker.Threshold {
				circuit.State = open
				circuit.OpenedAt = time.Now()

				breaker.OpenChan <- ID
			}

			breaker.FallbackChan <- ID

			breaker.cache.Set(ID, circuit)

			return nil, err
		})
	}
}

func handleSuccess(value fnResult) func(string, *CircuitBreaker) (interface{}, error) {
	return func(ID string, breaker *CircuitBreaker) (interface{}, error) {
		return breaker.lock.RunCritical(ID, func() (interface{}, error) {
			circuit, err := breaker.cache.Get(ID)
			if err != nil {
				return nil, err
			}
			if circuit == nil {
				return value.res, value.err
			}
			if circuit.State == closed {
				return value.res, value.err
			}

			circuit.State = closed
			circuit.Failures = 0

			breaker.cache.Set(ID, circuit)

			breaker.ClosedChan <- ID

			return value.res, value.err
		})
	}
}

func (breaker *CircuitBreaker) tryReset(ID string) (bool, error) {
	res, err := breaker.lock.RunCritical(ID, func() (interface{}, error) {
		circuit, err := breaker.cache.Get(ID)
		if err != nil {
			return true, err
		}
		if circuit == nil {
			return true, nil
		}

		moveToHalfOpen := circuit.State == open &&
			float64((time.Now().Sub(circuit.OpenedAt)).Nanoseconds())*float64(1e-06) > float64(breaker.GracePeriodMs)

		if moveToHalfOpen {
			circuit.State = halfOpen

			breaker.cache.Set(ID, circuit)

			breaker.HalfOpenChan <- ID
		}

		return false, nil
	})
	return res.(bool), err
}
