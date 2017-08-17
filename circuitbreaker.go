package main

import (
	"fmt"
	"reflect"
	"time"

	"github.com/danielglennross/go-dcb/schema"
)

// Fire circuit breaker contract
type Fire interface {
	Fire(ID string, fn interface{}) (interface{}, error)
}

type handler func(ID string, breaker *CircuitBreaker) (interface{}, error)

type eventHandler func(ID string)

type fnResult struct {
	res interface{}
	err error
}

type circuitChan struct {
	ID    string
	state schema.State
}

type fallbackChan struct {
	ID  string
	err error
}

// CircuitBreaker circuit breaker
type CircuitBreaker struct {
	*Options
	circuitChan                      chan circuitChan
	fallbackChan                     chan fallbackChan
	closed, open, halfOpen, fallback eventHandler
	exit                             chan bool
	cache                            schema.Cache
	lock                             schema.DistLock
	fn                               interface{}
}

// Options circuit breaker options
type Options struct {
	GracePeriodMs int64
	Threshold     int

	TimeoutMs int64

	BackoffMs int64
	Retry     int

	LogError schema.Log
	LogInfo  schema.Log
}

// NewCircuitBreaker create a new circuit breaker
func NewCircuitBreaker(fn interface{}, cache schema.Cache, lock schema.DistLock, options *Options) (*CircuitBreaker, error) {
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

	cb.circuitChan = make(chan circuitChan)
	cb.fallbackChan = make(chan fallbackChan)
	cb.exit = make(chan bool)

	nullEventHandler := func(ID string) {}
	cb.open = nullEventHandler
	cb.halfOpen = nullEventHandler
	cb.closed = nullEventHandler
	cb.fallback = nullEventHandler

	go handleEvents(cb)

	return cb, nil
}

// Destroy disposes of the circuit breaker
func (breaker *CircuitBreaker) Destroy() {
	close(breaker.exit) // kill go routine
	close(breaker.circuitChan)
	close(breaker.fallbackChan)
}

func handleEvents(breaker *CircuitBreaker) {
	for {
		select {
		case <-breaker.exit:
			return
		case f := <-breaker.fallbackChan:
			breaker.fallback(f.ID)
		case c := <-breaker.circuitChan:
			switch c.state {
			case schema.Closed:
				breaker.closed(c.ID)
			case schema.Open:
				breaker.open(c.ID)
			case schema.HalfOpen:
				breaker.halfOpen(c.ID)
			}
		}
	}
}

// OnClosed handle on closed
func (breaker *CircuitBreaker) OnClosed(closed eventHandler) *CircuitBreaker {
	breaker.closed = closed
	return breaker
}

// OnOpen handle on opened
func (breaker *CircuitBreaker) OnOpen(open eventHandler) *CircuitBreaker {
	breaker.open = open
	return breaker
}

// OnHalfOpen handle on half opened
func (breaker *CircuitBreaker) OnHalfOpen(halfOpen eventHandler) *CircuitBreaker {
	breaker.halfOpen = halfOpen
	return breaker
}

// OnFallback handle fallack
func (breaker *CircuitBreaker) OnFallback(fallback eventHandler) *CircuitBreaker {
	breaker.fallback = fallback
	return breaker
}

// Fire the breaker
func (breaker *CircuitBreaker) Fire(ID string, args ...interface{}) (interface{}, error) {
	circuit, err := breaker.getOrSetState(ID)
	if err != nil {
		return nil, err
	}

	if circuit.State == schema.Closed || circuit.State == schema.HalfOpen {
		return breaker.trigger(ID, args)
	}

	reset, err := breaker.tryReset(ID)
	if err != nil {
		return nil, err
	}

	if reset {
		return breaker.trigger(ID, args)
	}

	err = fmt.Errorf("circuit open for ID: %s", ID)
	breaker.fallbackChan <- fallbackChan{ID, err}
	return nil, err
}

func (breaker *CircuitBreaker) getOrSetState(ID string) (*schema.Circuit, error) {
	res, err := breaker.lock.RunCritical(ID, func() (interface{}, error) {
		circuit, err := breaker.cache.Get(ID)
		if err != nil {
			return nil, err
		}
		if circuit == nil {
			circuit = &schema.Circuit{
				State:    schema.Closed,
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
	if err != nil {
		return nil, err
	}
	return res.(*schema.Circuit), nil
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
			if circuit.State == schema.Open {
				return nil, err
			}

			circuit.Failures++

			if circuit.Failures > breaker.Threshold {
				circuit.State = schema.Open
				circuit.OpenedAt = time.Now()

				breaker.circuitChan <- circuitChan{ID, schema.Open}
			}

			breaker.fallbackChan <- fallbackChan{ID, err}

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
			if circuit.State == schema.Closed {
				return value.res, value.err
			}

			circuit.State = schema.Closed
			circuit.Failures = 0

			breaker.cache.Set(ID, circuit)

			breaker.circuitChan <- circuitChan{ID, schema.Closed}

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

		moveToHalfOpen := circuit.State == schema.Open &&
			float64((time.Now().Sub(circuit.OpenedAt)).Nanoseconds())*float64(1e-06) > float64(breaker.GracePeriodMs)

		if moveToHalfOpen {
			circuit.State = schema.HalfOpen

			breaker.cache.Set(ID, circuit)

			breaker.circuitChan <- circuitChan{ID, schema.HalfOpen}
		}

		return false, nil
	})
	return res.(bool), err
}
