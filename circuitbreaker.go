package main

import (
	"fmt"
	"reflect"
	"time"

	"github.com/danielglennross/go-dcb/policies"
	"github.com/danielglennross/go-dcb/schema"
)

// CircuitBreakerFn circuit breaker func
type CircuitBreakerFn func() (interface{}, error)

// FireDynamic circuit breaker contract
type FireDynamic interface {
	Fire(ID string, args ...interface{}) (interface{}, error)
}

// FireStatic circuit breaker contract
type FireStatic interface {
	Fire(ID string, fn CircuitBreakerFn) (interface{}, error)
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

type failCondition func(err error) bool

// CircuitBreaker regular
type CircuitBreaker struct {
	*options
	circuitChan                      chan circuitChan
	fallbackChan                     chan fallbackChan
	closed, open, halfOpen, fallback eventHandler
	exit                             chan bool
	cache                            schema.Cache
	lock                             schema.DistLock
}

// CircuitBreakerDynamic circuit breaker
type CircuitBreakerDynamic struct {
	*CircuitBreaker
	fn interface{}
}

// Options circuit breaker options
type options struct {
	gracePeriodMs int64
	threshold     int

	timeoutMs int64

	failCondition failCondition
	backoff       policies.Backoff
	retry         int

	logError schema.Log
	logInfo  schema.Log
}

type circuitBreakerOption func(*CircuitBreaker)

// FailCondition condition which to fail the circuit breaker
func FailCondition(failCondition failCondition) circuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.failCondition = failCondition
	}
}

// GracePeriodMs grace period in milliseconds
func GracePeriodMs(gp int64) circuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.gracePeriodMs = gp
	}
}

// Threshold threshold (< 1)
func Threshold(t int) circuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.threshold = t
	}
}

// TimeoutMs in milliseconds
func TimeoutMs(t int64) circuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.timeoutMs = t
	}
}

// BackoffMs in milliseconds
func BackoffMs(b policies.Backoff) circuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.backoff = b
	}
}

// Retry count
func Retry(r int) circuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.retry = r
	}
}

// LogError log error delegate
func LogError(le schema.Log) circuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.logError = le
	}
}

// LogInfo log info delegate
func LogInfo(li schema.Log) circuitBreakerOption {
	return func(cb *CircuitBreaker) {
		cb.logInfo = li
	}
}

// NewCircuitBreaker ctor
func NewCircuitBreaker(cache schema.Cache, lock schema.DistLock, options ...circuitBreakerOption) (*CircuitBreaker, error) {
	cb := new(CircuitBreaker)

	initCircuitBreaker(cb, cache, lock, options...)

	return cb, nil
}

// NewCircuitBreakerDynamic ctor
func NewCircuitBreakerDynamic(fn interface{}, cache schema.Cache, lock schema.DistLock, options ...circuitBreakerOption) (*CircuitBreakerDynamic, error) {
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

	cb := new(CircuitBreakerDynamic)
	cb.fn = fn

	initCircuitBreaker(cb.CircuitBreaker, cache, lock, options...)

	return cb, nil
}

// Destroy disposes of the circuit breaker
func (breaker *CircuitBreaker) Destroy() {
	close(breaker.exit) // kill go routine
	close(breaker.circuitChan)
	close(breaker.fallbackChan)
}

// Isolate manually open (and hold open) a circuit breaker
func (breaker *CircuitBreaker) Isolate(ID string) bool {
	return breaker.safelyUpdateCircuit(ID, func(circuit *schema.Circuit) {
		circuit.State = schema.Isolate

		breaker.circuitChan <- circuitChan{ID, schema.Open}
		breaker.fallbackChan <- fallbackChan{ID, fmt.Errorf("Ioslating ID %s", ID)}
	})
}

// Reset resets a circuit to closed
func (breaker *CircuitBreaker) Reset(ID string) bool {
	return breaker.safelyUpdateCircuit(ID, func(circuit *schema.Circuit) {
		circuit.State = schema.Closed
		circuit.OpenedAt = time.Time{}
		circuit.Failures = 0

		breaker.circuitChan <- circuitChan{ID, schema.Closed}
	})
}

func initCircuitBreaker(cb *CircuitBreaker, cache schema.Cache, lock schema.DistLock, options ...circuitBreakerOption) {
	cb.cache = cache
	cb.lock = lock

	cb.gracePeriodMs = 500
	cb.threshold = 1
	cb.timeoutMs = 3000

	cb.failCondition = func(err error) bool { return true }
	cb.backoff = &policies.Fixed{WaitDuration: 300 * time.Millisecond}
	cb.retry = 3

	cb.circuitChan = make(chan circuitChan)
	cb.fallbackChan = make(chan fallbackChan)
	cb.exit = make(chan bool)

	nullEventHandler := func(ID string) {}
	cb.open = nullEventHandler
	cb.halfOpen = nullEventHandler
	cb.closed = nullEventHandler
	cb.fallback = nullEventHandler

	cb.logError = func(message string, context interface{}) {}
	cb.logInfo = func(message string, context interface{}) {}

	for _, opt := range options {
		opt(cb)
	}

	go handleEvents(cb)
}

func (breaker *CircuitBreaker) safelyUpdateCircuit(ID string, fn func(circuit *schema.Circuit)) bool {
	res, err := breaker.lock.RunCritical(ID, func() (interface{}, error) {
		circuit, err := breaker.cache.Get(ID)
		if err != nil {
			return false, nil
		}

		fn(circuit)

		err = breaker.cache.Set(ID, circuit)
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return false
	}
	return res.(bool)
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

// Fire the static breaker
func (breaker *CircuitBreaker) Fire(ID string, fn CircuitBreakerFn) (interface{}, error) {
	circuit, err := breaker.getOrSetState(ID)
	if err != nil {
		return nil, err
	}

	handleOpen := func() (interface{}, error) {
		err = fmt.Errorf("circuit open for ID: %s", ID)
		breaker.fallbackChan <- fallbackChan{ID, err}
		return nil, err
	}

	if circuit.State == schema.Isolate {
		return handleOpen()
	}

	if circuit.State == schema.Open {
		reset, err := breaker.tryReset(ID)
		if err != nil {
			return nil, err
		}

		if !reset {
			return handleOpen()
		}
	}

	// Closed || HalfOpen
	return breaker.trigger(ID, fn)
}

// Fire the dynamic breaker
func (dBreaker *CircuitBreakerDynamic) Fire(ID string, args ...interface{}) (interface{}, error) {
	breaker := dBreaker.CircuitBreaker
	fnc := dBreaker.fn

	fn := func() (interface{}, error) {
		var arr []reflect.Value
		for _, v := range args {
			arr = append(arr, reflect.ValueOf(v))
		}

		result := reflect.ValueOf(fnc).Call(arr)
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

	return breaker.Fire(ID, fn)
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

func (breaker *CircuitBreaker) trigger(ID string, fn CircuitBreakerFn) (interface{}, error) {
	getTimout := func() <-chan time.Time {
		return time.After(time.Millisecond * time.Duration(breaker.timeoutMs))
	}

	var handler handler
	var timeout <-chan time.Time
	var result chan fnResult

	var start <-chan time.Time
	tryCounter := 0

	for tryCounter < breaker.retry {
		if result == nil {
			if tryCounter == 0 {
				start = time.After(0)
			} else {
				start = time.After(breaker.backoff.Duration())
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
				defer func() {
					e := recover()
					if e != nil {
						_, _ = handleFail(fmt.Errorf("A panic occurred"))(ID, breaker)
						panic(e)
					}
				}()

				res, err := fn()
				result <- fnResult{res, err}
			}()
		case value := <-result:
			if value.err != nil && breaker.failCondition(value.err) {
				handler = handleFail(value.err)
			} else {
				handler = handleSuccess(value.res)
			}
			goto HANDLE
		}
	}

HANDLE:
	return handler(ID, breaker)
}

func handleFail(err error) handler {
	return wrapSafeHandler(func(ID string, breaker *CircuitBreaker) (interface{}, error) {
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

		if circuit.Failures > breaker.threshold {
			circuit.State = schema.Open
			circuit.OpenedAt = time.Now()

			breaker.circuitChan <- circuitChan{ID, schema.Open}
		}

		breaker.fallbackChan <- fallbackChan{ID, err}

		breaker.cache.Set(ID, circuit)

		return nil, err
	})
}

func handleSuccess(value interface{}) handler {
	return wrapSafeHandler(func(ID string, breaker *CircuitBreaker) (interface{}, error) {
		circuit, cacheErr := breaker.cache.Get(ID)
		if cacheErr != nil {
			return nil, cacheErr
		}
		if circuit == nil {
			return value, nil
		}
		if circuit.State == schema.Closed {
			return value, nil
		}

		circuit.State = schema.Closed
		circuit.Failures = 0

		breaker.cache.Set(ID, circuit)

		breaker.circuitChan <- circuitChan{ID, schema.Closed}

		return value, nil
	})
}

func wrapSafeHandler(handle handler) handler {
	return func(ID string, breaker *CircuitBreaker) (interface{}, error) {
		return breaker.lock.RunCritical(ID, func() (interface{}, error) {
			return handle(ID, breaker)
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
			float64((time.Now().Sub(circuit.OpenedAt)).Nanoseconds())*float64(1e-06) > float64(breaker.gracePeriodMs)

		if moveToHalfOpen {
			circuit.State = schema.HalfOpen

			breaker.cache.Set(ID, circuit)

			breaker.circuitChan <- circuitChan{ID, schema.HalfOpen}
		}

		return false, nil
	})
	return res.(bool), err
}
