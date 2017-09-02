package schema

import "time"

const (
	Closed State = iota
	Open
	HalfOpen
	Isolate
)

// State circuit state
type State int

// Circuit distributed circuit
type Circuit struct {
	State    State
	Failures int
	OpenedAt time.Time
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

// Log delegate to log event
type Log func(message string, context interface{})
