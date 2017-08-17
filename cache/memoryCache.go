package cache

import (
	"sync"

	"github.com/danielglennross/go-dcb/schema"
)

// MemoryCache default memory cache
type MemoryCache struct {
	lookup map[string]*schema.Circuit
	mutex  sync.Mutex
}

// NewMemoryCache ctor
func NewMemoryCache() *MemoryCache {
	cache := new(MemoryCache)
	cache.lookup = make(map[string]*schema.Circuit)
	return cache
}

// Get gets item from cache
func (cache *MemoryCache) Get(ID string) (*schema.Circuit, error) {
	circuit := cache.lookup[ID]
	return circuit, nil
}

// Set sets item in cache
func (cache *MemoryCache) Set(ID string, circuit *schema.Circuit) error {
	cache.lookup[ID] = circuit
	return nil
}

// RunCritical run critical section
func (cache *MemoryCache) RunCritical(ID string, fn func() (interface{}, error)) (interface{}, error) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	return fn()
}
