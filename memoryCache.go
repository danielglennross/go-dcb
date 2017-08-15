package main

import "sync"

// MemoryCache default memory cache
type MemoryCache struct {
	lookup map[string]*Circuit
	mutex  sync.Mutex
}

// NewMemoryCache ctor
func NewMemoryCache() *MemoryCache {
	cache := new(MemoryCache)
	cache.lookup = make(map[string]*Circuit)
	return cache
}

// Get gets item from cache
func (cache *MemoryCache) Get(ID string) (*Circuit, error) {
	circuit := cache.lookup[ID]
	return circuit, nil
}

// Set sets item in cache
func (cache *MemoryCache) Set(ID string, circuit *Circuit) error {
	cache.lookup[ID] = circuit
	return nil
}

// RunCritical run critical section
func (cache *MemoryCache) RunCritical(ID string, fn func() (interface{}, error)) (interface{}, error) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	return fn()
}
