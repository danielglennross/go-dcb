package main

// MemoryCache default memory cache
type MemoryCache struct {
	lookup map[string]*Circuit
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
