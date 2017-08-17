package cache

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/danielglennross/go-dcb/schema"
	"github.com/go-redis/redis"
)

const (
	lockScript   = `return redis.call("set", KEYS[1], ARGV[1], "NX", "PX", ARGV[2])`
	unlockScript = `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
	`
)

// ReditClientOptions client options
type ReditClientOptions struct {
	Address  string
	Password string
	DB       int
}

// ReditCacheOptions cache options
type ReditCacheOptions struct {
	*ReditClientOptions

	LogError schema.Log
	LogInfo  schema.Log
	TTL      int
}

// ReditCache default memory cache
type ReditCache struct {
	*ReditCacheOptions
	client *redis.Client
}

// CommonRedLockOptions common lock options
type CommonRedLockOptions struct {
	RetryCount   int
	RetryDelayMs int
	DriftFactor  float64
	TTLms        int
	LogError     schema.Log
	LogInfo      schema.Log
}

// RedLockOptions red lock options
type RedLockOptions struct {
	CommonRedLockOptions
	ClientOptions []*ReditClientOptions
}

// RedLock redis lock
type RedLock struct {
	CommonRedLockOptions

	clients []*redis.Client
	quorum  int

	resource string
	val      string
	expiry   int64
}

// NewReditCache ctor
func NewReditCache(options *ReditCacheOptions) *ReditCache {
	cache := new(ReditCache)
	cache.ReditCacheOptions = &ReditCacheOptions{
		ReditClientOptions: &ReditClientOptions{
			Address:  options.ReditClientOptions.Address,
			Password: options.ReditClientOptions.Password,
			DB:       options.ReditClientOptions.DB,
		},
	}
	cache.TTL = options.TTL
	cache.LogError = options.LogError
	cache.LogInfo = options.LogInfo

	cache.client = redis.NewClient(&redis.Options{
		Addr:     options.Address,
		Password: options.Password,
		DB:       options.DB,
	})

	if cache.TTL == 0 {
		cache.TTL = 500
	}
	if cache.LogError == nil {
		cache.LogError = func(message string, context interface{}) {}
	}
	if cache.LogInfo == nil {
		cache.LogInfo = func(message string, context interface{}) {}
	}

	return cache
}

// NewRedLock create new red lock
func NewRedLock(options *RedLockOptions) *RedLock {
	clients := []*redis.Client{}
	for _, co := range options.ClientOptions {
		c := redis.NewClient(&redis.Options{
			Addr:     co.Address,
			Password: co.Password,
			DB:       co.DB,
		})
		clients = append(clients, c)
	}

	rl := new(RedLock)
	rl.clients = clients
	rl.CommonRedLockOptions = options.CommonRedLockOptions

	if rl.RetryDelayMs == 0 {
		rl.RetryDelayMs = 300
	}
	if rl.RetryCount == 0 {
		rl.RetryDelayMs = 3
	}
	if rl.DriftFactor == 0 {
		rl.DriftFactor = 0.01
	}
	if rl.TTLms == 0 {
		rl.TTLms = 500
	}
	if rl.LogError == nil {
		rl.LogError = func(message string, context interface{}) {}
	}
	if rl.LogInfo == nil {
		rl.LogInfo = func(message string, context interface{}) {}
	}
	return rl
}

// Get gets item from cache
func (cache *ReditCache) Get(ID string) (*schema.Circuit, error) {
	val, err := cache.client.Get(ID).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	res := &schema.Circuit{}
	json.Unmarshal([]byte(val), res)
	return res, nil
}

// Set sets item in cache
func (cache *ReditCache) Set(ID string, circuit *schema.Circuit) error {
	cir, err := json.Marshal(circuit)
	if err != nil {
		return err
	}
	ttl := time.Millisecond * time.Duration(cache.TTL)
	return cache.client.Set(ID, cir, ttl).Err()
}

// RunCritical run critical section
func (rl *RedLock) RunCritical(ID string, fn func() (interface{}, error)) (interface{}, error) {
	rl.lock(fmt.Sprintf("%s-lock", ID))
	defer rl.unlock(fmt.Sprintf("%s-lock", ID))
	return fn()
}

func (rl *RedLock) lock(ID string) error {
	lockInstance := func(client *redis.Client, ID string, ttl int, c chan bool) {
		_, err := client.Eval(lockScript, []string{ID}, ID, strconv.Itoa(ttl)).Result()
		c <- err == nil
	}

	for i := 0; i < rl.RetryCount; i++ {
		ttlMs := rl.TTLms
		success := 0
		startNs := time.Now().UnixNano()

		c := make(chan bool, len(rl.clients))
		for _, client := range rl.clients {
			go lockInstance(client, ID, ttlMs, c)
		}
		for j := 0; j < len(rl.clients); j++ {
			if <-c {
				success++
			}
		}
		close(c)

		driftMs := int(float64(ttlMs)*rl.DriftFactor) + 2
		costTimeMs := (time.Now().UnixNano() - startNs) / 1e6
		validityTime := int64(ttlMs) - costTimeMs - int64(driftMs)

		quorum := (len(rl.clients) / 2) + 1
		if success >= quorum && validityTime > 0 {
			return nil
		}

		rl.unlock(ID)
		time.Sleep(time.Duration(rl.RetryDelayMs) * time.Millisecond)
	}

	return fmt.Errorf("Failed to lock for %s", ID)
}

func (rl *RedLock) unlock(ID string) {
	var wg sync.WaitGroup

	unlockInstance := func(client *redis.Client, ID string) {
		defer wg.Done()

		_, err := client.Eval(unlockScript, []string{ID}, ID).Result()

		rl.LogError(fmt.Sprintf("Could not unlock ID %s", ID), err)
	}

	wg.Add(len(rl.clients))
	for _, client := range rl.clients {
		go unlockInstance(client, ID)
	}

	wg.Wait()
}
