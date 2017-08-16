package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

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

	LogError Log
	LogInfo  Log
	TTL      int
}

// ReditCache default memory cache
type ReditCache struct {
	*ReditCacheOptions
	client *redis.Client
}

type commonRedLockOptions struct {
	RetryCount  int
	DriftFactor float64
	TTL         int
	LogError    Log
	LogInfo     Log
}

// RedLockOptions red lock options
type RedLockOptions struct {
	commonRedLockOptions
	clientOptions []*ReditClientOptions
}

// RedLock redis lock
type RedLock struct {
	commonRedLockOptions

	clients []*redis.Client
	quorum  int

	resource string
	val      string
	expiry   int64
}

// NewReditCache ctor
func NewReditCache(options *ReditCacheOptions) *ReditCache {
	cache := new(ReditCache)
	cache.ReditClientOptions = options.ReditClientOptions
	cache.client = redis.NewClient(&redis.Options{
		Addr:     options.Address,
		Password: options.Password,
		DB:       options.DB,
	})
	if options.LogError == nil {
		cache.LogError = func(message string, context interface{}) {}
	}
	if options.LogInfo == nil {
		cache.LogInfo = func(message string, context interface{}) {}
	}
	return cache
}

// NewRedLock create new red lock
func NewRedLock(options *RedLockOptions) *RedLock {
	clients := []*redis.Client{}
	for _, co := range options.clientOptions {
		c := redis.NewClient(&redis.Options{
			Addr:     co.Address,
			Password: co.Password,
			DB:       co.DB,
		})
		clients = append(clients, c)
	}

	rl := new(RedLock)
	rl.clients = clients
	rl.commonRedLockOptions = options.commonRedLockOptions
	return rl
}

// Get gets item from cache
func (cache *ReditCache) Get(ID string) (*Circuit, error) {
	val, err := cache.client.Get(ID).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	res := &Circuit{}
	json.Unmarshal([]byte(val), res)
	return res, nil
}

// Set sets item in cache
func (cache *ReditCache) Set(ID string, circuit *Circuit) error {
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
		ttl := rl.TTL
		success := 0
		//start := time.Now().UnixNano()

		c := make(chan bool, len(rl.clients))
		for _, client := range rl.clients {
			go lockInstance(client, ID, ttl, c)
		}
		for j := 0; j < len(rl.clients); j++ {
			if <-c {
				success++
			}
		}
		close(c)

		//drift := int(float64(ttl)*rl.driftFactor) + 2
		//costTime := (time.Now().UnixNano() - start) / 1e6
		//validityTime := int64(ttl) - costTime - int64(drift)

		quorum := (len(rl.clients) / 2) + 1
		if success >= quorum /*&& validityTime > 0*/ {
			return nil
		}

		rl.unlock(ID)
		time.Sleep(time.Duration(300 * time.Millisecond))
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
