package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// ReditCacheOptions cache options
type ReditCacheOptions struct {
	Address  string
	Password string
	DB       int
}

// ReditCache default memory cache
type ReditCache struct {
	client *redis.Client
	ttl    int
}

type commonRedLockOptions struct {
	retryCount  int
	retrydelay  int
	driftFactor float64
	ttl         int
}

// RedLockOptions red lock options
type RedLockOptions struct {
	commonRedLockOptions
	clientOptions []ReditCacheOptions
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
	cache.ttl = 0
	cache.client = redis.NewClient(&redis.Options{
		Addr:     options.Address,
		Password: options.Password,
		DB:       options.DB,
	})
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
	if err != nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, nil
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
	ttl := time.Millisecond * time.Duration(cache.ttl)
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
		_, err := client.Eval("SET KEYS[1] ARGV[1] NX PX ARGV[2]", []string{ID}, ID, ttl).Result()
		c <- err != nil
	}

	for i := 0; i < rl.retryCount; i++ {
		ttl := rl.ttl
		success := 0
		start := time.Now().UnixNano()

		c := make(chan bool, len(rl.clients))
		for _, client := range rl.clients {
			go lockInstance(client, ID, ttl, c)
		}
		for e := range c {
			if e {
				success++
			}
		}

		drift := int(float64(ttl)*rl.driftFactor) + 2
		costTime := (time.Now().UnixNano() - start) / 1e6
		validityTime := int64(ttl) - costTime - int64(drift)

		if success >= (len(rl.clients)/2)+1 && validityTime > 0 {
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

		client.Eval(`
			if redis.call("get", KEYS[1]) == ARGV[1] then
					return redis.call("del", KEYS[1])
			else
					return 0
			end`,
			[]string{ID}, ID).Result()
	}

	for _, client := range rl.clients {
		wg.Add(1)
		go unlockInstance(client, ID)
	}

	wg.Wait()
}
