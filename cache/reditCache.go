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

// ReditCache default memory cache
type ReditCache struct {
	logError schema.Log
	logInfo  schema.Log
	ttl      int
	client   *redis.Client
}

// RedLock redis lock
type RedLock struct {
	retryCount   int
	retryDelayMs int
	driftFactor  float64
	ttlMs        int
	logError     schema.Log
	logInfo      schema.Log
	clients      []*redis.Client
}

type redLockOption func(*RedLock)

// ClientOption redis client option
type ClientOption struct {
	Address  string
	Password string
	DB       int
}

// RetryCount rery count
func RetryCount(r int) redLockOption {
	return func(rc *RedLock) {
		rc.retryCount = r
	}
}

// RetryDelayMs in milliseconds
func RetryDelayMs(r int) redLockOption {
	return func(rc *RedLock) {
		rc.retryDelayMs = r
	}
}

// DriftFactor drift factor
func DriftFactor(df float64) redLockOption {
	return func(rc *RedLock) {
		rc.driftFactor = df
	}
}

// TTLms to live in milliseconds
func TTLms(ttlMs int) redLockOption {
	return func(rc *RedLock) {
		rc.ttlMs = ttlMs
	}
}

// RedLockLogError log error delegate
func RedLockLogError(le schema.Log) redLockOption {
	return func(rc *RedLock) {
		rc.logError = le
	}
}

// RedLockLogInfo log info delegate
func RedLockLogInfo(li schema.Log) redLockOption {
	return func(rc *RedLock) {
		rc.logInfo = li
	}
}

type reditCacheOption func(*ReditCache)

// TTL time to live in milliseconds
func TTL(ttl int) reditCacheOption {
	return func(rc *ReditCache) {
		rc.ttl = ttl
	}
}

// CacheLogError log error delegate
func CacheLogError(le schema.Log) reditCacheOption {
	return func(rc *ReditCache) {
		rc.logError = le
	}
}

// CacheLogInfo log info delegate
func CacheLogInfo(li schema.Log) reditCacheOption {
	return func(rc *ReditCache) {
		rc.logInfo = li
	}
}

// NewReditCache ctor
func NewReditCache(client ClientOption, options ...reditCacheOption) *ReditCache {
	cache := new(ReditCache)
	cache.client = redis.NewClient(&redis.Options{
		Addr:     client.Address,
		Password: client.Password,
		DB:       client.DB,
	})

	cache.ttl = 500
	cache.logError = func(message string, context interface{}) {}
	cache.logInfo = func(message string, context interface{}) {}

	for _, opt := range options {
		opt(cache)
	}

	return cache
}

// NewRedLock create new red lock
func NewRedLock(clients []ClientOption, options ...redLockOption) *RedLock {
	rl := new(RedLock)

	cls := []*redis.Client{}
	for _, co := range clients {
		c := redis.NewClient(&redis.Options{
			Addr:     co.Address,
			Password: co.Password,
			DB:       co.DB,
		})
		cls = append(cls, c)
	}
	rl.clients = cls

	rl.retryDelayMs = 300
	rl.retryDelayMs = 3
	rl.driftFactor = 0.01
	rl.ttlMs = 500
	rl.logError = func(message string, context interface{}) {}
	rl.logInfo = func(message string, context interface{}) {}

	for _, opt := range options {
		opt(rl)
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
		_, err := client.Eval(lockScript, []string{ID}, ID, strconv.Itoa(ttl)).Result()
		c <- err == nil
	}

	for i := 0; i < rl.retryCount; i++ {
		ttlMs := rl.ttlMs
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

		driftMs := int(float64(ttlMs)*rl.driftFactor) + 2
		costTimeMs := (time.Now().UnixNano() - startNs) / 1e6
		validityTime := int64(ttlMs) - costTimeMs - int64(driftMs)

		quorum := (len(rl.clients) / 2) + 1
		if success >= quorum && validityTime > 0 {
			return nil
		}

		rl.unlock(ID)
		time.Sleep(time.Duration(rl.retryDelayMs) * time.Millisecond)
	}

	return fmt.Errorf("Failed to lock for %s", ID)
}

func (rl *RedLock) unlock(ID string) {
	var wg sync.WaitGroup

	unlockInstance := func(client *redis.Client, ID string) {
		defer wg.Done()

		_, err := client.Eval(unlockScript, []string{ID}, ID).Result()

		rl.logError(fmt.Sprintf("Could not unlock ID %s", ID), err)
	}

	wg.Add(len(rl.clients))
	for _, client := range rl.clients {
		go unlockInstance(client, ID)
	}

	wg.Wait()
}
