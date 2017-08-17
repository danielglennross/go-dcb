package main

import (
	"fmt"

	c "github.com/danielglennross/go-dcb/cache"
)

func main() {
	fn := func(id string, fh int) (int, error) {
		fmt.Println("Hello World " + id)
		return 5 * fh, nil
	}
	//cache := NewMemoryCache()
	cache := c.NewReditCache(&c.ReditCacheOptions{
		ReditClientOptions: &c.ReditClientOptions{
			Address:  "localhost:6379",
			Password: "",
			DB:       0,
		},
		TTL: 1000 * 100,
	})
	lock := c.NewRedLock(&c.RedLockOptions{
		CommonRedLockOptions: c.CommonRedLockOptions{
			RetryCount:   3,
			RetryDelayMs: 300,
			DriftFactor:  0.01,
			TTLms:        1000 * 100,
		},
		ClientOptions: []*c.ReditClientOptions{
			&c.ReditClientOptions{
				Address:  "localhost:6379",
				Password: "",
				DB:       0,
			},
		},
	})
	options := &Options{
		GracePeriodMs: 500,
		Threshold:     1,
		TimeoutMs:     1,
		BackoffMs:     1000,
		Retry:         3,
	}

	breaker, err := NewCircuitBreaker(fn, cache, lock, options)
	go func() {
		for {
			select {
			case o, ok := <-breaker.OpenChan:
				if !ok {
					return
				}
				fmt.Printf("%s", o)
			case c, ok := <-breaker.ClosedChan:
				if !ok {
					return
				}
				fmt.Printf("%s", c)
			case ho, ok := <-breaker.HalfOpenChan:
				if !ok {
					return
				}
				fmt.Printf("%s", ho)
			case f, ok := <-breaker.FallbackChan:
				if !ok {
					return
				}
				fmt.Printf("%s", f)
			}
		}
	}()

	res, err := breaker.Fire("myFn", "daniel", 2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("%v", res.(int))
	}

	breaker.Destroy()
}
