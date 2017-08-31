package main

import (
	"fmt"
	"time"

	c "github.com/danielglennross/go-dcb/cache"
)

func main() {
	fn := func(id string, fh int) (int, error) {
		fmt.Println("Hello World " + id)
		return 5 * fh, nil
	}

	// cache := cache.NewMemoryCache()

	cache := c.NewRedisCache(
		c.ClientOption{
			Address:  "localhost:6379",
			Password: "",
			DB:       0,
		},
		c.TTL(1000*100),
	)

	lock := c.NewRedLock(
		[]c.ClientOption{
			c.ClientOption{
				Address:  "localhost:6379",
				Password: "",
				DB:       0,
			},
		},
		c.RetryCount(3),
		c.RetryDelayMs(300),
		c.DriftFactor(0.01),
		c.TTLms(1000*100),
	)

	backoff, _ := NewExponential(
		&ExponentialOptions{
			Min:    300 * time.Millisecond,
			Max:    10 * time.Second,
			Factor: 2,
		},
	)

	dynamicBreaker, err := NewCircuitBreakerDynamic(
		fn,
		cache,
		lock,
		GracePeriodMs(500),
		Threshold(1),
		TimeoutMs(1000),
		BackoffMs(backoff),
		Retry(3),
	)

	if err != nil {
		fmt.Println(err)
	}

	dynamicBreaker.OnClosed(func(ID string) { fmt.Printf("%s", ID) })
	dynamicBreaker.OnFallback(func(ID string) { fmt.Printf("%s", ID) })
	dynamicBreaker.OnOpen(func(ID string) { fmt.Printf("%s", ID) })
	dynamicBreaker.OnHalfOpen(func(ID string) { fmt.Printf("%s", ID) })

	res1, err := dynamicBreaker.Fire("myFn", "daniel", 2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("%v", res1.(int))
	}

	dynamicBreaker.Destroy()

	// ... //

	staticBreaker, err := NewCircuitBreaker(
		cache,
		lock,
		GracePeriodMs(500),
		Threshold(1),
		TimeoutMs(1000),
		BackoffMs(backoff),
		Retry(3),
	)

	if err != nil {
		fmt.Println(err)
	}

	staticBreaker.OnClosed(func(ID string) { fmt.Printf("%s", ID) })
	staticBreaker.OnFallback(func(ID string) { fmt.Printf("%s", ID) })
	staticBreaker.OnOpen(func(ID string) { fmt.Printf("%s", ID) })
	staticBreaker.OnHalfOpen(func(ID string) { fmt.Printf("%s", ID) })

	res2, err := staticBreaker.Fire("myFn", func() (interface{}, error) {
		fmt.Println("Hello World")
		return 10, nil
	})

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("%v", res2.(int))
	}

	staticBreaker.Destroy()
}
