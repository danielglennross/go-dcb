package main

import (
	"fmt"
)

func main() {
	fn := func(id string, fh int) (interface{}, error) {
		fmt.Println("Hello World " + id)
		return 5 * fh, nil
	}
	cache := NewMemoryCache()
	options := &Options{
		GracePeriodMs: 500,
		Threshold:     1,
		TimeoutMs:     100,
		BackoffMs:     1000,
		Retry:         3,
	}

	breaker := NewCircuitBreaker(fn, cache, options)
	go func() {
		for {
			select {
			case o := <-breaker.OpenChan:
				fmt.Printf("%s", o)
			case c := <-breaker.ClosedChan:
				fmt.Printf("%s", c)
			case ho := <-breaker.HalfOpenChan:
				fmt.Printf("%s", ho)
			case f := <-breaker.FallbackChan:
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
