# go-dcb
A distributed circuit breaker implemented in Go.

# example
A distributed circuit breaker using a Redis cache & Redlock sync mechanism.

```go
// ignoring errors for purpose of example

fn := func(name string) (string, error) {
  msg := fmt.Sprintf("%s, Hello World", name)
  return msg, nil
}

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

breaker, _ := NewCircuitBreaker(
  fn,
  cache,
  lock,
  GracePeriodMs(500),
  Threshold(1),
  TimeoutMs(1000),
  BackoffMs(backoff),
  Retry(3),
)

breaker.OnClosed(func(ID string) { fmt.Printf("%s", ID) })
breaker.OnFallback(func(ID string) { fmt.Printf("%s", ID) })
breaker.OnOpen(func(ID string) { fmt.Printf("%s", ID) })
breaker.OnHalfOpen(func(ID string) { fmt.Printf("%s", ID) })

res, _ := breaker.Fire("myFnId", "daniel")
fmt.Printf("%v", res.(string))

breaker.Destroy()
```
