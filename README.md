# go-dcb
A distributed circuit breaker implemented in Go.

# example
A distributed circuit breaker(s) using a Redis cache & Redlock sync mechanism.

Creating a distributed cache & lock strategy:

```go
// ignoring errors for purpose of example

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
```

Setting circuit breaker policies:

```go
// ignoring errors for purpose of example

backoff, _ := policies.NewExponential(
  policies.Min(300*time.Millisecond),
  policies.Max(10*time.Second),
  policies.Factor(2),
)
```

Creating a circuit breaker:

A circuit breaker is initialised with a cache & lock strategy, as well as various configuration options.

The circuit breaker is fired with an ID and function matching the signature: `func() (interface{}, error)`.

```go
// ignoring errors for purpose of example

breaker, err := NewCircuitBreaker(
  cache,
  lock,
  GracePeriodMs(500),
  Threshold(1),
  TimeoutMs(1000),
  BackoffMs(backoff),
  Retry(3),
)

res, _ := breaker.Fire("myFnId", func() (interface{}, error) {
  return "Daniel, Hello World"
})
fmt.Printf("%v", res.(string))

breaker.Destroy()
```

Creating a dynamic circuit breaker:

A dynamic circuit breaker is initialised with a function, a cache & lock strategy, as well as various configuration options.

The wrapped function can have any number of input paramter types (including none).
However, it must return two types that can be respectfully cast to `(interface{}, error)`.

The dynamic circuit breaker is fired with an ID and a spread of all the arguments required by the wrapped function.

```go
// ignoring errors for purpose of example

fn := func(name string) (string, error) {
  msg := fmt.Sprintf("%s, Hello World", name)
  return msg, nil
}

breaker, _ := NewCircuitBreakerDynamic(
  fn,
  cache,
  lock,
  GracePeriodMs(500),
  Threshold(1),
  TimeoutMs(1000),
  BackoffMs(backoff),
  Retry(3),
)

res, _ := breaker.Fire("myFnId", "Daniel")
fmt.Printf("%v", res.(string))

breaker.Destroy()
```

Handling circuit breaker events:

```go
breaker.OnClosed(func(ID string) { fmt.Printf("%s", ID) })
breaker.OnFallback(func(ID string) { fmt.Printf("%s", ID) })
breaker.OnOpen(func(ID string) { fmt.Printf("%s", ID) })
breaker.OnHalfOpen(func(ID string) { fmt.Printf("%s", ID) })
```