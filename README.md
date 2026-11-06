# circuit-breaker

[![Go Reference](https://pkg.go.dev/badge/github.com/Trysac/u046-circuit-breaker.svg)](https://pkg.go.dev/github.com/Trysac/u046-circuit-breaker)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/Trysac/u046-circuit-breaker/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u046-circuit-breaker/actions/workflows/ci.yml)

Context-first circuit breaker with closed, open and half-open states plus a bulkhead concurrency limiter and metrics snapshot, stdlib only

## Problem

Calling a failing dependency repeatedly exhausts goroutines and connection pools and turns one outage into a cascade. A circuit breaker fails fast while the dependency recovers, and a bulkhead caps concurrent calls. This package provides both, with a context-first API, an injectable clock (`Settings.Now`), correct handling of late results from before a state change (generations), and a `Metrics()` snapshot.

## Install

```sh
go get github.com/Trysac/u046-circuit-breaker
```

Requires Go 1.23 or newer. No dependencies outside the standard library.

## Usage

```go
package main
import ("context"; "fmt"; "time"; circuitbreaker "github.com/Trysac/u046-circuit-breaker")
func main() {
	br, _ := circuitbreaker.New(circuitbreaker.Settings{Name: "db", FailureThreshold: 3})
	bh, _ := circuitbreaker.NewBulkhead(10, 50*time.Millisecond)
	err := bh.Execute(context.Background(), func(ctx context.Context) error {
		return br.Execute(ctx, func(context.Context) error { return nil })
	})
	fmt.Println(err, br.State())
}
```

## API summary

- `type State int` with `StateClosed`, `StateOpen`, `StateHalfOpen`; `String()` returns `closed`, `open` or `half-open`.
- `New(s Settings) (*Breaker, error)`; `(*Breaker).Execute(ctx, fn func(ctx context.Context) error) error`; `(*Breaker).State() State`; `(*Breaker).Metrics() Metrics`.
- `Do[T any](ctx, b *Breaker, fn func(ctx context.Context) (T, error)) (T, error)`; on `ErrOpen` it returns the zero value of `T`.
- `Settings` fields: `Name`, `FailureThreshold` (default 5), `OpenTimeout` (default 30s), `HalfOpenMaxCalls` (default 1), `IsFailure func(error) bool` (default: non-nil error other than `context.Canceled`), `OnStateChange func(name string, from, to State)`, `Now func() time.Time` (default `time.Now`).
- `Metrics` fields: `State`, `Successes`, `Failures`, `Rejections`, `Transitions` (uint64) and `ConsecutiveFailures` (int).
- `NewBulkhead(maxConcurrent int, maxWait time.Duration) (*Bulkhead, error)`; `(*Bulkhead).Execute(ctx, fn) error`; `(*Bulkhead).InFlight() int`.
- Errors (match with `errors.Is`): `ErrOpen`, `ErrTooManyHalfOpenCalls`, `ErrBulkheadFull`, `ErrInvalidSettings`. Invalid settings return an error wrapping or matching `ErrInvalidSettings`. A cancelled context returns `ctx.Err()` without calling `fn`.

## Limits and behaviour

- A panic inside `fn` is counted as one failure; `Execute` re-panics with the same value after accounting.
- Half-open state admits at most `HalfOpenMaxCalls` trial calls; any trial failure reopens the breaker, and `HalfOpenMaxCalls` consecutive successes close it.
- `OnStateChange` is delivered after the internal lock is released, in order. It runs on the goroutine that caused the transition and should not block for long.
- `Name` is limited to 128 bytes.
- The bulkhead is a buffered channel semaphore; `maxWait` of 0 fails a second concurrent call immediately.
- The breaker and the bulkhead compose by nesting `Execute` calls, as shown above.

## Why not sony/gobreaker

[sony/gobreaker](https://github.com/sony/gobreaker) implements a circuit breaker only. It has no bulkhead and no context-first API, so this package provides both in one dependency-free module.

## Security

See [docs/SECURITY.md](docs/SECURITY.md) for the threat model and its limits.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
