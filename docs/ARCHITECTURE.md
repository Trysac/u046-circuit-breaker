# Architecture

## Purpose

`github.com/Trysac/u046-circuit-breaker` is a small, dependency-free Go library: a context-first circuit breaker with closed, open and half-open states, a bulkhead concurrency limiter and a metrics snapshot, using only the standard library.

## Shape

```text
caller code --> package circuitbreaker (repository root)
                    |
                    +-- Breaker: state machine with generations, guarded by a mutex
                    +-- Bulkhead: buffered channel semaphore with MaxWait timer
                    +-- callback queue: transitions delivered after unlock, in order
```

- **Public API**: every exported identifier of the root package. Unexported identifiers may change without a major version.
- **Generations**: every state transition starts a new generation. A call admitted in an earlier generation that finishes later is counted only in `Successes` or `Failures`; it cannot change the current state.
- **Callbacks**: transitions are queued under the state mutex and delivered after unlocking. The queue is capped at 1024 entries; the oldest entry is dropped when full. A callback that panics is recovered and ignored.
- **Errors**: invalid input returns an error; sentinel errors (`ErrOpen`, `ErrTooManyHalfOpenCalls`, `ErrBulkheadFull`, `ErrInvalidSettings`) let callers branch with `errors.Is`. Nil breaker, nil context or nil function return `ErrInvalidSettings`.
- **Side effects**: none beyond calling the caller's function and callback. Time comes from `Settings.Now`.

## Quality contract

`scripts/check` is the single definition of the gates; `fabrica.json` tells the factory and CI how to run it. See [TESTING.md](TESTING.md).

## Decisions

Architecture decisions are recorded in [decisions/](decisions/) as numbered ADRs.
