# Generation-based circuit breaker with post-unlock ordered callbacks and a semaphore bulkhead

Status: accepted (generated, pending human review)

## Context

Calling a failing dependency repeatedly exhausts goroutines and pools. Existing libraries such as sony/gobreaker have no bulkhead and no context-first API, and naive breakers miscount results that arrive after a state change or deadlock when a state-change callback calls back into the breaker.

## Decision

The breaker keeps a generation counter that starts anew on every transition; each admitted call remembers its generation and a stale result only updates the Successes or Failures counters. The open to half-open transition is lazy and based on an injectable clock. Transitions are queued under the state mutex and delivered after unlocking by a single draining goroutine at a time, in order, with a capped queue, so callbacks can call State, Metrics or Execute without deadlock. Panics in fn are counted as failures and re-panicked. The bulkhead is a buffered-channel semaphore with a stopped-on-every-path timer for the wait. Sentinel errors are values of one typed *Error carrying a stable code, so errors.Is works. Breaker and bulkhead compose by nesting Execute.

## Consequences

Late results cannot corrupt state and tests are deterministic with a fake clock. Callbacks can be delivered by another goroutine than the one that caused the transition, and a slow callback delays later callbacks; the queue is capped at 1024 entries with the oldest dropped. Callback panics are swallowed. Sliding-window failure rates, exporters, retries and adaptive concurrency are out of scope.
