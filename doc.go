// Package circuitbreaker provides a context-first circuit breaker and a
// bulkhead concurrency limiter using only the standard library.
//
// A [Breaker] is always in one of three states. While closed it lets calls
// through and counts consecutive failures; reaching FailureThreshold opens it.
// While open it rejects calls with [ErrOpen] until OpenTimeout has elapsed on
// the injected clock, after which the next call or State query moves it to
// half-open. While half-open it admits at most HalfOpenMaxCalls trial calls,
// rejecting the rest with [ErrTooManyHalfOpenCalls]; any failed trial reopens
// the breaker and HalfOpenMaxCalls consecutive successes close it.
//
// Every state transition starts a new generation. A call remembers the
// generation in which it was admitted; if it finishes after the breaker has
// changed state, only the Successes or Failures counter is updated, so a late
// result from before an outage cannot close or reopen the breaker.
//
// State-change callbacks are queued while the internal lock is held and
// delivered after it is released, in order, so a callback may call State,
// Metrics or Execute without deadlocking. Callbacks should return quickly.
//
// A [Bulkhead] caps concurrent calls with a semaphore. A breaker and a
// bulkhead compose by nesting Execute calls.
package circuitbreaker
