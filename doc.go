// Package circuitbreaker is a small, dependency-free Go library: Context-first circuit breaker with closed, open and half-open states plus a bulkhead concurrency limiter and metrics snapshot, stdlib only
//
// The public API is every exported identifier of this root package. Functions are pure: they do
// no I/O, keep no global mutable state and report invalid input through returned errors.
package circuitbreaker
