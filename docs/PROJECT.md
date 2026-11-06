# Project record

## Objective

Provide a context-first circuit breaker with closed, open and half-open states, a bulkhead concurrency limiter and a metrics snapshot, using only the Go standard library, so that a failing dependency fails fast and concurrent calls are capped.

## Scope

- In scope: the public API documented in [README.md](../README.md), documented and tested. This covers the breaker state machine with generations, `Do`, the bulkhead with `MaxWait`, `Metrics()` and the injectable clock.
- Out of scope: sliding-window failure rates, Prometheus or other metrics exporters, retries, adaptive concurrency, and anything that needs dependencies or I/O without an accepted ADR.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from the factory utility template. |

## Effort metrics

Measured values only; a value that has not been measured stays "pending measurement".

| Metric | Value |
|---|---|
| Tokens (input / output) | 112053 / 53370 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.4057 |

## Factory run u046 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | claude-sonnet-5-5 | yes | 2417 | 42906 | 0 | 4285 | 0.2174 |
| audit | claude-sonnet-5-5 | no | 8 | 2341 | 37173 | 67168 | 0.1231 |
| fix-1 | claude-sonnet-5-5 | no | 6 | 1466 | 17370 | 30865 | 0.0612 |
| docs | claude-haiku-5-5 | no | 7304 | 6657 | 0 | 0 | 0.0041 |

Total: $0.4057. Prices: Anthropic list prices.
