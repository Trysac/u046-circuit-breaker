# Security notes

## Threat model

`github.com/Trysac/u046-circuit-breaker` is a pure library without I/O.

Defended:

- **Supply chain**: standard library only, so there is no third-party module to compromise. `go.sum` verification, `govulncheck` in the local gates, Dependabot weekly updates, actions pinned by commit SHA, minimal workflow permissions.
- **Leaked secrets**: `gitleaks` runs in the local gates and in CI; `.env` files are git-ignored.
- **Untrusted input**: invalid settings return `ErrInvalidSettings`; `Name` is limited to 128 bytes; a nil breaker, context or function is rejected. The code does not use `unsafe` or `os/exec`.
- **Callback failures**: a panicking `OnStateChange` callback is recovered and ignored, so it cannot break the breaker's accounting.
- **Memory growth from callbacks**: the callback queue is capped at 1024 entries; the oldest entries are dropped when full.
- **Panics in `fn`**: counted as failures and re-raised to the caller after accounting.

Not defended:

- The library does not enforce a timeout on `fn`; a slow or hanging call holds its bulkhead slot until the caller's context ends.
- A blocking `OnStateChange` callback blocks the goroutine that caused the transition. The library does not bound its duration.
- Dropped callback entries (queue overflow) are lost; callers that need every transition must not rely on the callback alone and should use `Metrics()`.
- The `Now` function is trusted; a faulty clock changes open and half-open timing.
- No rate limiting or admission control beyond the breaker and bulkhead limits.

## Publishing

Releases are git tags served through the Go module proxy and checksum database. No registry token is stored in the repository.

To report a vulnerability, see [SECURITY.md](../SECURITY.md).
