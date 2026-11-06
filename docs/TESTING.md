# Testing

## Running

```sh
go test -cover .          # tests with coverage summary
go run ./scripts/check    # every gate, as CI and the factory run it
```

## Policy

- Statement coverage of the library (every package except `scripts/`) must stay at or above 80 %; the gate fails below it.
- Tests live in package `circuitbreaker_test` and exercise the public API only. They assert return values, returned errors (`errors.Is`) and their messages.
- The breaker is tested with an injected fake clock through `Settings.Now`. Goroutine interaction uses channels. The only real timer is a `MaxWait` of 20 ms, asserted to return under 1 s.
- The race detector runs whenever cgo and a C compiler are available (always in Linux CI); the gate prints why when it has to run without it.
- `example_test.go` (package `circuitbreaker_test`) holds the runnable `Example` functions, which `go test` verifies through their `// Output:` comments.
- Each bug fix starts with a failing test that reproduces it.
- Tests are deterministic: no network, no dependence on test order.

## Layout

`<file>_test.go` sits next to the file it tests. Fixtures and golden files go in `testdata/`. `doc.go` documents the states and generations.
