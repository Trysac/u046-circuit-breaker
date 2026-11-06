package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const defaultFailureThreshold = 5

const defaultOpenTimeout = 30 * time.Second

const defaultHalfOpenMaxCalls = 1

const maxNameLen = 128

const maxPendingTransitions = 1024

// Error is the base error type of this package. Every sentinel error is a
// *Error with a stable machine-readable code.
type Error struct {
	code string

	message string
}

// Code returns a stable machine-readable identifier for the error.
func (e *Error) Code() string { return e.code }

// Error implements the error interface.
func (e *Error) Error() string { return "circuitbreaker: " + e.message }

// ErrOpen is returned when the breaker is open and rejects a call (code circuit_open).
var ErrOpen error = &Error{code: "circuit_open", message: "circuit breaker is open"}

// ErrTooManyHalfOpenCalls is returned when a half-open breaker already admitted
// HalfOpenMaxCalls trial calls (code too_many_half_open_calls).
var ErrTooManyHalfOpenCalls error = &Error{code: "too_many_half_open_calls", message: "too many half-open trial calls"}

// ErrBulkheadFull is returned when a bulkhead has no free slot within its wait time (code bulkhead_full).
var ErrBulkheadFull error = &Error{code: "bulkhead_full", message: "bulkhead is full"}

// ErrInvalidSettings is returned for invalid constructor settings or nil arguments (code invalid_settings).
var ErrInvalidSettings error = &Error{code: "invalid_settings", message: "invalid settings"}

func invalid(detail string) error {
	return fmt.Errorf("%w: %s", ErrInvalidSettings, detail)
}

// State is the state of a circuit breaker.
type State int

const (
	// StateClosed lets calls through and counts consecutive failures.
	StateClosed State = iota
	// StateOpen rejects calls until the open timeout elapses.
	StateOpen
	// StateHalfOpen admits a limited number of trial calls.
	StateHalfOpen
)

// String returns closed, open or half-open (unknown for other values).
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Settings configures a Breaker. Zero values select the documented defaults.
type Settings struct {
	// Name identifies the breaker in OnStateChange (at most 128 bytes).
	Name string

	// FailureThreshold is the number of consecutive failures that opens a closed breaker (default 5).
	FailureThreshold int

	// OpenTimeout is how long the breaker stays open before becoming half-open (default 30s).
	OpenTimeout time.Duration

	// HalfOpenMaxCalls is the number of trial calls admitted while half-open and
	// the number of consecutive successes that close the breaker (default 1).
	HalfOpenMaxCalls int

	// IsFailure classifies a non-nil error returned by the function. The default
	// treats every error except context.Canceled as a failure.
	IsFailure func(error) bool

	// OnStateChange is called once per transition, after the internal lock is
	// released and in transition order. It must not block for long: while it
	// runs, further transitions are queued (at most 1024, oldest dropped).
	// Panics in the callback are recovered and ignored.
	OnStateChange func(name string, from, to State)

	// Now returns the current time (default time.Now).
	Now func() time.Time
}

// Metrics is a consistent snapshot of a Breaker.
type Metrics struct {
	// State is the state at the time of the snapshot.
	State State

	// Successes counts calls classified as not failed.
	Successes uint64

	// Failures counts calls classified as failed (including panics).
	Failures uint64

	// Rejections counts calls rejected with ErrOpen or ErrTooManyHalfOpenCalls.
	Rejections uint64

	// Transitions counts state changes.
	Transitions uint64

	// ConsecutiveFailures is the failure streak of the current closed generation.
	ConsecutiveFailures int
}

type transition struct {
	from State

	to State
}

// Breaker is a circuit breaker. It is safe for concurrent use.
type Breaker struct {
	name string

	threshold int

	openTimeout time.Duration

	maxHalf int

	isFailure func(error) bool

	onChange func(string, State, State)

	now func() time.Time

	mu sync.Mutex

	state State

	generation uint64

	openedAt time.Time

	consecutive int

	halfAdmitted int

	halfSuccesses int

	successes uint64

	failures uint64

	rejections uint64

	transitions uint64

	pending []transition

	delivering bool
}

func defaultIsFailure(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled)
}

// New validates the settings, applies defaults and returns a closed Breaker.
// Negative values and names longer than 128 bytes return ErrInvalidSettings.
func New(s Settings) (*Breaker, error) {
	if s.FailureThreshold < 0 {
		return nil, invalid("FailureThreshold must not be negative")
	}
	if s.OpenTimeout < 0 {
		return nil, invalid("OpenTimeout must not be negative")
	}
	if s.HalfOpenMaxCalls < 0 {
		return nil, invalid("HalfOpenMaxCalls must not be negative")
	}
	if len(s.Name) > maxNameLen {
		return nil, invalid("Name is too long")
	}
	b := &Breaker{}
	b.name = s.Name
	b.threshold = s.FailureThreshold
	if b.threshold == 0 {
		b.threshold = defaultFailureThreshold
	}
	b.openTimeout = s.OpenTimeout
	if b.openTimeout == 0 {
		b.openTimeout = defaultOpenTimeout
	}
	b.maxHalf = s.HalfOpenMaxCalls
	if b.maxHalf == 0 {
		b.maxHalf = defaultHalfOpenMaxCalls
	}
	b.isFailure = s.IsFailure
	if b.isFailure == nil {
		b.isFailure = defaultIsFailure
	}
	b.onChange = s.OnStateChange
	b.now = s.Now
	if b.now == nil {
		b.now = time.Now
	}
	b.state = StateClosed
	return b, nil
}

// setState performs a transition and starts a new generation. b.mu must be held.
func (b *Breaker) setState(to State) {
	from := b.state
	b.state = to
	b.generation++
	b.transitions++
	b.consecutive = 0
	b.halfAdmitted = 0
	b.halfSuccesses = 0
	if to == StateOpen {
		b.openedAt = b.now()
	}
	if b.onChange != nil {
		if len(b.pending) >= maxPendingTransitions {
			b.pending = b.pending[1:]
		}
		b.pending = append(b.pending, transition{from: from, to: to})
	}
}

// refresh applies the lazy open to half-open transition. b.mu must be held.
func (b *Breaker) refresh() {
	if b.state == StateOpen && b.now().Sub(b.openedAt) >= b.openTimeout {
		b.setState(StateHalfOpen)
	}
}

// deliver sends queued transitions to OnStateChange without holding the lock.
// Only one goroutine delivers at a time, which keeps callbacks ordered and
// allows callbacks to call back into the breaker without deadlocking.
func (b *Breaker) deliver() {
	if b.onChange == nil {
		return
	}
	b.mu.Lock()
	if b.delivering || len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	b.delivering = true
	for len(b.pending) > 0 {
		batch := b.pending
		b.pending = nil
		b.mu.Unlock()
		for _, t := range batch {
			b.notify(t)
		}
		b.mu.Lock()
	}
	b.delivering = false
	b.mu.Unlock()
}

func (b *Breaker) notify(t transition) {
	defer func() { _ = recover() }()
	b.onChange(b.name, t.from, t.to)
}

// State returns the current state, applying the lazy open to half-open rule.
func (b *Breaker) State() State {
	b.mu.Lock()
	b.refresh()
	s := b.state
	b.mu.Unlock()
	b.deliver()
	return s
}

// Metrics returns a consistent snapshot of the counters.
func (b *Breaker) Metrics() Metrics {
	b.mu.Lock()
	b.refresh()
	m := Metrics{}
	m.State = b.state
	m.Successes = b.successes
	m.Failures = b.failures
	m.Rejections = b.rejections
	m.Transitions = b.transitions
	m.ConsecutiveFailures = b.consecutive
	b.mu.Unlock()
	b.deliver()
	return m
}

func (b *Breaker) admit() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh()
	switch b.state {
	case StateOpen:
		b.rejections++
		return 0, ErrOpen
	case StateHalfOpen:
		if b.halfAdmitted >= b.maxHalf {
			b.rejections++
			return 0, ErrTooManyHalfOpenCalls
		}
		b.halfAdmitted++
	}
	return b.generation, nil
}

func (b *Breaker) record(gen uint64, failed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if failed {
		b.failures++
	} else {
		b.successes++
	}
	if gen != b.generation {
		return
	}
	switch b.state {
	case StateClosed:
		if !failed {
			b.consecutive = 0
			return
		}
		b.consecutive++
		if b.consecutive >= b.threshold {
			b.setState(StateOpen)
		}
	case StateHalfOpen:
		if failed {
			b.setState(StateOpen)
			return
		}
		b.halfSuccesses++
		if b.halfSuccesses >= b.maxHalf {
			b.setState(StateClosed)
		}
	}
}

func run(ctx context.Context, fn func(ctx context.Context) error) (panicked bool, value any, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			value = r
		}
	}()
	err = fn(ctx)
	return false, nil, err
}

// Execute runs fn through the breaker.
//
// A context that is already done returns ctx.Err() without calling fn and
// without counting anything. An open breaker returns ErrOpen; a half-open
// breaker that already admitted HalfOpenMaxCalls trials returns
// ErrTooManyHalfOpenCalls. A panic in fn is counted as a failure and then
// re-panicked with the original value. Results of calls admitted in an earlier
// generation only update Successes or Failures. A nil error is never passed to
// IsFailure.
func (b *Breaker) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if b == nil || ctx == nil || fn == nil {
		return invalid("nil breaker, context or function")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	gen, err := b.admit()
	b.deliver()
	if err != nil {
		return err
	}
	panicked, value, err := run(ctx, fn)
	failed := panicked || (err != nil && b.isFailure(err))
	b.record(gen, failed)
	b.deliver()
	if panicked {
		panic(value)
	}
	return err
}

// Do runs fn through the breaker and returns its value. When the breaker
// rejects the call the zero value of T is returned with the rejection error.
func Do[T any](ctx context.Context, b *Breaker, fn func(ctx context.Context) (T, error)) (T, error) {
	var result T
	if fn == nil {
		return result, invalid("nil function")
	}
	err := b.Execute(ctx, func(c context.Context) error {
		r, e := fn(c)
		result = r
		return e
	})
	return result, err
}

// Bulkhead limits the number of concurrent calls. It is safe for concurrent use.
type Bulkhead struct {
	sem chan struct{}

	maxWait time.Duration
}

// NewBulkhead returns a bulkhead admitting maxConcurrent calls at once. A call
// that finds no free slot waits at most maxWait (0 fails immediately).
// maxConcurrent < 1 or maxWait < 0 returns ErrInvalidSettings.
func NewBulkhead(maxConcurrent int, maxWait time.Duration) (*Bulkhead, error) {
	if maxConcurrent < 1 {
		return nil, invalid("maxConcurrent must be at least 1")
	}
	if maxWait < 0 {
		return nil, invalid("maxWait must not be negative")
	}
	h := &Bulkhead{}
	h.sem = make(chan struct{}, maxConcurrent)
	h.maxWait = maxWait
	return h, nil
}

func (h *Bulkhead) acquire(ctx context.Context) error {
	select {
	case h.sem <- struct{}{}:
		return nil
	default:
	}
	if h.maxWait == 0 {
		return ErrBulkheadFull
	}
	timer := time.NewTimer(h.maxWait)
	defer timer.Stop()
	select {
	case h.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ErrBulkheadFull
	}
}

// Execute runs fn when a slot is free. It returns ctx.Err() if the context is
// done (before or while waiting) and ErrBulkheadFull when the wait time
// elapses. The slot is released even if fn panics.
func (h *Bulkhead) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if h == nil || ctx == nil || fn == nil {
		return invalid("nil bulkhead, context or function")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := h.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-h.sem }()
	return fn(ctx)
}

// InFlight returns the number of calls currently holding a slot.
func (h *Bulkhead) InFlight() int {
	return len(h.sem)
}
