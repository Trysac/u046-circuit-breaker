package circuitbreaker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Trysac/u046-circuit-breaker"
)

var errBoom = errors.New("downstream failed")

var errBenign = errors.New("benign")

type clock struct {
	mu sync.Mutex

	now time.Time
}

func newClock() *clock {
	return &clock{now: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func failFn(context.Context) error { return errBoom }

func okFn(context.Context) error { return nil }

func mustBreaker(t *testing.T, s circuitbreaker.Settings) *circuitbreaker.Breaker {
	t.Helper()
	b, err := circuitbreaker.New(s)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	return b
}

func startBlocked(t *testing.T, b *circuitbreaker.Breaker) (func(), <-chan error) {
	t.Helper()
	started := make(chan struct{})
	gate := make(chan struct{})
	out := make(chan error, 1)
	go func() {
		out <- b.Execute(context.Background(), func(context.Context) error {
			close(started)
			<-gate
			return nil
		})
	}()
	<-started
	return func() { close(gate) }, out
}

func holdSlot(t *testing.T, h *circuitbreaker.Bulkhead) (func(), <-chan error) {
	t.Helper()
	started := make(chan struct{})
	gate := make(chan struct{})
	out := make(chan error, 1)
	go func() {
		out <- h.Execute(context.Background(), func(context.Context) error {
			close(started)
			<-gate
			return nil
		})
	}()
	<-started
	return func() { close(gate) }, out
}

func TestOpensAfterConsecutiveFailures(t *testing.T) {
	calls := 0
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 3})
	fn := func(context.Context) error {
		calls++
		return errBoom
	}
	for i := 0; i < 3; i++ {
		if err := b.Execute(context.Background(), fn); !errors.Is(err, errBoom) {
			t.Fatalf("call %d error = %v, want errBoom", i, err)
		}
	}
	if got := b.State().String(); got != "open" {
		t.Fatalf("State = %q, want open", got)
	}
	if err := b.Execute(context.Background(), fn); !errors.Is(err, circuitbreaker.ErrOpen) {
		t.Fatalf("4th call error = %v, want ErrOpen", err)
	}
	if calls != 3 {
		t.Fatalf("fn called %d times, want 3", calls)
	}
}

func TestThresholdOne(t *testing.T) {
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1})
	_ = b.Execute(context.Background(), failFn)
	if b.State() != circuitbreaker.StateOpen {
		t.Fatalf("State = %v, want open", b.State())
	}
}

func TestDefaults(t *testing.T) {
	clk := newClock()
	b := mustBreaker(t, circuitbreaker.Settings{Now: clk.Now})
	for i := 0; i < 4; i++ {
		_ = b.Execute(context.Background(), failFn)
	}
	if b.State() != circuitbreaker.StateClosed {
		t.Fatalf("State after 4 failures = %v, want closed", b.State())
	}
	_ = b.Execute(context.Background(), failFn)
	if b.State() != circuitbreaker.StateOpen {
		t.Fatalf("State after 5 failures = %v, want open", b.State())
	}
	clk.Advance(29 * time.Second)
	if b.State() != circuitbreaker.StateOpen {
		t.Fatalf("State at +29s = %v, want open", b.State())
	}
	clk.Advance(time.Second)
	if b.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("State at +30s = %v, want half-open", b.State())
	}
	if err := b.Execute(context.Background(), okFn); err != nil {
		t.Fatalf("trial error = %v", err)
	}
	if b.State() != circuitbreaker.StateClosed {
		t.Fatalf("State after trial = %v, want closed", b.State())
	}
}

func TestStateString(t *testing.T) {
	cases := map[circuitbreaker.State]string{
		circuitbreaker.StateClosed:   "closed",
		circuitbreaker.StateOpen:     "open",
		circuitbreaker.StateHalfOpen: "half-open",
		circuitbreaker.State(99):     "unknown",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Fatalf("State(%d).String() = %q, want %q", int(s), got, want)
		}
	}
}

func TestTimeoutHalfOpenAndClose(t *testing.T) {
	clk := newClock()
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1, OpenTimeout: 30 * time.Second, HalfOpenMaxCalls: 2, Now: clk.Now})
	_ = b.Execute(context.Background(), failFn)
	clk.Advance(29 * time.Second)
	if err := b.Execute(context.Background(), okFn); !errors.Is(err, circuitbreaker.ErrOpen) {
		t.Fatalf("error at +29s = %v, want ErrOpen", err)
	}
	clk.Advance(time.Second)
	if b.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("State at +30s = %v, want half-open", b.State())
	}
	rel1, done1 := startBlocked(t, b)
	rel2, done2 := startBlocked(t, b)
	if err := b.Execute(context.Background(), okFn); !errors.Is(err, circuitbreaker.ErrTooManyHalfOpenCalls) {
		t.Fatalf("third trial error = %v, want ErrTooManyHalfOpenCalls", err)
	}
	rel1()
	rel2()
	if err := <-done1; err != nil {
		t.Fatalf("trial 1 error = %v", err)
	}
	if err := <-done2; err != nil {
		t.Fatalf("trial 2 error = %v", err)
	}
	m := b.Metrics()
	if m.State != circuitbreaker.StateClosed {
		t.Fatalf("State = %v, want closed", m.State)
	}
	if m.Transitions != 3 {
		t.Fatalf("Transitions = %d, want 3", m.Transitions)
	}
	if m.Rejections != 2 {
		t.Fatalf("Rejections = %d, want 2", m.Rejections)
	}
}

func TestFailingTrialReopens(t *testing.T) {
	clk := newClock()
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1, OpenTimeout: 30 * time.Second, HalfOpenMaxCalls: 2, Now: clk.Now})
	_ = b.Execute(context.Background(), failFn)
	clk.Advance(30 * time.Second)
	if err := b.Execute(context.Background(), failFn); !errors.Is(err, errBoom) {
		t.Fatalf("trial error = %v, want errBoom", err)
	}
	if b.State() != circuitbreaker.StateOpen {
		t.Fatalf("State = %v, want open", b.State())
	}
	if err := b.Execute(context.Background(), okFn); !errors.Is(err, circuitbreaker.ErrOpen) {
		t.Fatalf("error after reopen = %v, want ErrOpen", err)
	}
	if got := b.Metrics().Transitions; got != 3 {
		t.Fatalf("Transitions = %d, want 3", got)
	}
	clk.Advance(30 * time.Second)
	if b.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("State = %v, want half-open after second timeout", b.State())
	}
}

func TestCancelledIsNotAFailureByDefault(t *testing.T) {
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1})
	wrapped := fmt.Errorf("call aborted: %w", context.Canceled)
	err := b.Execute(context.Background(), func(context.Context) error { return wrapped })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if b.State() != circuitbreaker.StateClosed {
		t.Fatalf("State = %v, want closed", b.State())
	}
	if got := b.Metrics().Failures; got != 0 {
		t.Fatalf("Failures = %d, want 0", got)
	}
}

func TestCustomIsFailure(t *testing.T) {
	isFailure := func(err error) bool { return err != nil && !errors.Is(err, errBenign) }
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1, IsFailure: isFailure})
	_ = b.Execute(context.Background(), func(context.Context) error { return errBenign })
	if b.State() != circuitbreaker.StateClosed {
		t.Fatalf("State = %v, want closed", b.State())
	}
	_ = b.Execute(context.Background(), failFn)
	if b.State() != circuitbreaker.StateOpen {
		t.Fatalf("State = %v, want open", b.State())
	}
}

func TestSuccessResetsConsecutiveFailures(t *testing.T) {
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 3})
	_ = b.Execute(context.Background(), failFn)
	_ = b.Execute(context.Background(), failFn)
	if got := b.Metrics().ConsecutiveFailures; got != 2 {
		t.Fatalf("ConsecutiveFailures = %d, want 2", got)
	}
	_ = b.Execute(context.Background(), okFn)
	if got := b.Metrics().ConsecutiveFailures; got != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want 0", got)
	}
}

func TestAlreadyCancelledContext(t *testing.T) {
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := b.Metrics()
	called := false
	err := b.Execute(ctx, func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("fn must not be called")
	}
	if after := b.Metrics(); after != before {
		t.Fatalf("Metrics changed: %+v -> %+v", before, after)
	}
}

func TestPanicCountsAsFailureAndRepanics(t *testing.T) {
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 2})
	var got any
	func() {
		defer func() { got = recover() }()
		_ = b.Execute(context.Background(), func(context.Context) error { panic("boom") })
	}()
	if got != "boom" {
		t.Fatalf("recovered %v, want boom", got)
	}
	m := b.Metrics()
	if m.Failures != 1 || m.ConsecutiveFailures != 1 {
		t.Fatalf("Metrics = %+v, want one failure", m)
	}
}

func TestStaleSuccessDoesNotChangeState(t *testing.T) {
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 2})
	release, done := startBlocked(t, b)
	_ = b.Execute(context.Background(), failFn)
	_ = b.Execute(context.Background(), failFn)
	before := b.Metrics()
	if before.State != circuitbreaker.StateOpen {
		t.Fatalf("State = %v, want open", before.State)
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("slow call error = %v", err)
	}
	after := b.Metrics()
	if after.State != circuitbreaker.StateOpen {
		t.Fatalf("State = %v, want open", after.State)
	}
	if after.ConsecutiveFailures != before.ConsecutiveFailures {
		t.Fatalf("ConsecutiveFailures changed: %d -> %d", before.ConsecutiveFailures, after.ConsecutiveFailures)
	}
	if after.Successes != 1 || after.Failures != 2 || after.Transitions != before.Transitions {
		t.Fatalf("Metrics = %+v", after)
	}
}

func TestStaleSuccessDoesNotCloseHalfOpen(t *testing.T) {
	clk := newClock()
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1, OpenTimeout: 10 * time.Second, Now: clk.Now})
	release, done := startBlocked(t, b)
	_ = b.Execute(context.Background(), failFn)
	clk.Advance(10 * time.Second)
	if b.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("State = %v, want half-open", b.State())
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("slow call error = %v", err)
	}
	m := b.Metrics()
	if m.State != circuitbreaker.StateHalfOpen || m.Transitions != 2 {
		t.Fatalf("Metrics = %+v, want half-open with 2 transitions", m)
	}
}

func TestStaleFailureDoesNotReopen(t *testing.T) {
	clk := newClock()
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1, OpenTimeout: 10 * time.Second, Now: clk.Now})
	gate := make(chan struct{})
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- b.Execute(context.Background(), func(context.Context) error {
			close(started)
			<-gate
			return errBoom
		})
	}()
	<-started
	_ = b.Execute(context.Background(), failFn)
	clk.Advance(10 * time.Second)
	_ = b.State()
	close(gate)
	if err := <-done; !errors.Is(err, errBoom) {
		t.Fatalf("error = %v, want errBoom", err)
	}
	m := b.Metrics()
	if m.State != circuitbreaker.StateHalfOpen || m.Failures != 2 || m.Transitions != 2 {
		t.Fatalf("Metrics = %+v", m)
	}
}

func TestOnStateChange(t *testing.T) {
	clk := newClock()
	var b *circuitbreaker.Breaker
	var mu sync.Mutex
	var got []string
	cb := func(name string, from, to circuitbreaker.State) {
		_ = b.State()
		_ = b.Metrics()
		mu.Lock()
		defer mu.Unlock()
		got = append(got, fmt.Sprintf("%s %s>%s", name, from, to))
	}
	b = mustBreaker(t, circuitbreaker.Settings{Name: "db", FailureThreshold: 1, OpenTimeout: time.Second, Now: clk.Now, OnStateChange: cb})
	_ = b.Execute(context.Background(), failFn)
	mu.Lock()
	if len(got) != 1 || got[0] != "db closed>open" {
		t.Fatalf("callbacks = %v, want [db closed>open]", got)
	}
	mu.Unlock()
	clk.Advance(time.Second)
	_ = b.Execute(context.Background(), okFn)
	mu.Lock()
	defer mu.Unlock()
	want := []string{"db closed>open", "db open>half-open", "db half-open>closed"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("callbacks = %v, want %v", got, want)
	}
}

func TestCallbackMayCallExecute(t *testing.T) {
	var b *circuitbreaker.Breaker
	var mu sync.Mutex
	var inner error
	cb := func(string, circuitbreaker.State, circuitbreaker.State) {
		err := b.Execute(context.Background(), okFn)
		mu.Lock()
		inner = err
		mu.Unlock()
	}
	b = mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1, OnStateChange: cb})
	_ = b.Execute(context.Background(), failFn)
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(inner, circuitbreaker.ErrOpen) {
		t.Fatalf("inner error = %v, want ErrOpen", inner)
	}
	if got := b.Metrics().Rejections; got != 1 {
		t.Fatalf("Rejections = %d, want 1", got)
	}
}

func TestCallbackPanicIsIgnored(t *testing.T) {
	cb := func(string, circuitbreaker.State, circuitbreaker.State) { panic("callback bug") }
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1, OnStateChange: cb})
	if err := b.Execute(context.Background(), failFn); !errors.Is(err, errBoom) {
		t.Fatalf("error = %v, want errBoom", err)
	}
	if b.State() != circuitbreaker.StateOpen {
		t.Fatalf("State = %v, want open", b.State())
	}
}

func TestPendingTransitionsAreCapped(t *testing.T) {
	clk := newClock()
	entered := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	first := true
	var got []string
	cb := func(name string, from, to circuitbreaker.State) {
		mu.Lock()
		isFirst := first
		first = false
		got = append(got, fmt.Sprintf("%s>%s", from, to))
		mu.Unlock()
		if isFirst {
			close(entered)
			<-release
		}
	}
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1, OpenTimeout: time.Second, Now: clk.Now, OnStateChange: cb})
	done := make(chan error, 1)
	go func() { done <- b.Execute(context.Background(), failFn) }()
	<-entered
	for i := 0; i < 1100; i++ {
		clk.Advance(time.Second)
		_ = b.State()
		_ = b.Execute(context.Background(), failFn)
	}
	close(release)
	if err := <-done; !errors.Is(err, errBoom) {
		t.Fatalf("error = %v, want errBoom", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1025 {
		t.Fatalf("delivered %d callbacks, want 1025", len(got))
	}
	if last := got[len(got)-1]; last != "half-open>open" {
		t.Fatalf("last callback = %q, want half-open>open", last)
	}
	if m := b.Metrics(); m.Transitions != 2201 {
		t.Fatalf("Transitions = %d, want 2201", m.Transitions)
	}
}

func TestConcurrentMetricsAreConsistent(t *testing.T) {
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 10})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = b.Execute(context.Background(), func(context.Context) error {
				if i%2 == 0 {
					return errBoom
				}
				return nil
			})
			_ = b.Metrics()
		}(i)
	}
	wg.Wait()
	m := b.Metrics()
	if got := m.Successes + m.Failures + m.Rejections; got != 50 {
		t.Fatalf("Successes+Failures+Rejections = %d, want 50 (%+v)", got, m)
	}
}

func TestInvalidSettings(t *testing.T) {
	longName := make([]byte, 129)
	for i := range longName {
		longName[i] = 'a'
	}
	cases := []circuitbreaker.Settings{
		{FailureThreshold: -1},
		{OpenTimeout: -time.Second},
		{HalfOpenMaxCalls: -1},
		{Name: string(longName)},
	}
	for _, s := range cases {
		b, err := circuitbreaker.New(s)
		if !errors.Is(err, circuitbreaker.ErrInvalidSettings) || b != nil {
			t.Fatalf("New(%d, %v, %d) = %v, %v; want ErrInvalidSettings", s.FailureThreshold, s.OpenTimeout, s.HalfOpenMaxCalls, b, err)
		}
		if len(err.Error()) > 200 {
			t.Fatalf("error message too long: %d", len(err.Error()))
		}
	}
	if _, err := circuitbreaker.New(circuitbreaker.Settings{FailureThreshold: -1}); !errors.Is(err, circuitbreaker.ErrInvalidSettings) {
		t.Fatalf("error = %v", err)
	}
	if _, err := circuitbreaker.NewBulkhead(0, 0); !errors.Is(err, circuitbreaker.ErrInvalidSettings) {
		t.Fatalf("NewBulkhead(0, 0) error = %v", err)
	}
	if _, err := circuitbreaker.NewBulkhead(1, -time.Second); !errors.Is(err, circuitbreaker.ErrInvalidSettings) {
		t.Fatalf("NewBulkhead(1, -1s) error = %v", err)
	}
}

func TestErrorCodes(t *testing.T) {
	cases := map[string]error{
		"circuit_open":             circuitbreaker.ErrOpen,
		"too_many_half_open_calls": circuitbreaker.ErrTooManyHalfOpenCalls,
		"bulkhead_full":            circuitbreaker.ErrBulkheadFull,
		"invalid_settings":         circuitbreaker.ErrInvalidSettings,
	}
	for code, err := range cases {
		var e *circuitbreaker.Error
		if !errors.As(err, &e) || e.Code() != code {
			t.Fatalf("error %v does not carry code %q", err, code)
		}
	}
	if got, want := circuitbreaker.ErrOpen.Error(), "circuitbreaker: circuit breaker is open"; got != want {
		t.Fatalf("ErrOpen = %q, want %q", got, want)
	}
}

func TestNilArguments(t *testing.T) {
	b := mustBreaker(t, circuitbreaker.Settings{})
	h, err := circuitbreaker.NewBulkhead(1, 0)
	if err != nil {
		t.Fatalf("NewBulkhead failed: %v", err)
	}
	var nilCtx context.Context
	var nilBreaker *circuitbreaker.Breaker
	var nilBulkhead *circuitbreaker.Bulkhead
	checks := []error{
		b.Execute(nilCtx, okFn),
		b.Execute(context.Background(), nil),
		nilBreaker.Execute(context.Background(), okFn),
		h.Execute(nilCtx, okFn),
		h.Execute(context.Background(), nil),
		nilBulkhead.Execute(context.Background(), okFn),
	}
	for i, err := range checks {
		if !errors.Is(err, circuitbreaker.ErrInvalidSettings) {
			t.Fatalf("check %d error = %v, want ErrInvalidSettings", i, err)
		}
	}
	if _, err := circuitbreaker.Do[int](context.Background(), b, nil); !errors.Is(err, circuitbreaker.ErrInvalidSettings) {
		t.Fatalf("Do with nil fn error = %v", err)
	}
}

func TestDo(t *testing.T) {
	b := mustBreaker(t, circuitbreaker.Settings{FailureThreshold: 1})
	v, err := circuitbreaker.Do(context.Background(), b, func(context.Context) (string, error) { return "value", nil })
	if err != nil || v != "value" {
		t.Fatalf("Do = %q, %v", v, err)
	}
	v, err = circuitbreaker.Do(context.Background(), b, func(context.Context) (string, error) { return "partial", errBoom })
	if !errors.Is(err, errBoom) || v != "partial" {
		t.Fatalf("Do = %q, %v; want partial, errBoom", v, err)
	}
	called := false
	v, err = circuitbreaker.Do(context.Background(), b, func(context.Context) (string, error) {
		called = true
		return "never", nil
	})
	if !errors.Is(err, circuitbreaker.ErrOpen) || v != "" || called {
		t.Fatalf("Do on open breaker = %q, %v, called=%v", v, err, called)
	}
}

func TestBulkheadFullAfterMaxWait(t *testing.T) {
	h, err := circuitbreaker.NewBulkhead(1, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("NewBulkhead failed: %v", err)
	}
	release, done := holdSlot(t, h)
	if h.InFlight() != 1 {
		t.Fatalf("InFlight = %d, want 1", h.InFlight())
	}
	start := time.Now()
	err = h.Execute(context.Background(), okFn)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("second call took %v, want under 1s", elapsed)
	}
	if !errors.Is(err, circuitbreaker.ErrBulkheadFull) {
		t.Fatalf("error = %v, want ErrBulkheadFull", err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("holder error = %v", err)
	}
	if h.InFlight() != 0 {
		t.Fatalf("InFlight = %d, want 0", h.InFlight())
	}
}

func TestBulkheadZeroWaitFailsImmediately(t *testing.T) {
	h, err := circuitbreaker.NewBulkhead(1, 0)
	if err != nil {
		t.Fatalf("NewBulkhead failed: %v", err)
	}
	release, done := holdSlot(t, h)
	if err := h.Execute(context.Background(), okFn); !errors.Is(err, circuitbreaker.ErrBulkheadFull) {
		t.Fatalf("error = %v, want ErrBulkheadFull", err)
	}
	release()
	<-done
	if h.InFlight() != 0 {
		t.Fatalf("InFlight = %d, want 0", h.InFlight())
	}
}

func TestBulkheadContextCancelledWhileWaiting(t *testing.T) {
	h, err := circuitbreaker.NewBulkhead(1, time.Minute)
	if err != nil {
		t.Fatalf("NewBulkhead failed: %v", err)
	}
	release, done := holdSlot(t, h)
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() { res <- h.Execute(ctx, okFn) }()
	cancel()
	if err := <-res; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	release()
	<-done
	if h.InFlight() != 0 {
		t.Fatalf("InFlight = %d, want 0", h.InFlight())
	}
}

func TestBulkheadWaiterAcquiresReleasedSlot(t *testing.T) {
	h, err := circuitbreaker.NewBulkhead(1, time.Minute)
	if err != nil {
		t.Fatalf("NewBulkhead failed: %v", err)
	}
	release, done := holdSlot(t, h)
	res := make(chan error, 1)
	go func() { res <- h.Execute(context.Background(), okFn) }()
	release()
	<-done
	if err := <-res; err != nil {
		t.Fatalf("waiter error = %v, want nil", err)
	}
	if h.InFlight() != 0 {
		t.Fatalf("InFlight = %d, want 0", h.InFlight())
	}
}

func TestBulkheadCancelledBeforeStart(t *testing.T) {
	h, err := circuitbreaker.NewBulkhead(1, 0)
	if err != nil {
		t.Fatalf("NewBulkhead failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err = h.Execute(ctx, func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("error = %v, called = %v", err, called)
	}
}

func TestBulkheadPropagatesErrorAndReleasesOnPanic(t *testing.T) {
	h, err := circuitbreaker.NewBulkhead(1, 0)
	if err != nil {
		t.Fatalf("NewBulkhead failed: %v", err)
	}
	if err := h.Execute(context.Background(), failFn); !errors.Is(err, errBoom) {
		t.Fatalf("error = %v, want errBoom", err)
	}
	var got any
	func() {
		defer func() { got = recover() }()
		_ = h.Execute(context.Background(), func(context.Context) error { panic("oops") })
	}()
	if got != "oops" {
		t.Fatalf("recovered %v, want oops", got)
	}
	if h.InFlight() != 0 {
		t.Fatalf("InFlight = %d, want 0", h.InFlight())
	}
}

func TestBreakerAndBulkheadCompose(t *testing.T) {
	b := mustBreaker(t, circuitbreaker.Settings{Name: "svc", FailureThreshold: 1})
	h, err := circuitbreaker.NewBulkhead(2, 0)
	if err != nil {
		t.Fatalf("NewBulkhead failed: %v", err)
	}
	call := func(fn func(context.Context) error) error {
		return h.Execute(context.Background(), func(ctx context.Context) error { return b.Execute(ctx, fn) })
	}
	if err := call(okFn); err != nil {
		t.Fatalf("error = %v", err)
	}
	if err := call(failFn); !errors.Is(err, errBoom) {
		t.Fatalf("error = %v, want errBoom", err)
	}
	if err := call(okFn); !errors.Is(err, circuitbreaker.ErrOpen) {
		t.Fatalf("error = %v, want ErrOpen", err)
	}
	if h.InFlight() != 0 {
		t.Fatalf("InFlight = %d, want 0", h.InFlight())
	}
}
