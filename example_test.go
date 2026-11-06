package circuitbreaker_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/Trysac/u046-circuit-breaker"
)

func ExampleBreaker_Execute() {
	b, _ := circuitbreaker.New(circuitbreaker.Settings{Name: "db", FailureThreshold: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("down") })
	err := b.Execute(context.Background(), func(context.Context) error { return nil })
	fmt.Println(b.State())
	fmt.Println(err)
	// Output:
	// open
	// circuitbreaker: circuit breaker is open
}

func ExampleDo() {
	b, _ := circuitbreaker.New(circuitbreaker.Settings{FailureThreshold: 1})
	n, err := circuitbreaker.Do(context.Background(), b, func(context.Context) (int, error) { return 42, nil })
	fmt.Println(n, err)
	_, _ = circuitbreaker.Do(context.Background(), b, func(context.Context) (int, error) { return 0, errors.New("down") })
	n, err = circuitbreaker.Do(context.Background(), b, func(context.Context) (int, error) { return 42, nil })
	fmt.Println(n, err)
	// Output:
	// 42 <nil>
	// 0 circuitbreaker: circuit breaker is open
}

func ExampleBulkhead_Execute() {
	h, _ := circuitbreaker.NewBulkhead(2, 0)
	err := h.Execute(context.Background(), func(context.Context) error {
		fmt.Println("in flight:", h.InFlight())
		return nil
	})
	fmt.Println(err, h.InFlight())
	// Output:
	// in flight: 1
	// <nil> 0
}
