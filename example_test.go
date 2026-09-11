package batcher_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mikluko/batcher"
)

// A batcher with size limit 2 flushes as soon as the second item arrives;
// closing it drains the remainder as a final batch.
func Example() {
	b := batcher.New(2, time.Minute,
		batcher.WithCallback(batcher.CallbackFunc[string](func(_ context.Context, batch []string) error {
			fmt.Println(batch)
			return nil
		})),
	)

	ctx := context.Background()
	for _, v := range []string{"a", "b", "c"} {
		if err := b.Push(ctx, v); err != nil {
			fmt.Println("push:", err)
		}
	}
	if err := b.Close(ctx); err != nil {
		fmt.Println("close:", err)
	}
	// Output:
	// [a b]
	// [c]
}

// Every batch fans out to all registered callbacks sequentially, in
// registration order.
func ExampleWithCallback() {
	logBatch := batcher.CallbackFunc[string](func(_ context.Context, batch []string) error {
		fmt.Println("log:", batch)
		return nil
	})
	sendBatch := batcher.CallbackFunc[string](func(_ context.Context, batch []string) error {
		fmt.Println("send:", batch)
		return nil
	})
	b := batcher.New(2, time.Minute,
		batcher.WithCallback(logBatch),
		batcher.WithCallback(sendBatch),
	)

	ctx := context.Background()
	_ = b.Push(ctx, "a")
	_ = b.Push(ctx, "b")
	_ = b.Close(ctx)
	// Output:
	// log: [a b]
	// send: [a b]
}

// PushWait returns a Ticket alongside the usual error; Wait blocks until the
// item's batch has flushed.
func ExampleBatcher_PushWait() {
	b := batcher.New(2, time.Minute,
		batcher.WithCallback(batcher.CallbackFunc[string](func(_ context.Context, batch []string) error {
			fmt.Println("flushed:", batch)
			return nil
		})),
	)

	ctx := context.Background()
	_, _ = b.PushWait(ctx, "a")
	ticket, _ := b.PushWait(ctx, "b")
	if err := ticket.Wait(ctx); err != nil {
		fmt.Println("wait:", err)
	}
	_ = b.Close(ctx)
	// Output:
	// flushed: [a b]
}

// A callback returns ItemErrors instead of a plain error to attribute
// failure to specific items; every other item's Ticket still resolves
// cleanly.
func ExampleItemErrors() {
	boom := errors.New("delivery failed")
	b := batcher.New(3, time.Minute,
		batcher.WithCallback(batcher.CallbackFunc[string](func(_ context.Context, batch []string) error {
			errs := make(batcher.ItemErrors, len(batch))
			errs[1] = boom
			return errs
		})),
		batcher.WithErrorHandler[string](func(_ context.Context, err error) {
			fmt.Println("handled:", err)
		}),
	)

	ctx := context.Background()
	t0, _ := b.PushWait(ctx, "a")
	t1, _ := b.PushWait(ctx, "b")
	t2, _ := b.PushWait(ctx, "c")
	_ = b.Close(ctx)

	fmt.Println("a:", t0.Wait(ctx))
	fmt.Println("b:", t1.Wait(ctx))
	fmt.Println("c:", t2.Wait(ctx))
	// Output:
	// handled: delivery failed
	// a: <nil>
	// b: delivery failed
	// c: <nil>
}

// Without WithErrorHandler a callback error panics; installing a handler
// makes errors the caller's to observe instead.
func ExampleWithErrorHandler() {
	b := batcher.New(2, time.Minute,
		batcher.WithCallback(batcher.CallbackFunc[int](func(_ context.Context, batch []int) error {
			return fmt.Errorf("delivery failed for %d items", len(batch))
		})),
		batcher.WithErrorHandler[int](func(_ context.Context, err error) {
			fmt.Println("handled:", err)
		}),
	)

	ctx := context.Background()
	_ = b.Push(ctx, 1)
	_ = b.Push(ctx, 2)
	_ = b.Close(ctx)
	// Output:
	// handled: delivery failed for 2 items
}
