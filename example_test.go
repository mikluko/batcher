package batcher_test

import (
	"context"
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
