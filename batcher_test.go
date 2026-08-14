package batcher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatcher_Full(t *testing.T) {
	ctx := context.Background()

	var items, batches atomic.Int64
	b := New(2, 50*time.Second, WithCallback(CallbackFunc[struct{}](
		func(_ context.Context, v []struct{}) error {
			require.Len(t, v, 2)
			items.Add(int64(len(v)))
			batches.Add(1)
			return nil
		},
	)))
	t.Cleanup(func() { _ = b.Close(context.Background()) })

	require.NoError(t, b.Push(ctx, struct{}{}))
	require.NoError(t, b.Push(ctx, struct{}{}))
	time.Sleep(10 * time.Millisecond)

	require.Equal(t, int64(2), items.Load())
	require.Equal(t, int64(1), batches.Load())
}

func TestBatcher_Partial(t *testing.T) {
	ctx := context.Background()

	var items, batches atomic.Int64
	b := New(2, 50*time.Millisecond, WithCallback(CallbackFunc[struct{}](
		func(_ context.Context, v []struct{}) error {
			require.Len(t, v, 1)
			items.Add(int64(len(v)))
			batches.Add(1)
			return nil
		},
	)))
	t.Cleanup(func() { _ = b.Close(context.Background()) })

	require.NoError(t, b.Push(ctx, struct{}{}))
	time.Sleep(60 * time.Millisecond)

	require.Equal(t, int64(1), items.Load())
	require.Equal(t, int64(1), batches.Load())
}

func TestBatcher_FullThenPartial(t *testing.T) {
	ctx := context.Background()

	var items, batches atomic.Int64
	b := New(2, 50*time.Millisecond, WithCallback(CallbackFunc[struct{}](
		func(_ context.Context, v []struct{}) error {
			items.Add(int64(len(v)))
			batches.Add(1)
			return nil
		},
	)))
	t.Cleanup(func() { _ = b.Close(context.Background()) })

	require.NoError(t, b.Push(ctx, struct{}{}))
	require.NoError(t, b.Push(ctx, struct{}{}))
	time.Sleep(5 * time.Millisecond)

	require.Equal(t, int64(2), items.Load())
	require.Equal(t, int64(1), batches.Load())

	require.NoError(t, b.Push(ctx, struct{}{}))
	time.Sleep(60 * time.Millisecond)

	require.Equal(t, int64(3), items.Load())
	require.Equal(t, int64(2), batches.Load())
}

func TestBatcher_PartialThenFull(t *testing.T) {
	ctx := context.Background()

	var items, batches atomic.Int64
	b := New(2, 50*time.Millisecond, WithCallback(CallbackFunc[struct{}](
		func(_ context.Context, v []struct{}) error {
			items.Add(int64(len(v)))
			batches.Add(1)
			return nil
		},
	)))
	t.Cleanup(func() { _ = b.Close(context.Background()) })

	require.NoError(t, b.Push(ctx, struct{}{}))
	time.Sleep(60 * time.Millisecond)

	require.Equal(t, int64(1), items.Load())
	require.Equal(t, int64(1), batches.Load())

	require.NoError(t, b.Push(ctx, struct{}{}))
	require.NoError(t, b.Push(ctx, struct{}{}))
	time.Sleep(10 * time.Millisecond)

	require.Equal(t, int64(3), items.Load())
	require.Equal(t, int64(2), batches.Load())
}

func TestBatcher_Error(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	e := errors.New("some error")
	got := make(chan error, 1)

	b := New(1, 100*time.Millisecond,
		WithCallback(CallbackFunc[struct{}](
			func(context.Context, []struct{}) error { return e },
		)),
		WithErrorHandler[struct{}](func(_ context.Context, err error) { got <- err }),
	)
	t.Cleanup(func() { _ = b.Close(context.Background()) })

	require.NoError(t, b.Push(ctx, struct{}{}))

	select {
	case err := <-got:
		require.ErrorIs(t, err, e)
	case <-ctx.Done():
		t.Fatal("error was not delivered to the handler")
	}
}

func TestBatcher_Close(t *testing.T) {
	t.Run("drains pending batch", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var batches atomic.Int64
		b := New(100, time.Minute, WithCallback(CallbackFunc[struct{}](
			func(_ context.Context, v []struct{}) error {
				require.Len(t, v, 1)
				batches.Add(1)
				return nil
			},
		)))

		require.NoError(t, b.Push(ctx, struct{}{}))
		require.NoError(t, b.Close(ctx))
		require.Equal(t, int64(1), batches.Load())
	})

	t.Run("ignores empty buffer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		b := New(100, time.Minute, WithCallback(CallbackFunc[struct{}](
			func(context.Context, []struct{}) error {
				require.Fail(t, "callback should not be called")
				return nil
			},
		)))

		require.NoError(t, b.Close(ctx))
	})

	t.Run("push after close", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		b := New(100, time.Minute, WithCallback(CallbackFunc[struct{}](
			func(context.Context, []struct{}) error { return nil },
		)))

		require.NoError(t, b.Close(ctx))
		require.ErrorIs(t, b.Push(ctx, struct{}{}), ErrClosed)
	})
}

func TestBatcher_WithBuffer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	entered := make(chan struct{})
	unblock := make(chan struct{})
	first := true

	b := New(1, time.Minute,
		WithBuffer[struct{}](2),
		WithCallback(CallbackFunc[struct{}](
			func(context.Context, []struct{}) error {
				if first {
					first = false
					close(entered)
					<-unblock
				}
				return nil
			},
		)),
	)
	t.Cleanup(func() {
		close(unblock)
		_ = b.Close(context.Background())
	})

	require.NoError(t, b.Push(ctx, struct{}{}))
	<-entered

	require.NoError(t, b.Push(ctx, struct{}{}))
	require.NoError(t, b.Push(ctx, struct{}{}))

	err := b.Push(ctx, struct{}{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func BenchmarkBatcher(b *testing.B) {
	ctx := context.Background()

	d := New(10, 100*time.Millisecond, WithCallback(CallbackFunc[struct{}](
		func(context.Context, []struct{}) error { return nil },
	)))
	defer func() { _ = d.Close(ctx) }()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = d.Push(ctx, struct{}{})
		}
	})
}
