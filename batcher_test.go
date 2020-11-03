package batcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatcher_Full(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(2,
		time.Second * 50,
		func(_ context.Context, v []interface{}) error {
			require.Len(t, v, 2)
			return nil
		},
	)
	go func() {
		require.True(t, errors.Is(b.Run(ctx), context.Canceled))
	}()

	require.NoError(t, b.Push(ctx, nil))
	require.NoError(t, b.Push(ctx, nil))
	time.Sleep(time.Millisecond * 10)

	m, n := b.Counters()
	require.Equal(t, int64(2), m)
	require.Equal(t, int64(1), n)
}

func TestBatcher_Partial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(2,
		time.Millisecond * 50,
		func(_ context.Context, v []interface{}) error {
			require.Len(t, v, 1)
			return nil
		},
	)

	go func() {
		require.True(t, errors.Is(b.Run(ctx), context.Canceled))
	}()

	require.NoError(t, b.Push(ctx, nil))
	time.Sleep(time.Millisecond * 60)

	m, n := b.Counters()
	require.Equal(t, int64(1), m)
	require.Equal(t, int64(1), n)
}

func TestBatcher_FullThenPartial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(2,
		time.Millisecond * 50,
		func(_ context.Context, v []interface{}) error {
			return nil
		},
	)

	go func() {
		require.True(t, errors.Is(b.Run(ctx), context.Canceled))
	}()

	require.NoError(t, b.Push(ctx, nil))
	require.NoError(t, b.Push(ctx, nil))
	time.Sleep(time.Millisecond * 5)

	m, n := b.Counters()
	require.Equal(t, int64(2), m)
	require.Equal(t, int64(1), n)

	require.NoError(t, b.Push(ctx, nil))
	time.Sleep(time.Millisecond * 60)

	m, n = b.Counters()
	require.Equal(t, int64(3), m)
	require.Equal(t, int64(2), n)
}

func TestBatcher_PartialThenFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(2,
		time.Millisecond * 50,
		func(_ context.Context, v []interface{}) error {
			return nil
		},
	)

	go func() {
		require.True(t, errors.Is(b.Run(ctx), context.Canceled))
	}()

	require.NoError(t, b.Push(ctx, nil))
	time.Sleep(time.Millisecond * 60)

	m, n := b.Counters()
	require.Equal(t, int64(1), m)
	require.Equal(t, int64(1), n)

	require.NoError(t, b.Push(ctx, nil))
	require.NoError(t, b.Push(ctx, nil))
	time.Sleep(time.Millisecond * 10)

	m, n = b.Counters()
	require.Equal(t, int64(3), m)
	require.Equal(t, int64(2), n)
}

func TestBatcher_Error(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	e := errors.New("some error")

	b := New(1,
		time.Millisecond*100,
		func(_ context.Context, v []interface{}) error {
			return e
		},
	)

	go func() {
		require.True(t, errors.Is(b.Run(ctx), e))
	}()


	require.NoError(t, b.Push(ctx, nil))
}

func TestBatcher_Flush(t *testing.T) {
	t.Run("one time", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		b := New(100,
			time.Minute,
			func(_ context.Context, v []interface{}) error {
				require.Len(t, v, 1)
				return nil
			},
		)
		go func() {
			require.True(t, errors.Is(b.Run(ctx), context.Canceled))
		}()

		var err error

		err = b.Push(ctx, nil)
		require.NoError(t, err)

		err = b.Flush(ctx)
		require.NoError(t, err)
	})

	t.Run("ignores empty buffer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		b := New(100,
			time.Minute,
			func(_ context.Context, v []interface{}) error {
				require.Fail(t, "flush should not be called")
				return nil
			},
		)
		go func() {
			require.True(t, errors.Is(b.Run(ctx), context.Canceled))
		}()

		var err error

		err = b.Flush(ctx)
		require.NoError(t, err)
	})
}

func TestBatcher_WithBuffer(t *testing.T)  {
	ctx, cancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
	defer cancel()

	b := NewBuffer(1, 2, time.Minute, func(ctx context.Context, i []interface{}) error {
		<-ctx.Done()
		return nil
	})

	var err error

	err = b.Push(ctx, nil)
	require.NoError(t, err)

	err = b.Push(ctx, nil)
	require.NoError(t, err)

	err = b.Push(ctx, nil)
	require.True(t, errors.Is(err, context.DeadlineExceeded))
}

func BenchmarkBatcher(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := New(10,
		time.Millisecond * 100,
		func(_ context.Context, v []interface{}) error {
			// time.Sleep(time.Millisecond)
			return nil
		},
	)

	go func() {
		require.True(b, errors.Is(d.Run(ctx), context.Canceled))
	}()

	b.RunParallel(func(pb *testing.PB){
		for pb.Next() {
			_ = d.Push(ctx, nil)
		}
	})
}