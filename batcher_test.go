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
	b.Run(ctx)

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
	b.Run(ctx)

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
	b.Run(ctx)

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
	b.Run(ctx)

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
	b.Run(ctx)

	require.NoError(t, b.Push(ctx, nil))
	require.Error(t, b.Wait(ctx), e.Error())
}

func TestBatcher_Flush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b := New(100,
		time.Minute,
		func(_ context.Context, v []interface{}) error {
			require.Len(t, v, 1)
			return nil
		},
	)
	b.Run(ctx)

	var err error

	err = b.Push(ctx, nil)
	require.NoError(t, err)

	err = b.Flush(ctx)
	require.NoError(t, err)
}

func TestBatcher_WithBuffer(t *testing.T)  {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
	d.Run(ctx)

	b.RunParallel(func(pb *testing.PB){
		for pb.Next() {
			_ = d.Push(ctx, nil)
		}
	})
}