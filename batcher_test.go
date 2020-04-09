package batcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond * 110)
	defer cancel()

	var count int

	b := New(2,
		time.Millisecond*50,
		func(_ context.Context, v []interface{}) error {
			switch count {
			case 0:
				require.Len(t, v, 2)
			case 1:
				require.Len(t, v, 1)
			default:
				panic("should never happen")
			}
			count ++
			return nil
		},
	)
	b.Run(ctx)

	require.NoError(t, b.Push(ctx, nil))
	require.NoError(t, b.Push(ctx, nil))
	require.NoError(t, b.Push(ctx, nil))

	require.NoError(t, b.Wait(ctx))

	require.Equal(t, 2, count)
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
