package batcher

import (
	"context"
	"time"
)

type Batcher interface {
	Push(ctx context.Context, v interface{}) error
	Run(ctx context.Context)
	Wait(ctx context.Context) error
}

type CallbackFunc func(context.Context, []interface{}) error

func New(n int, d time.Duration, f CallbackFunc) Batcher {
	return newBatcher(n, d, f)
}

func newBatcher(n int, d time.Duration, f CallbackFunc) *batcher {
	return &batcher{
		n:   n,
		d:   d,
		f:   f,
		ch:  make(chan interface{}, n),
		ech: make(chan error),
		buf: make([]interface{}, 0, n),
	}
}

type batcher struct {
	n int           // batch size limit
	d time.Duration // batch time limit
	f CallbackFunc

	ch  chan interface{}
	ech chan error
	buf []interface{}
}

func (b *batcher) Push(ctx context.Context, i interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.ch <- i:
		return nil
	}
}

func (b *batcher) Run(ctx context.Context) {
	go func() {
		b.ech <- b.loop(ctx)
	}()
}

func (b *batcher) Wait(ctx context.Context) (err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
		if err == context.DeadlineExceeded {
			err = nil
		}
	case err = <-b.ech:
		break
	}
	return err
}

func (b *batcher) loop(ctx context.Context) error {
	for {
		if err := b.load(ctx); err != nil {
			return err
		}
		if len(b.buf) == 0 {
			continue
		}
		if err := b.f(ctx, b.buf); err != nil {
			return err
		}
		b.buf = b.buf[0:0]
	}
}

func (b *batcher) load(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, b.d)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err == context.DeadlineExceeded {
				err = nil
			}
			return err
		case i := <-b.ch:
			b.buf = append(b.buf, i)
		}
		if len(b.buf) == b.n {
			return nil
		}
	}
}
