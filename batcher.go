// Package batcher accumulates pushed items into batches and delivers them to
// registered callbacks when a batch reaches its size limit or its oldest item
// has waited out the interval.
package batcher

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrClosed is returned by Push after Close has been called.
var ErrClosed = errors.New("batcher: closed")

// Callback consumes one batch. The batch slice is owned by the batcher and
// must not be retained past the call.
type Callback[T any] interface {
	Call(ctx context.Context, batch []T) error
}

// CallbackFunc adapts a function to the Callback interface.
type CallbackFunc[T any] func(ctx context.Context, batch []T) error

// Call calls f(ctx, batch).
func (f CallbackFunc[T]) Call(ctx context.Context, batch []T) error {
	return f(ctx, batch)
}

// Option configures a Batcher at construction.
type Option[T any] func(*Batcher[T])

// WithCallback registers a callback. It is repeatable: every batch fans out
// to all registered callbacks sequentially, in registration order. An error
// from one callback does not skip the remaining callbacks for that batch.
func WithCallback[T any](cb Callback[T]) Option[T] {
	return func(b *Batcher[T]) {
		b.callbacks = append(b.callbacks, cb)
	}
}

// WithBuffer sets the intake channel buffer length. The default is 0, an
// unbuffered intake.
func WithBuffer[T any](l int) Option[T] {
	return func(b *Batcher[T]) {
		b.buffer = l
	}
}

// WithErrorHandler sets the handler invoked with each non-nil callback error.
// The default handler panics.
func WithErrorHandler[T any](h func(context.Context, error)) Option[T] {
	return func(b *Batcher[T]) {
		b.errh = h
	}
}

// Batcher accumulates items pushed to it and delivers them to its callbacks
// in batches. A batch flushes when it reaches n items or when its oldest item
// has waited d; nothing runs while the batcher is idle. Batcher is safe for
// concurrent use.
type Batcher[T any] struct {
	n         int
	d         time.Duration
	buffer    int
	callbacks []Callback[T]
	errh      func(context.Context, error)

	ch     chan T
	done   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// New returns a running Batcher that flushes a batch when it reaches n items
// or when its oldest item has waited d. At least one WithCallback option is
// required; New panics otherwise.
func New[T any](n int, d time.Duration, opts ...Option[T]) *Batcher[T] {
	b := &Batcher[T]{n: n, d: d}
	for _, opt := range opts {
		opt(b)
	}
	if len(b.callbacks) == 0 {
		panic("batcher: no callback registered")
	}
	if b.errh == nil {
		b.errh = func(_ context.Context, err error) {
			panic(err)
		}
	}
	b.ch = make(chan T, b.buffer)
	b.done = make(chan struct{})
	b.ctx, b.cancel = context.WithCancel(context.Background())
	go b.loop()
	return b
}

// Push submits one item. It blocks while the intake buffer is full, returns
// ctx.Err() when ctx expires first, and returns ErrClosed once Close has been
// called.
func (b *Batcher[T]) Push(ctx context.Context, v T) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrClosed
	}
	b.wg.Add(1)
	b.mu.RUnlock()
	defer b.wg.Done()

	select {
	case b.ch <- v:
		return nil
	case <-b.ctx.Done():
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops intake and drains everything already accepted through the
// callbacks as final batches. Expiry of ctx abandons the remainder and
// returns ctx.Err(); shutdown loss is always the caller's explicit deadline.
// Close is idempotent: a second call returns the first result.
func (b *Batcher[T]) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()

		go func() {
			b.wg.Wait()
			close(b.ch)
		}()

		select {
		case <-b.done:
		case <-ctx.Done():
			b.closeErr = ctx.Err()
		}
		b.cancel()
	})
	return b.closeErr
}

// loop is the single owner of the pending batch. It exits when the intake
// channel is closed and drained, or when the batcher context is canceled.
func (b *Batcher[T]) loop() {
	defer close(b.done)

	var (
		buf    []T
		timer  *time.Timer
		timerC <-chan time.Time
	)

	flush := func() {
		if timer != nil {
			timer.Stop()
			timerC = nil
		}
		if len(buf) == 0 {
			return
		}
		for _, cb := range b.callbacks {
			if err := cb.Call(b.ctx, buf); err != nil {
				b.errh(b.ctx, err)
			}
		}
		buf = buf[:0]
	}

	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}
		select {
		case <-b.ctx.Done():
			return
		case v, ok := <-b.ch:
			if !ok {
				flush()
				return
			}
			buf = append(buf, v)
			switch {
			case len(buf) >= b.n:
				flush()
			case len(buf) == 1:
				if timer == nil {
					timer = time.NewTimer(b.d)
				} else {
					timer.Reset(b.d)
				}
				timerC = timer.C
			}
		case <-timerC:
			timerC = nil
			flush()
		}
	}
}
