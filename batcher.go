// Package batcher accumulates pushed items into batches and delivers them to
// registered callbacks when a batch reaches its size limit or its oldest item
// has waited out the interval. Nothing runs while a batcher is idle.
//
// Delivery is loud by default: a callback error panics unless WithErrorHandler
// installs a handler. Close stops intake and drains everything already
// accepted; only expiry of its context abandons items.
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

// FlushReason says why a batch flushed.
type FlushReason string

// The flush reasons. A batch that fills to the size limit flushes for
// FlushReasonSize even while Close is draining; FlushReasonDrain marks only
// the final flush of what remained when intake ended.
const (
	FlushReasonSize     FlushReason = "size"     // the batch reached its size limit
	FlushReasonInterval FlushReason = "interval" // the oldest item waited out the interval
	FlushReasonDrain    FlushReason = "drain"    // Close delivered the remainder
)

// Observer receives measurement events from a Batcher, registered with
// WithObserver. The batcher calls ObservePush for each accepted Push,
// ObserveFlush once per delivered batch, ObserveError for each non-nil
// callback error, and ObserveDrop with the number of accepted items
// abandoned when the context given to Close expires. ObservePush may be
// called concurrently with the other methods; implementations must be safe
// for concurrent use.
type Observer interface {
	// ObservePush is called once per accepted Push.
	ObservePush()

	// ObserveFlush is called once per delivered batch with the reason the
	// batch flushed, its size, and the duration of the whole sequential
	// callback fan-out as measured by the batcher.
	ObserveFlush(reason FlushReason, size int, d time.Duration)

	// ObserveError is called with each non-nil callback error, in addition
	// to, not instead of, the error handler. It runs before the handler.
	ObserveError(err error)

	// ObserveDrop is called with the number of accepted items left
	// undelivered when the context given to Close expires. It is not called
	// when nothing was abandoned.
	ObserveDrop(n int)
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

// WithObserver registers an observer. It is repeatable: every event reaches
// all registered observers, in registration order. With no observers
// registered, observation costs nothing.
func WithObserver[T any](o Observer) Option[T] {
	return func(b *Batcher[T]) {
		b.observers = append(b.observers, o)
	}
}

// WithErrorHandler sets the handler invoked with each non-nil callback error.
// Without it the batcher is loud by default: the default handler panics on the
// first error. Tolerating callback errors is opt-in through this option.
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
	observers []Observer
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
// or when its oldest item has waited d. The delivery loop runs in a goroutine
// New starts; stop it with Close. At least one WithCallback option is
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
		for _, o := range b.observers {
			o.ObservePush()
		}
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

	flush := func(reason FlushReason) {
		if timer != nil {
			timer.Stop()
			timerC = nil
		}
		if len(buf) == 0 {
			return
		}
		var start time.Time
		if len(b.observers) != 0 {
			start = time.Now()
		}
		for _, cb := range b.callbacks {
			if err := cb.Call(b.ctx, buf); err != nil {
				for _, o := range b.observers {
					o.ObserveError(err)
				}
				b.errh(b.ctx, err)
			}
		}
		if len(b.observers) != 0 {
			d := time.Since(start)
			for _, o := range b.observers {
				o.ObserveFlush(reason, len(buf), d)
			}
		}
		buf = buf[:0]
	}

	for {
		select {
		case <-b.ctx.Done():
			b.abandon(len(buf))
			return
		default:
		}
		select {
		case <-b.ctx.Done():
			b.abandon(len(buf))
			return
		case v, ok := <-b.ch:
			if !ok {
				flush(FlushReasonDrain)
				return
			}
			buf = append(buf, v)
			switch {
			case len(buf) >= b.n:
				flush(FlushReasonSize)
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
			flush(FlushReasonInterval)
		}
	}
}

// abandon reports to the observers the accepted items the loop leaves
// undelivered on context expiry: the pending batch plus everything still in
// the intake channel, which it drains to count. It blocks until Push can no
// longer accept, so the count is final.
func (b *Batcher[T]) abandon(pending int) {
	if len(b.observers) == 0 {
		return
	}
	n := pending
	for range b.ch {
		n++
	}
	if n == 0 {
		return
	}
	for _, o := range b.observers {
		o.ObserveDrop(n)
	}
}
