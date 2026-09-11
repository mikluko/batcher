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

// ErrAbandoned is returned by Ticket.Wait when the item's batch is dropped
// undelivered because the context passed to Close expired first.
var ErrAbandoned = errors.New("batcher: abandoned")

// Callback consumes one batch. The batch slice is owned by the batcher and
// must not be retained past the call. Returning an ItemErrors instead of a
// plain error attributes failure to individual items rather than the whole
// batch. Call is never invoked concurrently with itself unless
// WithFlushConcurrency sets M>1, in which case it may run concurrently for
// different batches.
type Callback[T any] interface {
	Call(ctx context.Context, batch []T) error
}

// CallbackFunc adapts a function to the Callback interface.
type CallbackFunc[T any] func(ctx context.Context, batch []T) error

// Call calls f(ctx, batch).
func (f CallbackFunc[T]) Call(ctx context.Context, batch []T) error {
	return f(ctx, batch)
}

// ItemErrors is returned by Callback.Call in place of a single error to
// attribute failure to individual items rather than the batch as a whole. It
// must have the same length as the batch passed to Call; entry i is item i's
// error, or nil if that item succeeded. An ItemErrors of any other length is
// treated as an ordinary whole-batch error instead. A waiter's Ticket
// resolves with the errors.Join of whatever every registered callback
// attributed to its own item.
type ItemErrors []error

// Error joins the messages of the non-nil entries.
func (e ItemErrors) Error() string {
	if err := errors.Join([]error(e)...); err != nil {
		return err.Error()
	}
	return "batcher: no item errors"
}

// Unwrap exposes the non-nil entries to errors.Is and errors.As.
func (e ItemErrors) Unwrap() []error {
	return e
}

// entry is one item in transit through the intake channel, paired with the
// ticket to resolve once its batch flushes or is abandoned.
type entry[T any] struct {
	v T
	t *Ticket
}

// Ticket is returned by PushWait and resolves once the item's batch has
// flushed or been abandoned. The zero value is not usable; obtain a Ticket
// from PushWait.
type Ticket struct {
	done chan struct{}
	err  error
}

// resolve delivers err to the ticket and unblocks Wait. It must be called
// exactly once, only by the loop goroutine that owns the batch.
func (t *Ticket) resolve(err error) {
	t.err = err
	close(t.done)
}

// Wait blocks until the item's batch has flushed or been abandoned. It
// returns nil once the batch has flushed with nothing attributed to this
// item; the error attributed to it if some callback failed (see
// ItemErrors); ErrAbandoned if the item was dropped without flushing; or
// ctx.Err() if ctx expires first.
func (t *Ticket) Wait(ctx context.Context) error {
	select {
	case <-t.done:
		return t.err
	case <-ctx.Done():
		return ctx.Err()
	}
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
// for concurrent use. Once WithFlushConcurrency sets M>1, every method may
// additionally be called concurrently with itself, for different batches.
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
// first error. Tolerating callback errors is opt-in through this option. Once
// WithFlushConcurrency sets M>1, h may be called concurrently for different
// batches and must be safe for that.
func WithErrorHandler[T any](h func(context.Context, error)) Option[T] {
	return func(b *Batcher[T]) {
		b.errh = h
	}
}

// WithFlushConcurrency sets how many batches may be filling or flushing at
// once, M. The default is 1: batches never overlap. A higher M instead
// builds M independent sequential batchers and routes intake to one of them
// until it starts flushing (by reaching its size limit or timing out), then
// moves to the next one that isn't already flushing; Push and PushWait
// block once every one is, until one finishes. This trades two guarantees
// for throughput under a slow callback: Callback.Call and the
// WithErrorHandler handler may now run concurrently across different
// batches and must be safe for that, and batches are no longer guaranteed
// to complete in arrival order. New panics if m is less than 1.
func WithFlushConcurrency[T any](m int) Option[T] {
	return func(b *Batcher[T]) {
		b.flushConcurrency = m
	}
}

// Batcher accumulates items pushed to it and delivers them to its callbacks
// in batches. A batch flushes when it reaches n items or when its oldest item
// has waited d; nothing runs while the batcher is idle. Batcher is safe for
// concurrent use.
type Batcher[T any] struct {
	n                int
	d                time.Duration
	buffer           int
	callbacks        []Callback[T]
	observers        []Observer
	errh             func(context.Context, error)
	flushConcurrency int

	children []*Batcher[T]

	// routerMu, cur, busy, and wake implement pick's routing on the
	// top-level wrapper only, when children is non-empty. flushHook is set
	// on each child by the wrapper that built it, and is otherwise nil.
	routerMu  sync.Mutex
	cur       int
	busy      []bool
	wake      chan struct{}
	flushHook func(active bool)

	ch     chan entry[T]
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
// required; New panics otherwise, and so does a WithFlushConcurrency below 1.
//
// With WithFlushConcurrency left at its default of 1, New returns a single
// sequential instance. A higher M instead builds M such instances sharing
// the same callbacks, observers, and error handler, and returns a Batcher
// that routes Push and PushWait across them; see WithFlushConcurrency.
func New[T any](n int, d time.Duration, opts ...Option[T]) *Batcher[T] {
	b := &Batcher[T]{n: n, d: d, flushConcurrency: 1}
	for _, opt := range opts {
		opt(b)
	}
	if len(b.callbacks) == 0 {
		panic("batcher: no callback registered")
	}
	if b.flushConcurrency < 1 {
		panic("batcher: flush concurrency must be at least 1")
	}
	if b.errh == nil {
		b.errh = func(_ context.Context, err error) {
			panic(err)
		}
	}
	if b.flushConcurrency == 1 {
		b.start()
		return b
	}
	w := &Batcher[T]{
		children: make([]*Batcher[T], b.flushConcurrency),
		busy:     make([]bool, b.flushConcurrency),
		wake:     make(chan struct{}),
	}
	for i := range w.children {
		c := &Batcher[T]{
			n: n, d: d,
			buffer:    b.buffer,
			callbacks: b.callbacks,
			observers: b.observers,
			errh:      b.errh,
		}
		c.flushHook = func(active bool) {
			w.routerMu.Lock()
			w.busy[i] = active
			if !active {
				close(w.wake)
				w.wake = make(chan struct{})
			}
			w.routerMu.Unlock()
		}
		c.start()
		w.children[i] = c
	}
	return w
}

// start allocates the intake channel and launches the delivery loop. b must
// already carry its resolved configuration.
func (b *Batcher[T]) start() {
	b.ch = make(chan entry[T], b.buffer)
	b.done = make(chan struct{})
	b.ctx, b.cancel = context.WithCancel(context.Background())
	go b.loop()
}

// Push submits one item. It blocks while the intake buffer is full, returns
// ctx.Err() when ctx expires first, and returns ErrClosed once Close has been
// called. See PushWait to additionally wait for the item's batch to flush.
// With WithFlushConcurrency set above 1, Push routes to one of the
// underlying batchers; see WithFlushConcurrency.
func (b *Batcher[T]) Push(ctx context.Context, v T) error {
	if len(b.children) != 0 {
		c, err := b.pick(ctx)
		if err != nil {
			return err
		}
		return c.Push(ctx, v)
	}
	_, err := b.push(ctx, v, nil)
	return err
}

// PushWait submits one item like Push, additionally returning a Ticket whose
// Wait method resolves once the item's batch has flushed or been abandoned.
func (b *Batcher[T]) PushWait(ctx context.Context, v T) (*Ticket, error) {
	if len(b.children) != 0 {
		c, err := b.pick(ctx)
		if err != nil {
			return nil, err
		}
		return c.PushWait(ctx, v)
	}
	return b.push(ctx, v, &Ticket{done: make(chan struct{})})
}

// pick returns the last underlying batcher used, if it is not currently
// flushing, or else the next one after it, in rotation, that isn't. It
// blocks until one is available or ctx expires.
func (b *Batcher[T]) pick(ctx context.Context) (*Batcher[T], error) {
	b.routerMu.Lock()
	for {
		n := len(b.children)
		for i := range n {
			idx := (b.cur + i) % n
			if !b.busy[idx] {
				b.cur = idx
				c := b.children[idx]
				b.routerMu.Unlock()
				return c, nil
			}
		}
		wake := b.wake
		b.routerMu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		b.routerMu.Lock()
	}
}

// push is the shared implementation of Push and PushWait. t is nil for Push,
// which forgoes the per-item ticket.
func (b *Batcher[T]) push(ctx context.Context, v T, t *Ticket) (*Ticket, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrClosed
	}
	b.wg.Add(1)
	b.mu.RUnlock()
	defer b.wg.Done()

	select {
	case b.ch <- entry[T]{v: v, t: t}:
		for _, o := range b.observers {
			o.ObservePush()
		}
		return t, nil
	case <-b.ctx.Done():
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close stops intake and drains everything already accepted through the
// callbacks as final batches. Expiry of ctx abandons the remainder and
// returns ctx.Err(); shutdown loss is always the caller's explicit deadline.
// Close is idempotent: a second call returns the first result. With
// WithFlushConcurrency set above 1, Close closes every underlying batcher
// concurrently against the same ctx and returns their errors joined
// (errors.Join), nil if every one closed cleanly.
func (b *Batcher[T]) Close(ctx context.Context) error {
	if len(b.children) != 0 {
		errs := make([]error, len(b.children))
		var wg sync.WaitGroup
		wg.Add(len(b.children))
		for i, c := range b.children {
			go func() {
				defer wg.Done()
				errs[i] = c.Close(ctx)
			}()
		}
		wg.Wait()
		return errors.Join(errs...)
	}
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
		buf      []T
		tickets  []*Ticket
		itemErrs []error
		timer    *time.Timer
		timerC   <-chan time.Time
	)

	flush := func(reason FlushReason) {
		if timer != nil {
			timer.Stop()
			timerC = nil
		}
		if len(buf) == 0 {
			return
		}
		if b.flushHook != nil {
			b.flushHook(true)
			defer b.flushHook(false)
		}
		var start time.Time
		if len(b.observers) != 0 {
			start = time.Now()
		}
		needItemErrs := false
		for _, t := range tickets {
			if t != nil {
				needItemErrs = true
				break
			}
		}
		if needItemErrs {
			if cap(itemErrs) < len(buf) {
				itemErrs = make([]error, len(buf))
			} else {
				itemErrs = itemErrs[:len(buf)]
				clear(itemErrs)
			}
		}
		for _, cb := range b.callbacks {
			err := cb.Call(b.ctx, buf)
			if err != nil {
				for _, o := range b.observers {
					o.ObserveError(err)
				}
				b.errh(b.ctx, err)
			}
			if needItemErrs {
				attributeItemError(itemErrs, err)
			}
		}
		if len(b.observers) != 0 {
			d := time.Since(start)
			for _, o := range b.observers {
				o.ObserveFlush(reason, len(buf), d)
			}
		}
		for i, t := range tickets {
			if t != nil {
				t.resolve(itemErrs[i])
			}
		}
		buf = buf[:0]
		tickets = tickets[:0]
	}

	for {
		select {
		case <-b.ctx.Done():
			b.abandon(tickets)
			return
		default:
		}
		select {
		case <-b.ctx.Done():
			b.abandon(tickets)
			return
		case e, ok := <-b.ch:
			if !ok {
				flush(FlushReasonDrain)
				return
			}
			buf = append(buf, e.v)
			tickets = append(tickets, e.t)
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

// attributeItemError folds one callback's returned error into dst, one slot
// per batch item. An ItemErrors of the same length as dst attributes each
// entry to its item; any other error is attributed to every item.
func attributeItemError(dst []error, err error) {
	if err == nil {
		return
	}
	var ie ItemErrors
	if errors.As(err, &ie) && len(ie) == len(dst) {
		for i, e := range ie {
			if e == nil {
				continue
			}
			if dst[i] == nil {
				dst[i] = e
			} else {
				dst[i] = errors.Join(dst[i], e)
			}
		}
		return
	}
	for i := range dst {
		if dst[i] == nil {
			dst[i] = err
		} else {
			dst[i] = errors.Join(dst[i], err)
		}
	}
}

// abandon resolves with ErrAbandoned the tickets of items the loop leaves
// undelivered on context expiry, and reports their count to the observers:
// the pending batch's tickets plus everything still in the intake channel,
// which it drains to collect them. It blocks until Push can no longer
// accept, so the count is final.
func (b *Batcher[T]) abandon(pending []*Ticket) {
	n := len(pending)
	for _, t := range pending {
		if t != nil {
			t.resolve(ErrAbandoned)
		}
	}
	for e := range b.ch {
		n++
		if e.t != nil {
			e.t.resolve(ErrAbandoned)
		}
	}
	if len(b.observers) == 0 || n == 0 {
		return
	}
	for _, o := range b.observers {
		o.ObserveDrop(n)
	}
}
