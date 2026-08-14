package batcher

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// recorder collects batches, cloning each because the batcher owns the slice.
// It is safe for concurrent use, so tests may inspect it mid-run.
type recorder struct {
	mu      sync.Mutex
	batches [][]int
}

func (r *recorder) Call(_ context.Context, batch []int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, slices.Clone(batch))
	return nil
}

func (r *recorder) got() [][]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.batches)
}

func assertBatches(t *testing.T, got, want [][]int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d batches %v, want %v", len(got), got, want)
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Fatalf("batch %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

func mustPush[T any](t *testing.T, b *Batcher[T], v T) {
	t.Helper()
	if err := b.Push(context.Background(), v); err != nil {
		t.Fatalf("Push: %v", err)
	}
}

func mustClose[T any](t *testing.T, b *Batcher[T]) {
	t.Helper()
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestFlushOnFullBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		b := New(2, time.Hour, WithCallback[int](&r))
		mustPush(t, b, 1)
		mustPush(t, b, 2)
		synctest.Wait()
		assertBatches(t, r.got(), [][]int{{1, 2}})
		mustClose(t, b)
	})
}

func TestFlushOnTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const d = 5 * time.Second
		var r recorder
		var flushAt time.Time
		b := New(3, d, WithCallback[int](CallbackFunc[int](func(ctx context.Context, batch []int) error {
			flushAt = time.Now()
			return r.Call(ctx, batch)
		})))
		start := time.Now()
		mustPush(t, b, 1)
		time.Sleep(2 * time.Second)
		mustPush(t, b, 2)
		time.Sleep(3*time.Second - time.Nanosecond)
		synctest.Wait()
		if got := r.got(); len(got) != 0 {
			t.Fatalf("flushed before d elapsed: %v", got)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		assertBatches(t, r.got(), [][]int{{1, 2}})
		mustClose(t, b)
		if got := flushAt.Sub(start); got != d {
			t.Fatalf("flushed %v after first item, want exactly %v", got, d)
		}
	})
}

func TestFullBatchThenTimerFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const d = 5 * time.Second
		var r recorder
		b := New(2, d, WithCallback[int](&r))
		mustPush(t, b, 1)
		mustPush(t, b, 2)
		synctest.Wait()
		assertBatches(t, r.got(), [][]int{{1, 2}})
		mustPush(t, b, 3)
		time.Sleep(d)
		synctest.Wait()
		assertBatches(t, r.got(), [][]int{{1, 2}, {3}})
		mustClose(t, b)
	})
}

func TestTimerFlushThenFullBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const d = 5 * time.Second
		var r recorder
		b := New(2, d, WithCallback[int](&r))
		mustPush(t, b, 1)
		time.Sleep(d)
		synctest.Wait()
		assertBatches(t, r.got(), [][]int{{1}})
		mustPush(t, b, 2)
		mustPush(t, b, 3)
		synctest.Wait()
		assertBatches(t, r.got(), [][]int{{1}, {2, 3}})
		mustClose(t, b)
	})
}

func TestCloseDrainsPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		b := New(10, time.Hour, WithCallback[int](&r))
		mustPush(t, b, 1)
		mustPush(t, b, 2)
		mustPush(t, b, 3)
		mustClose(t, b)
		assertBatches(t, r.got(), [][]int{{1, 2, 3}})
	})
}

func TestCloseDrainsBufferedInBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		b := New(2, time.Hour, WithBuffer[int](8), WithCallback[int](&r))
		for i := 1; i <= 5; i++ {
			mustPush(t, b, i)
		}
		mustClose(t, b)
		assertBatches(t, r.got(), [][]int{{1, 2}, {3, 4}, {5}})
	})
}

func TestCloseEmpty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(10, time.Hour, WithCallback[int](CallbackFunc[int](func(context.Context, []int) error {
			t.Error("callback called with nothing pushed")
			return nil
		})))
		mustClose(t, b)
	})
}

func TestPushAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(10, time.Hour, WithCallback[int](CallbackFunc[int](func(context.Context, []int) error {
			return nil
		})))
		mustClose(t, b)
		if err := b.Push(context.Background(), 1); !errors.Is(err, ErrClosed) {
			t.Fatalf("Push after Close: got %v, want ErrClosed", err)
		}
	})
}

func TestCloseIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		b := New(10, time.Hour, WithCallback[int](&r))
		mustPush(t, b, 1)
		mustClose(t, b)
		mustClose(t, b)
		assertBatches(t, r.got(), [][]int{{1}})
	})
}

func TestCloseExpiryAbandonsRemainder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		var calls atomic.Int32
		b := New(1, time.Hour,
			WithBuffer[int](1),
			WithCallback[int](CallbackFunc[int](func(cbCtx context.Context, _ []int) error {
				calls.Add(1)
				<-cbCtx.Done()
				return nil
			})))
		mustPush(t, b, 1)
		synctest.Wait()
		mustPush(t, b, 2)
		closeCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := b.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close with expired ctx: got %v, want DeadlineExceeded", err)
		}
		if err := b.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second Close: got %v, want the first result", err)
		}
		synctest.Wait()
		if n := calls.Load(); n != 1 {
			t.Fatalf("callback called %d times, want 1: remainder was not abandoned", n)
		}
	})
}

func TestErrorDeliveredAndBatchingContinues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("boom")
		var r recorder
		var handled []error
		b := New(1, time.Hour,
			WithCallback[int](CallbackFunc[int](func(ctx context.Context, batch []int) error {
				_ = r.Call(ctx, batch)
				return boom
			})),
			WithErrorHandler[int](func(_ context.Context, err error) {
				handled = append(handled, err)
			}))
		mustPush(t, b, 1)
		mustPush(t, b, 2)
		mustClose(t, b)
		assertBatches(t, r.got(), [][]int{{1}, {2}})
		if len(handled) != 2 || !errors.Is(handled[0], boom) || !errors.Is(handled[1], boom) {
			t.Fatalf("handled errors: got %v, want [boom boom]", handled)
		}
	})
}

func TestFanOutSequentialAndErrorDoesNotSkip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		err1 := errors.New("first")
		err2 := errors.New("second")
		var order []string
		var handled []error
		cb := func(name string, err error) Callback[int] {
			return CallbackFunc[int](func(context.Context, []int) error {
				order = append(order, name)
				return err
			})
		}
		b := New(1, time.Hour,
			WithCallback[int](cb("a", err1)),
			WithCallback[int](cb("b", nil)),
			WithCallback[int](cb("c", err2)),
			WithErrorHandler[int](func(_ context.Context, err error) {
				handled = append(handled, err)
			}))
		mustPush(t, b, 1)
		mustClose(t, b)
		if !slices.Equal(order, []string{"a", "b", "c"}) {
			t.Fatalf("callback order: got %v, want [a b c]", order)
		}
		if len(handled) != 2 || !errors.Is(handled[0], err1) || !errors.Is(handled[1], err2) {
			t.Fatalf("handled errors: got %v, want [first second]", handled)
		}
	})
}

func TestBufferBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		b := New(1, time.Hour,
			WithBuffer[int](2),
			WithCallback[int](CallbackFunc[int](func(context.Context, []int) error {
				<-release
				return nil
			})))
		mustPush(t, b, 1)
		synctest.Wait()
		mustPush(t, b, 2)
		mustPush(t, b, 3)
		pushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := b.Push(pushCtx, 4); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Push into full buffer: got %v, want DeadlineExceeded", err)
		}
		close(release)
		mustClose(t, b)
	})
}

func TestDefaultErrorHandlerPanics(t *testing.T) {
	b := New(1, time.Hour, WithCallback[int](CallbackFunc[int](func(context.Context, []int) error {
		return nil
	})))
	defer func() { _ = b.Close(context.Background()) }()
	boom := errors.New("boom")
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("default error handler did not panic")
		}
		err, ok := r.(error)
		if !ok || !errors.Is(err, boom) {
			t.Fatalf("panic value: got %v, want the callback error", r)
		}
	}()
	b.errh(context.Background(), boom)
}

func TestNewPanicsWithoutCallback(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New without a callback did not panic")
		}
	}()
	New[int](1, time.Second)
}

func TestCallbackFunc(t *testing.T) {
	boom := errors.New("boom")
	var got []int
	cb := CallbackFunc[int](func(_ context.Context, batch []int) error {
		got = slices.Clone(batch)
		return boom
	})
	if err := cb.Call(context.Background(), []int{1, 2}); !errors.Is(err, boom) {
		t.Fatalf("Call: got %v, want the adapted func's error", err)
	}
	if !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("batch passed through: got %v, want [1 2]", got)
	}
}

// flushEvent is one ObserveFlush call as recorded by observerRecorder.
type flushEvent struct {
	reason FlushReason
	size   int
	d      time.Duration
}

// observerRecorder records every observer event. It is safe for concurrent
// use because ObservePush arrives from pushing goroutines while the rest
// arrive from the delivery loop.
type observerRecorder struct {
	mu      sync.Mutex
	pushes  int
	flushes []flushEvent
	errs    []error
	drops   []int
}

func (o *observerRecorder) ObservePush() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pushes++
}

func (o *observerRecorder) ObserveFlush(reason FlushReason, size int, d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.flushes = append(o.flushes, flushEvent{reason, size, d})
}

func (o *observerRecorder) ObserveError(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.errs = append(o.errs, err)
}

func (o *observerRecorder) ObserveDrop(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.drops = append(o.drops, n)
}

func (o *observerRecorder) snapshot() observerRecorder {
	o.mu.Lock()
	defer o.mu.Unlock()
	return observerRecorder{
		pushes:  o.pushes,
		flushes: slices.Clone(o.flushes),
		errs:    slices.Clone(o.errs),
		drops:   slices.Clone(o.drops),
	}
}

func assertFlushes(t *testing.T, got, want []flushEvent) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("flush events: got %v, want %v", got, want)
	}
}

func TestObserverSizeFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		var o observerRecorder
		b := New(2, time.Hour, WithCallback[int](&r), WithObserver[int](&o))
		mustPush(t, b, 1)
		mustPush(t, b, 2)
		synctest.Wait()
		got := o.snapshot()
		if got.pushes != 2 {
			t.Fatalf("pushes observed: got %d, want 2", got.pushes)
		}
		assertFlushes(t, got.flushes, []flushEvent{{FlushReasonSize, 2, 0}})
		mustClose(t, b)
	})
}

func TestObserverIntervalFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const d = 5 * time.Second
		var r recorder
		var o observerRecorder
		b := New(3, d, WithCallback[int](&r), WithObserver[int](&o))
		mustPush(t, b, 1)
		time.Sleep(d)
		synctest.Wait()
		assertFlushes(t, o.snapshot().flushes, []flushEvent{{FlushReasonInterval, 1, 0}})
		mustClose(t, b)
	})
}

func TestObserverDrainFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		var o observerRecorder
		b := New(10, time.Hour, WithCallback[int](&r), WithObserver[int](&o))
		mustPush(t, b, 1)
		mustPush(t, b, 2)
		mustPush(t, b, 3)
		mustClose(t, b)
		assertFlushes(t, o.snapshot().flushes, []flushEvent{{FlushReasonDrain, 3, 0}})
	})
}

func TestObserverSizeFlushesDuringDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		var o observerRecorder
		b := New(2, time.Hour, WithBuffer[int](8), WithCallback[int](&r), WithObserver[int](&o))
		for i := 1; i <= 5; i++ {
			mustPush(t, b, i)
		}
		mustClose(t, b)
		assertFlushes(t, o.snapshot().flushes, []flushEvent{
			{FlushReasonSize, 2, 0},
			{FlushReasonSize, 2, 0},
			{FlushReasonDrain, 1, 0},
		})
	})
}

func TestObserverFlushDurationCoversFanOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const each = 2 * time.Second
		slow := CallbackFunc[int](func(context.Context, []int) error {
			time.Sleep(each)
			return nil
		})
		var o observerRecorder
		b := New(1, time.Hour,
			WithCallback[int](slow),
			WithCallback[int](slow),
			WithObserver[int](&o))
		mustPush(t, b, 1)
		mustClose(t, b)
		assertFlushes(t, o.snapshot().flushes, []flushEvent{{FlushReasonSize, 1, 2 * each}})
	})
}

func TestObserverError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("boom")
		var handled []error
		var o observerRecorder
		b := New(1, time.Hour,
			WithCallback[int](CallbackFunc[int](func(context.Context, []int) error {
				return boom
			})),
			WithCallback[int](CallbackFunc[int](func(context.Context, []int) error {
				return nil
			})),
			WithErrorHandler[int](func(_ context.Context, err error) {
				handled = append(handled, err)
			}),
			WithObserver[int](&o))
		mustPush(t, b, 1)
		mustClose(t, b)
		got := o.snapshot()
		if len(got.errs) != 1 || !errors.Is(got.errs[0], boom) {
			t.Fatalf("errors observed: got %v, want [boom]", got.errs)
		}
		if len(handled) != 1 || !errors.Is(handled[0], boom) {
			t.Fatalf("error handler still runs: got %v, want [boom]", handled)
		}
		assertFlushes(t, got.flushes, []flushEvent{{FlushReasonSize, 1, 0}})
	})
}

func TestObserverDropOnCloseExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var o observerRecorder
		b := New(1, time.Hour,
			WithBuffer[int](1),
			WithCallback[int](CallbackFunc[int](func(cbCtx context.Context, _ []int) error {
				<-cbCtx.Done()
				return nil
			})),
			WithObserver[int](&o))
		mustPush(t, b, 1)
		synctest.Wait()
		mustPush(t, b, 2)
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := b.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close with expired ctx: got %v, want DeadlineExceeded", err)
		}
		synctest.Wait()
		got := o.snapshot()
		if got.pushes != 2 {
			t.Fatalf("pushes observed: got %d, want 2", got.pushes)
		}
		if !slices.Equal(got.drops, []int{1}) {
			t.Fatalf("drops observed: got %v, want [1]", got.drops)
		}
		assertFlushes(t, got.flushes, []flushEvent{{FlushReasonSize, 1, time.Second}})
	})
}

func TestObserverNoDropOnCleanClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		var o observerRecorder
		b := New(10, time.Hour, WithCallback[int](&r), WithObserver[int](&o))
		mustPush(t, b, 1)
		mustClose(t, b)
		if got := o.snapshot().drops; len(got) != 0 {
			t.Fatalf("drops observed on clean close: got %v, want none", got)
		}
	})
}

// eventLog is a mutex-guarded shared log the tagging observers append to.
// Push events arrive from the pushing goroutine and the rest from the
// delivery loop, so their relative order across categories is scheduling;
// within one category the batcher promises registration order.
type eventLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *eventLog) add(entry string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
}

func (l *eventLog) withSuffix(suffix string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, e := range l.entries {
		if strings.HasSuffix(e, suffix) {
			out = append(out, e)
		}
	}
	return out
}

// taggingObserver tags every event it receives into a shared eventLog.
type taggingObserver struct {
	tag string
	log *eventLog
}

func (o taggingObserver) ObservePush()                                 { o.log.add(o.tag + ":push") }
func (o taggingObserver) ObserveFlush(FlushReason, int, time.Duration) { o.log.add(o.tag + ":flush") }
func (o taggingObserver) ObserveError(error)                           { o.log.add(o.tag + ":error") }
func (o taggingObserver) ObserveDrop(int)                              { o.log.add(o.tag + ":drop") }

func TestObserversAllReceiveEventsInRegistrationOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("boom")
		var log eventLog
		b := New(1, time.Hour,
			WithCallback[int](CallbackFunc[int](func(context.Context, []int) error {
				return boom
			})),
			WithErrorHandler[int](func(context.Context, error) {}),
			WithObserver[int](taggingObserver{"a", &log}),
			WithObserver[int](taggingObserver{"b", &log}))
		mustPush(t, b, 1)
		mustClose(t, b)
		for suffix, want := range map[string][]string{
			":push":  {"a:push", "b:push"},
			":error": {"a:error", "b:error"},
			":flush": {"a:flush", "b:flush"},
		} {
			if got := log.withSuffix(suffix); !slices.Equal(got, want) {
				t.Fatalf("%s events: got %v, want %v", suffix, got, want)
			}
		}
	})
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
