package prom_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/mikluko/batcher"
	"github.com/mikluko/batcher/prom"
)

func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	m := findMetric(t, reg, name, labels)
	if m == nil {
		return 0
	}
	return m.GetCounter().GetValue()
}

func histogramValue(t *testing.T, reg *prometheus.Registry, name string) *dto.Histogram {
	t.Helper()
	m := findMetric(t, reg, name, nil)
	if m == nil {
		t.Fatalf("metric %s not found", name)
	}
	return m.GetHistogram()
}

func findMetric(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	metrics:
		for _, m := range mf.GetMetric() {
			for k, v := range labels {
				if !hasLabel(m, k, v) {
					continue metrics
				}
			}
			return m
		}
	}
	return nil
}

func hasLabel(m *dto.Metric, k, v string) bool {
	for _, p := range m.GetLabel() {
		if p.GetName() == k && p.GetValue() == v {
			return true
		}
	}
	return false
}

func checkCounter(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string, want float64) {
	t.Helper()
	if got := counterValue(t, reg, name, labels); got != want {
		t.Fatalf("%s%v: got %v, want %v", name, labels, got, want)
	}
}

func TestWithRegisterer(t *testing.T) {
	reg := prometheus.NewRegistry()
	b := batcher.New(2, time.Hour,
		batcher.WithCallback(batcher.CallbackFunc[int](func(context.Context, []int) error {
			return nil
		})),
		prom.WithRegisterer[int](reg),
	)

	ctx := context.Background()
	for i := range 5 {
		if err := b.Push(ctx, i); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	checkCounter(t, reg, "batcher_items_total", nil, 5)
	checkCounter(t, reg, "batcher_batches_total", map[string]string{"reason": "size"}, 2)
	checkCounter(t, reg, "batcher_batches_total", map[string]string{"reason": "drain"}, 1)
	checkCounter(t, reg, "batcher_batches_total", map[string]string{"reason": "interval"}, 0)
	checkCounter(t, reg, "batcher_callback_errors_total", nil, 0)
	checkCounter(t, reg, "batcher_items_dropped_total", nil, 0)

	size := histogramValue(t, reg, "batcher_batch_size")
	if got := size.GetSampleCount(); got != 3 {
		t.Fatalf("batch_size sample count: got %d, want 3", got)
	}
	if got := size.GetSampleSum(); got != 5 {
		t.Fatalf("batch_size sample sum: got %v, want 5", got)
	}

	dur := histogramValue(t, reg, "batcher_flush_duration_seconds")
	if got := dur.GetSampleCount(); got != 3 {
		t.Fatalf("flush_duration sample count: got %d, want 3", got)
	}
	if got := dur.GetSampleSum(); got < 0 {
		t.Fatalf("flush_duration sample sum: got %v, want >= 0", got)
	}
}

func TestWithRegistererIntervalReason(t *testing.T) {
	reg := prometheus.NewRegistry()
	flushed := make(chan struct{}, 1)
	b := batcher.New(10, 10*time.Millisecond,
		batcher.WithCallback(batcher.CallbackFunc[int](func(context.Context, []int) error {
			flushed <- struct{}{}
			return nil
		})),
		prom.WithRegisterer[int](reg),
	)

	ctx := context.Background()
	if err := b.Push(ctx, 1); err != nil {
		t.Fatalf("Push: %v", err)
	}
	<-flushed
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	checkCounter(t, reg, "batcher_batches_total", map[string]string{"reason": "interval"}, 1)
}

func TestWithRegistererCallbackErrors(t *testing.T) {
	reg := prometheus.NewRegistry()
	b := batcher.New(1, time.Hour,
		batcher.WithCallback(batcher.CallbackFunc[int](func(context.Context, []int) error {
			return errors.New("boom")
		})),
		batcher.WithErrorHandler[int](func(context.Context, error) {}),
		prom.WithRegisterer[int](reg),
	)

	ctx := context.Background()
	if err := b.Push(ctx, 1); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := b.Push(ctx, 2); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	checkCounter(t, reg, "batcher_callback_errors_total", nil, 2)
}

func TestWithRegistererDroppedItems(t *testing.T) {
	reg := prometheus.NewRegistry()
	started := make(chan struct{})
	b := batcher.New(1, time.Hour,
		batcher.WithCallback(batcher.CallbackFunc[int](func(ctx context.Context, _ []int) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})),
		batcher.WithErrorHandler[int](func(context.Context, error) {}),
		batcher.WithBuffer[int](8),
		prom.WithRegisterer[int](reg),
	)

	ctx := context.Background()
	if err := b.Push(ctx, 0); err != nil {
		t.Fatalf("Push: %v", err)
	}
	<-started
	for i := 1; i < 4; i++ {
		if err := b.Push(ctx, i); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Close(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close with expired ctx: got %v, want Canceled", err)
	}

	deadline := time.Now().Add(time.Second)
	for counterValue(t, reg, "batcher_items_dropped_total", nil) != 3 {
		if time.Now().After(deadline) {
			t.Fatalf("items_dropped: got %v, want 3", counterValue(t, reg, "batcher_items_dropped_total", nil))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWithRegistererDoubleRegistrationPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = prom.WithRegisterer[int](reg)
	defer func() {
		if recover() == nil {
			t.Fatal("second WithRegisterer on the same registry did not panic")
		}
	}()
	prom.WithRegisterer[int](reg)
}

func TestWithDefaultRegisterer(t *testing.T) {
	reg := prometheus.NewRegistry()
	orig := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = reg
	t.Cleanup(func() { prometheus.DefaultRegisterer = orig })

	b := batcher.New(1, time.Hour,
		batcher.WithCallback(batcher.CallbackFunc[int](func(context.Context, []int) error {
			return nil
		})),
		prom.WithDefaultRegisterer[int](),
	)

	ctx := context.Background()
	if err := b.Push(ctx, 1); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	n, err := testutil.GatherAndCount(reg, "batcher_items_total")
	if err != nil {
		t.Fatalf("GatherAndCount: %v", err)
	}
	if n != 1 {
		t.Fatalf("batcher_items_total series: got %d, want 1", n)
	}
	checkCounter(t, reg, "batcher_items_total", nil, 1)
}
