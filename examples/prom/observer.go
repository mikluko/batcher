package main

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mikluko/batcher"
)

// observer implements batcher.Observer over six collectors. Every Prometheus
// metric type below is safe for concurrent use, so the observer is too, which
// batcher.Observer requires of its implementations.
type observer struct {
	items         prometheus.Counter
	batches       *prometheus.CounterVec
	batchSize     prometheus.Histogram
	flushDuration prometheus.Histogram
	errors        prometheus.Counter
	dropped       prometheus.Counter
}

// newObserver builds the metric set. Batch size uses exponential buckets
// 1..512; a batch at or above the top bucket lands in +Inf, which still
// records the observation.
//
// The names carry no instance label. To tell several batchers in one process
// apart, register through prometheus.WrapRegistererWith.
func newObserver() *observer {
	return &observer{
		items: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batcher_items_total",
			Help: "Items accepted by Push.",
		}),
		batches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "batcher_batches_total",
			Help: "Batches delivered, by flush reason.",
		}, []string{"reason"}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batcher_batch_size",
			Help:    "Size of delivered batches.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		}),
		flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batcher_flush_duration_seconds",
			Help:    "Duration of the callback fan-out per delivered batch.",
			Buckets: prometheus.DefBuckets,
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batcher_callback_errors_total",
			Help: "Non-nil callback errors.",
		}),
		dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batcher_items_dropped_total",
			Help: "Accepted items abandoned when the context given to Close expired.",
		}),
	}
}

// register registers every collector on r, panicking if one of them is
// already registered there.
func (o *observer) register(r prometheus.Registerer) {
	r.MustRegister(o.items, o.batches, o.batchSize, o.flushDuration, o.errors, o.dropped)
}

func (o *observer) ObservePush() {
	o.items.Inc()
}

func (o *observer) ObserveFlush(reason batcher.FlushReason, size int, d time.Duration) {
	o.batches.WithLabelValues(string(reason)).Inc()
	o.batchSize.Observe(float64(size))
	o.flushDuration.Observe(d.Seconds())
}

func (o *observer) ObserveError(error) {
	o.errors.Inc()
}

func (o *observer) ObserveDrop(n int) {
	o.dropped.Add(float64(n))
}
