// Package prom exposes a batcher's observer events as Prometheus metrics.
//
// Each option constructs a batcher.Observer backed by six collectors,
// registers them on a prometheus.Registerer with MustRegister semantics
// (registering the same metrics twice panics), and returns the
// batcher.Option that installs the observer:
//
//	b := batcher.New(100, time.Second,
//	    batcher.WithCallback(cb),
//	    prom.WithRegisterer[Item](reg),
//	)
//
// Because the options are generic over the batcher's item type and take no
// argument that names it, the type argument is spelled explicitly:
// prom.WithRegisterer[Item](reg), prom.WithDefaultRegisterer[Item]().
//
// The metric names carry no instance label. To tell several batchers in one
// process apart, wrap the registerer per instance:
//
//	prom.WithRegisterer[Item](prometheus.WrapRegistererWith(
//	    prometheus.Labels{"batcher": "audit-log"}, reg,
//	))
package prom

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mikluko/batcher"
)

// WithRegisterer returns a batcher option that registers the metric set on r
// and installs the observer feeding it. Registration panics if any of the
// metrics is already registered on r.
func WithRegisterer[T any](r prometheus.Registerer) batcher.Option[T] {
	obs := newObserver()
	obs.register(r)
	return batcher.WithObserver[T](obs)
}

// WithDefaultRegisterer returns a batcher option that registers the metric
// set on prometheus.DefaultRegisterer and installs the observer feeding it.
// Registration panics if any of the metrics is already registered there.
func WithDefaultRegisterer[T any]() batcher.Option[T] {
	return WithRegisterer[T](prometheus.DefaultRegisterer)
}

// observer implements batcher.Observer over the six collectors. All
// underlying Prometheus metric types are safe for concurrent use, so the
// observer is too.
type observer struct {
	items         prometheus.Counter
	batches       *prometheus.CounterVec
	batchSize     prometheus.Histogram
	flushDuration prometheus.Histogram
	errors        prometheus.Counter
	dropped       prometheus.Counter
}

// newObserver builds the metric set. Flush duration uses
// prometheus.DefBuckets. Batch size uses exponential buckets 1..512
// (doubling), covering typical size limits from single items to the
// hundreds; a batch at or above the top bucket lands in +Inf, which still
// records the observation.
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

// register registers all collectors on r with MustRegister semantics.
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

func (o *observer) ObserveError(err error) {
	o.errors.Inc()
}

func (o *observer) ObserveDrop(n int) {
	o.dropped.Add(float64(n))
}
