// Command prom exposes a batcher's activity as Prometheus metrics: an
// observer backed by six collectors, registered on a prometheus.Registerer
// and installed with batcher.WithObserver.
//
// This is example code to copy rather than a package to import. It lives in
// its own module so that github.com/mikluko/batcher requires nothing beyond
// the standard library.
//
// Run it and scrape http://localhost:2112/metrics; interrupt it to see the
// drain path.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mikluko/batcher"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	obs := newObserver()
	obs.register(reg)

	b := batcher.New(100, time.Second,
		batcher.WithCallback(batcher.CallbackFunc[string](func(_ context.Context, batch []string) error {
			log.Printf("delivered %d items", len(batch))
			return nil
		})),
		batcher.WithObserver[string](obs),
	)

	go produce(ctx, b)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	srv := &http.Server{Addr: ":2112", Handler: mux}

	go func() {
		log.Println("serving metrics on :2112/metrics")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("serve: %v", err)
			stop()
		}
	}()

	<-ctx.Done()

	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdown); err != nil {
		log.Printf("shutdown: %v", err)
	}
	if err := b.Close(shutdown); err != nil {
		log.Printf("close: %v", err)
	}
}

// produce pushes items until ctx expires or the batcher stops accepting.
func produce(ctx context.Context, b *batcher.Batcher[string]) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.Push(ctx, fmt.Sprintf("item-%d", i)); err != nil {
				return
			}
		}
	}
}
