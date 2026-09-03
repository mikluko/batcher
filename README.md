# batcher

[![CI](https://github.com/mikluko/batcher/actions/workflows/ci.yml/badge.svg)](https://github.com/mikluko/batcher/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mikluko/batcher.svg)](https://pkg.go.dev/github.com/mikluko/batcher)

Generic batching for Go: accumulate pushed items and deliver them to callbacks when a batch fills up or its oldest item has waited long enough.

## Install

```sh
go get github.com/mikluko/batcher
```

Requires Go 1.26 or newer. The module depends on nothing beyond the standard library: `go.mod` carries no `require` directive, so nothing reaches your module graph, your `go.sum`, or your build. CI fails if that stops being true.

## Usage

```go
b := batcher.New(2, time.Minute,
    batcher.WithCallback(batcher.CallbackFunc[string](func(_ context.Context, batch []string) error {
        fmt.Println(batch)
        return nil
    })),
)

ctx := context.Background()
for _, v := range []string{"a", "b", "c"} {
    if err := b.Push(ctx, v); err != nil {
        fmt.Println("push:", err)
    }
}
if err := b.Close(ctx); err != nil {
    fmt.Println("close:", err)
}
// Output:
// [a b]
// [c]
```

## Semantics

- **Batching guarantee.** A batch flushes when it reaches the size limit or when its oldest item has waited out the interval, whichever comes first. Nothing runs while the batcher is idle. Every batch fans out to all registered callbacks sequentially, in registration order; the batch slice is owned by the batcher and must not be retained past the call.
- **Error policy.** Loud by default: a non-nil callback error panics unless `WithErrorHandler` installs a handler. An error from one callback does not skip the remaining callbacks for that batch.
- **Shutdown.** `Close` stops intake (`Push` returns `ErrClosed`) and drains everything already accepted through the callbacks as final batches. Only expiry of the context passed to `Close` abandons the remainder, returning its error; shutdown loss is always the caller's explicit deadline. `Close` is idempotent.

Full documentation and runnable examples: [pkg.go.dev/github.com/mikluko/batcher](https://pkg.go.dev/github.com/mikluko/batcher).

## Instrumentation

`WithObserver` takes an `Observer` and calls it on every accepted push, delivered batch, callback error, and item abandoned at shutdown. It is the whole instrumentation seam: with no observer registered, observation costs nothing.

```go
b := batcher.New(100, time.Second,
    batcher.WithCallback(cb),
    batcher.WithObserver[Item](obs),
)
```

Wiring an `Observer` to a metrics backend is the caller's, since the backend is the caller's. [`examples/prom`](examples/prom) is a worked Prometheus one: six collectors, registered on a `prometheus.Registerer`, served over `/metrics`. It is its own module, so its dependencies stay out of yours. Copy it and adjust the names, buckets, and labels to the tree it lands in.

| Metric | Type | Meaning |
|---|---|---|
| `batcher_items_total` | counter | Items accepted by `Push` |
| `batcher_batches_total{reason="size"\|"interval"\|"drain"}` | counter | Batches delivered, by flush reason |
| `batcher_batch_size` | histogram | Size of delivered batches |
| `batcher_flush_duration_seconds` | histogram | Duration of the callback fan-out per batch |
| `batcher_callback_errors_total` | counter | Non-nil callback errors |
| `batcher_items_dropped_total` | counter | Accepted items abandoned when the `Close` context expired |

## License

[MIT](LICENSE)
