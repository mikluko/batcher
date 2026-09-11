# batcher

[![CI](https://github.com/mikluko/batcher/actions/workflows/release.yaml/badge.svg)](https://github.com/mikluko/batcher/actions/workflows/release.yaml)
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
- **Waiting for a flush.** `PushWait` returns a `Ticket` alongside the usual error; `Ticket.Wait` blocks until the item's batch has flushed (`nil` if nothing was attributed to that item, or the error attributed to it otherwise), is abandoned at shutdown (`ErrAbandoned`), or the `Wait` call's own context expires (`ctx.Err()`).
- **Per-item errors.** A callback returns `ItemErrors` instead of a plain error to attribute failure to specific items rather than the whole batch; entries left `nil` succeeded. A plain error still works and is attributed to every item, so waiters can tell which of their own pushes need a retry.
- **Flush concurrency.** By default (`WithFlushConcurrency(1)`) a batch never flushes while the next one is filling. `WithFlushConcurrency(M)` instead builds M independent sequential batchers sharing the same callbacks, observers, and error handler, and routes intake to one of them until it starts flushing (by reaching its size limit or timing out), then moves to the next one that isn't already flushing; `Push`/`PushWait` block once every one is, until one finishes. That costs two guarantees: `Callback.Call` and the `WithErrorHandler` handler may now run concurrently for different batches and must be safe for that, and batches are no longer guaranteed to complete in arrival order. `Close` closes every one concurrently against the same context and returns their errors joined (`errors.Join`).

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

## Changelog

[CHANGELOG.md](CHANGELOG.md) lists every release; the `Unreleased` section names what has landed on `main` since. A push there that names a new version cuts its tag automatically ([`mikluko/action-changelog`](https://github.com/mikluko/action-changelog)).

## License

[MIT](LICENSE)
