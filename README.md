# batcher

[![CI](https://github.com/mikluko/batcher/actions/workflows/ci.yml/badge.svg)](https://github.com/mikluko/batcher/actions/workflows/ci.yml)

Generic batching for Go: accumulate pushed items and deliver them to callbacks when a batch fills up or its oldest item has waited long enough.

## Install

```sh
go get github.com/mikluko/batcher
```

Requires Go 1.26 or newer. The core package imports nothing beyond the standard library; the `prom` package carries `prometheus/client_golang`, and only importers of it compile the dependency.

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

## Metrics

`github.com/mikluko/batcher/prom` exposes a batcher's activity as Prometheus metrics. Two options wire it up; both are generic over the item type and spell the type argument explicitly:

```go
b := batcher.New(100, time.Second,
    batcher.WithCallback(cb),
    prom.WithRegisterer[Item](reg),      // register on a caller-supplied prometheus.Registerer
    // prom.WithDefaultRegisterer[Item]() // or on prometheus.DefaultRegisterer
)
```

Registration uses `MustRegister` semantics: registering the same metrics twice on one registerer panics.

| Metric | Type | Meaning |
|---|---|---|
| `batcher_items_total` | counter | Items accepted by `Push` |
| `batcher_batches_total{reason="size"\|"interval"\|"drain"}` | counter | Batches delivered, by flush reason |
| `batcher_batch_size` | histogram | Size of delivered batches |
| `batcher_flush_duration_seconds` | histogram | Duration of the callback fan-out per batch |
| `batcher_callback_errors_total` | counter | Non-nil callback errors |
| `batcher_items_dropped_total` | counter | Accepted items abandoned when the `Close` context expired |

The metrics carry no instance label. To tell several batchers in one process apart, wrap the registerer per instance:

```go
prom.WithRegisterer[Item](prometheus.WrapRegistererWith(
    prometheus.Labels{"batcher": "audit-log"}, reg,
))
```

## License

[MIT](LICENSE)
