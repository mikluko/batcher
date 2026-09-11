# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `PushWait` returns a `Ticket` that resolves once the pushed item's batch has flushed or been abandoned; `Ticket.Wait` blocks for it.
- `ErrAbandoned` is returned by `Ticket.Wait` when the item is dropped undelivered at shutdown.
- A callback can return `ItemErrors` instead of a plain error to attribute failure to individual items rather than the whole batch.
- `WithFlushConcurrency` lets up to M batches fill or flush at once, instead of always one at a time.

## [0.9.0] - 2026-09-03

### Removed

- **Breaking:** the `prom` subpackage is gone, so the module now depends on nothing beyond the standard library; [`examples/prom`](examples/prom) is a copyable starting point instead.

## [0.8.0] - 2026-08-14

### Added

- `WithObserver` registers an `Observer` notified of every push, delivered batch, callback error, and item dropped at shutdown.
- A `prom` subpackage registers these as Prometheus metrics.

## [0.7.0] - 2026-08-14

### Added

- `New` now returns an already-running `Batcher[T]`; `WithCallback`, `WithBuffer`, and `WithErrorHandler` configure it.
- `Close` stops intake and drains the remainder through the callbacks.
- `ErrClosed` is returned by `Push` once `Close` has been called.
- `Callback[T]` replaces a bare callback function; `CallbackFunc[T]` adapts a function to it.

### Removed

- **Breaking:** the `Batcher[T]` interface, `NewBuffer`, `Run`, `Flush`, `Wait`, and `Counters` are gone, replaced by the concrete `*Batcher[T]` above.

## [0.6.0] - 2024-09-28

### Changed

- **Breaking:** `Batcher` and `CallbackFunc` are now generic: `Batcher[T]`, `CallbackFunc[T]`.

## [0.5.0] - 2024-09-28

### Changed

- **Breaking:** the module path is now `github.com/mikluko/batcher` (previously `github.com/akabos/batcher`).

## [0.4.0] - 2020-11-03

### Changed

- **Breaking:** `Run` now blocks until delivery stops and returns its error directly.

### Removed

- **Breaking:** `Wait`, replaced by `Run`'s return value.

## [0.3.1] - 2020-06-05

### Fixed

- `Flush` no longer calls the callback with an empty batch.

## [0.3.0] - 2020-04-27

### Added

- `NewBuffer` takes an explicit intake buffer length, separate from the batch size.

### Changed

- `New`'s intake channel is now unbuffered; it used to buffer up to the batch size.

## [0.2.0] - 2020-04-27

### Added

- `Flush` delivers the buffered items immediately, without waiting for the size or interval limit.

## [0.1.1] - 2020-04-09

### Added

- `Counters` reports the cumulative number of items pushed and batches delivered.

### Fixed

- `Wait` no longer swallows its own context's deadline as a clean stop.

## [0.1.0] - 2020-04-09

### Added

- Initial release: `New` builds a `Batcher` that batches pushed values by count or by interval and delivers each batch to a callback; `Run` starts delivery, `Wait` blocks for it to stop.

[Unreleased]: https://github.com/mikluko/batcher/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/mikluko/batcher/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/mikluko/batcher/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/mikluko/batcher/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/mikluko/batcher/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/mikluko/batcher/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/mikluko/batcher/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/mikluko/batcher/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/mikluko/batcher/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/mikluko/batcher/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/mikluko/batcher/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mikluko/batcher/releases/tag/v0.1.0
