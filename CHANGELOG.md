# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## Unreleased

## [0.2.4] - 2026-06-28

### Fixed

- Started `Runtime` processors before internal pools shareablize the processor object graph.
- Forwarded `Runtime` supervisor tuning options to both event and transaction pools.

### Added

- Added fatal-worker supervisor regression tests for respawn, degraded cooldown, and shutdown behavior.
- Added runtime regression tests for lifecycle ordering and supervisor option forwarding.
- Updated RBS signatures and supervisor plan documentation for the new runtime contract.

## [0.2.3] - 2026-06-03

### Added

- Added Port-native `Ractor::Port` worker inbox dispatch for the pre-warmed processor pool.
- Added concurrent threaded caller regression coverage for `ProcessorPool#process_many`.
- Added worker inbox boot verification coverage.
- Added multi-trial processor-pool benchmark reporting with min, median, max, and p95 distributions.
- Added minimum measurement duration support for benchmark trials.
- Added worker-count sweep support through `BENCHMARK_WORKER_COUNTS`.
- Added benchmark comparison across serial execution, repeated `ProcessorPool#process`, and batched `ProcessorPool#process_many`.
- Added benchmark environment metadata for Ruby, platform, host, CPU count, and uname details.
- Added detailed benchmark methodology and report documentation under `benchmark/README.md`.

### Changed

- Updated worker dispatch to send work through worker-owned inbox ports instead of direct worker messages.
- Synchronized dispatch and shutdown with a mutex so multiple Ruby threads can submit work safely.
- Updated processor pool RBS signatures for worker inboxes and Port-native dispatch helpers.
- Expanded README documentation for the worker dispatch model.
- Updated README benchmark guidance to point to the detailed benchmark report documentation.
- Updated benchmark ratio reporting to compare median throughput against serial execution.

## [0.2.2] - 2026-06-03

### Changed

- Improved processor pool shutdown so workers are signaled and confirmed stopped where practical.
- Updated transaction processing so partial event failures fail the transaction result while preserving per-event results.
- Added CI validation for RBS signatures.

### Added

- Added regression coverage for shutdown after processed and pending work.
- Added regression coverage for timeout-bounded shutdown behavior.
- Added regression coverage for `process_many([])` returning a clean empty result.
- Added transaction pool coverage for successful and partially failed transactions.

## [0.2.1] - 2026-06-03

### Added

- Enforced processor timeout handling.
- Fixed transaction partial-failure behavior.
- Added regression coverage for hung processors and transaction failure cases.

### Changed

- Released a correctness and reliability patch.

## [0.2.0] - 2026-06-03

### Added

- Pre-warmed persistent Ractor worker pool implementation.
- `ProcessorPool#process_many` for batched dispatch.
- Tiny workload benchmark for dispatch overhead analysis.
- CPU-bound workload benchmark for throughput analysis.
- Batch workload benchmark for CDC-style event processing.
- Performance test suite guarded by `CDC_PARALLEL_PERFORMANCE_TESTS=1`.
- Reusable benchmark Docker image.
- `benchmark:processor_pool` Rake task.
- `benchmark:docker_build` Rake task.
- `benchmark:docker_run` Rake task.
- Benchmark documentation and reproducibility guidance.

### Changed

- Processor workers are now initialized once and reused for the lifetime of the pool.
- Benchmark methodology updated to measure pre-warmed worker execution.
- README updated with benchmark execution instructions and example results.

### Performance

Local benchmark results on Ruby 4.0.5 (4 workers) demonstrated measurable throughput improvements for CPU-bound workloads using pre-warmed worker pools compared to serial execution.

Benchmark results vary by hardware, operating system, Ruby version, and workload characteristics. Users are encouraged to reproduce results on their own systems using the included benchmark suite.

## [0.1.1] - 2026-06-03

No code changes.

Improves RubyGems metadata and documentation wording to
explicitly identify CDC as Change Data Capture.

## [0.1.0] - 2026-05-31

### Added

- Initial `CDC::Parallel` namespace.
- Added `CDC::Parallel::Runtime`.
- Added `CDC::Parallel::ProcessorPool`.
- Added `CDC::Parallel::TransactionPool`.
- Added `CDC::Parallel::Router`.
- Added `CDC::Parallel::ResultCollector`.
- Added Parallel-safe processor validation.
- Added support for `CDC::Core::ChangeEvent` processing.
- Added support for `CDC::Core::TransactionEnvelope` processing.
- Added graceful shutdown behavior.
- Added RBS signatures.
- Added Minitest suite.
- Added README and example.
- Added CI and release workflows.
