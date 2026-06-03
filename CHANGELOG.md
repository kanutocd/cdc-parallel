# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [0.2.1] - 2026-06-03

### Added

  v0.2.1 - Correctness and reliability patch

  - Enforced processor timeout handling.
  - Fixed transaction partial-failure behavior.
  - Added regression coverage for hung processors and transaction failure cases.

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

## [0.1.1] - 2026-06-03

No code changes.

Improves RubyGems metadata and documentation wording to
explicitly identify CDC as Change Data Capture.