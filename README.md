# cdc-parallel

[![Gem Version](https://badge.fury.io/rb/cdc-parallel.svg)](https://badge.fury.io/rb/cdc-parallel)
[![CI](https://github.com/kanutocd/cdc-parallel/workflows/CI/badge.svg)](https://github.com/kanutocd/cdc-parallel/actions)
[![Coverage Status](https://codecov.io/gh/kanutocd/cdc-parallel/branch/main/graph/badge.svg)](https://codecov.io/gh/kanutocd/cdc-parallel)
[![Ruby Version](https://img.shields.io/badge/ruby-%3E%3D%204.0-ruby.svg)](https://www.ruby-lang.org/en/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Optional high-throughput Ractor runtime for `cdc-core`.

`cdc-parallel` executes `CDC::Core::Processor` objects in Ractors when those processors explicitly declare themselves Ractor-safe.

## Requirements

- Ruby 4.0+
- `cdc-core`
- `parallel-pool`

Ruby 4.0+ is required because this gem targets the stabilized Ruby Ractor API.

## Purpose

```text
cdc-core
   │
   ▼
cdc-parallel
   │
   ▼
parallel Parallel-aware processing
```

`cdc-parallel` is a runtime adapter. It does not define CDC events and does not parse database streams.

## Installation

```ruby
gem "cdc-parallel"
```

## Usage

```ruby
require "cdc/core"
require "cdc/parallel"

class MetricsProcessor < CDC::Core::Processor
  ractor_safe!

  def process(event)
    CDC::Core::ProcessorResult.success(
      table: event.table,
      operation: event.operation
    )
  end
end

runtime =
  CDC::Parallel::Runtime.new(
    processor: MetricsProcessor.new,
    size: 4
  )

result = runtime.process(event)

runtime.shutdown
```

## Processor Safety

Only processors that declare `ractor_safe!` can run in this runtime.

```ruby
class AnalyticsProcessor < CDC::Core::Processor
  ractor_safe!
end
```

Unsafe processors raise:

```ruby
CDC::Parallel::UnsafeProcessorError
```

## What Belongs Here

- Ractor processor execution
- Transaction envelope processing
- Processor safety validation
- Graceful shutdown
- Result normalization

## What Does Not Belong Here

- PostgreSQL connection handling
- pgoutput parsing
- pgoutput decoding
- Rails integration
- Audit persistence
- Kafka/Redis/S3 publishing

## Ecosystem Position

```text
cdc-parallel
      │
      ▼
pgoutput-parser
      │
      ▼
pgoutput-decoder
      │
      ▼
cdc-core
      │
      ▼
cdc-parallel
      │
      ▼
whodunit-chronicles
```

## Roadmap

- Persistent worker pools using `parallel-pool`
- Mixed `CompositeProcessor` routing
- Ratomic-backed queues
- Ratomic-backed metrics
- Backpressure policies
- Transaction ordering strategies


## Test Organization

The test suite is grouped by intent so the same structure can be reused across CDC ecosystem gems.

```text
test/unit/          focused class and branch coverage
test/integration/   component interaction and runtime integration
test/behavior/      ecosystem contracts and guardrails
test/performance/   opt-in smoke benchmarks
```

Run the default quality suite:

```bash
bundle exec rake test
```

Run a specific group:

```bash
bundle exec rake test:unit
bundle exec rake test:integration
bundle exec rake test:behavior
bundle exec rake test:performance
```

The default `test` task runs unit, integration, and behavior tests. Performance tests are intentionally separate because they are environment-sensitive.

## License

MIT.


## Benchmarking

`cdc-parallel` includes reproducible benchmarks that compare serial processor execution against the pre-warmed Ractor worker pool.

The benchmark focuses on three workload categories:

| Workload | Purpose                                         |
| -------- | ----------------------------------------------- |
| tiny     | Measure dispatch overhead                       |
| cpu      | Measure CPU-bound processing throughput         |
| batch    | Measure batched CDC event processing throughput |

### Running Benchmarks

Tiny workload:

```bash
BENCHMARK_WORKLOAD=tiny \
bundle exec rake benchmark:processor_pool
```

CPU-bound workload:

```bash
BENCHMARK_WORKLOAD=cpu \
BENCHMARK_CPU_ROUNDS=5000 \
bundle exec rake benchmark:processor_pool
```

Batch workload:

```bash
BENCHMARK_WORKLOAD=batch \
BENCHMARK_BATCH_SIZE=10000 \
bundle exec rake benchmark:processor_pool
```

### Benchmark Docker Image

Build and run the reusable Docker image:

```bash
bundle exec rake benchmark:docker_build
bundle exec rake benchmark:docker_run
```

Or run the image directly after it is published to GitHub Container Registry:

```bash
docker run --rm ghcr.io/kanutocd/cdc-parallel-benchmark:main
```

The benchmark image is intended to become the shared performance validation
pattern across CDC Ecosystem gems, enabling reproducible benchmark execution
locally, in CI, and across different development environments.

### Example Result

Environment:

* Ruby 4.0.5
* x86_64 Linux
* 4 workers

CPU workload (`BENCHMARK_CPU_ROUNDS=5000`):

```json
{
  "serial": {
    "events_per_second": 120.26
  },
  "parallel": {
    "events_per_second": 250.15
  },
  "ratio": {
    "parallel_to_serial": 2.08
  }
}
```

### Interpretation

A ratio greater than `1.0` indicates that the pre-warmed Ractor worker pool outperformed serial execution.

```text
ratio > 1.0  => parallel faster
ratio = 1.0  => equivalent
ratio < 1.0  => serial faster
```

### Reproducibility

Benchmark results vary depending on:

* CPU model
* Core count
* Operating system
* Ruby version
* Background system activity

The benchmark suite is provided so that users can reproduce and validate results on their own hardware.
