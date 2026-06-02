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
