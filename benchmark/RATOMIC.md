# Ratomic + cdc-parallel

This document demonstrates how Ractor workers can safely update
shared state using ratomic while processing CDC events through
cdc-parallel.

## Why

Most examples of Ractors focus on message passing.

This example explores an alternative pattern:

CDC Event
↓
ProcessorPool
↓
Ractor Workers
↓
Shared Metrics Registry
↓
Ratomic::Map
↓
Ratomic::Counter

## Running

```bash
RATOMIC_POC_EVENTS=100 RATOMIC_POC_WARMUP=10 RATOMIC_POC_TRIALS=1 RATOMIC_POC_WORKERS=2 bundle exec ruby benchmark/ratomic_cdc_parallel_poc.rb
```

## Example Result

```tttt

  - events_processed: 100
  - events_failed: 0
  - metrics["events.total"]: 100

```

## Ideas

- Metrics aggregation
- Shared counters
- Routing tables
- Backpressure indicators
- Runtime observability

## Future Experiments

- Ratomic::Queue
- Ratomic::Map routing tables
- Shared deduplication caches
- Worker coordination patterns
