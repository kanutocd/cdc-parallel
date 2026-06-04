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

...

## Example Output

...

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
