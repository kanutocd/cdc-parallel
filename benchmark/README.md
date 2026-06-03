# cdc-parallel Benchmarking

This directory contains the reproducible benchmark harness for `cdc-parallel`.

The benchmark compares direct serial processor execution against the
Port-backed, pre-warmed `CDC::Parallel::ProcessorPool`.

## Goals

The benchmark is designed to answer practical runtime questions:

- What is the dispatch overhead for tiny work?
- When does Ractor-backed parallel execution amortize its overhead?
- How much does batched `process_many` improve throughput?
- How does throughput change as worker count changes?
- Are results stable across multiple trials?

## Workloads

| Workload | Purpose                                 | Default options        |
| -------- | --------------------------------------- | ---------------------- |
| tiny     | Measures processor-pool dispatch cost   | none                   |
| cpu      | Measures CPU-bound processing throughput | `cpu_rounds: 250`      |
| batch    | Measures CDC-style batch throughput      | `batch_size: 100`      |

Tiny workloads intentionally do almost no work. They are useful for measuring
runtime overhead, but they are not expected to make parallel execution look
faster than direct method calls.

CPU and batch workloads are better indicators of useful parallel throughput.

## Execution Modes

The benchmark compares three execution modes.

| Mode             | Meaning                                    |
| ---------------- | ------------------------------------------ |
| serial           | Direct processor execution                 |
| repeated_process | Repeated `ProcessorPool#process` calls     |
| process_many     | Batched `ProcessorPool#process_many` calls |

`serial` is measured once per benchmark run. `repeated_process` and
`process_many` are measured once for each configured worker count.

## Configuration

| Environment variable       | Default          | Meaning                                      |
| -------------------------- | ---------------- | -------------------------------------------- |
| `BENCHMARK_WORKLOAD`       | `tiny`           | `tiny`, `cpu`, or `batch`                    |
| `BENCHMARK_ITERATIONS`     | `1000`           | Work items submitted per pass                |
| `BENCHMARK_WARMUP`         | `100`            | Warmup work items before measurement         |
| `BENCHMARK_TRIALS`         | `5`              | Number of measured trials                    |
| `BENCHMARK_MIN_DURATION`   | `0.1`            | Minimum seconds per trial                    |
| `BENCHMARK_WORKERS`        | `Etc.nprocessors` | Single worker count when no sweep is given |
| `BENCHMARK_WORKER_COUNTS`  | unset            | Comma-separated worker sweep, e.g. `1,2,4`  |
| `BENCHMARK_CPU_ROUNDS`     | `250`            | SHA256 rounds for the CPU workload           |
| `BENCHMARK_BATCH_SIZE`     | `100`            | Events inside each batch workload item       |

`BENCHMARK_WORKER_COUNTS` takes precedence over `BENCHMARK_WORKERS`.

## Examples

Run the default tiny workload:

```bash
bundle exec rake benchmark:processor_pool
```

Run the CPU workload with a worker-count sweep:

```bash
BENCHMARK_WORKLOAD=cpu \
BENCHMARK_WORKER_COUNTS=1,2,4 \
bundle exec rake benchmark:processor_pool
```

Run a longer benchmark:

```bash
BENCHMARK_WORKLOAD=batch \
BENCHMARK_TRIALS=9 \
BENCHMARK_MIN_DURATION=0.5 \
BENCHMARK_WORKER_COUNTS=1,2,4 \
bundle exec rake benchmark:processor_pool
```

## Report Shape

The benchmark prints JSON.

Top-level fields:

| Field            | Meaning                                      |
| ---------------- | -------------------------------------------- |
| `benchmark`      | Benchmark name                               |
| `gem`            | Gem name                                     |
| `timestamp`      | UTC timestamp                                |
| `environment`    | Ruby, platform, host, CPU, and uname metadata |
| `config`         | Benchmark configuration                      |
| `workload_options` | Workload-specific options                  |
| `serial`         | Serial execution distribution                |
| `worker_sweep`   | Parallel mode distributions by worker count  |
| `interpretation` | Ratio interpretation guide                   |

Each distribution includes:

| Field    | Meaning                          |
| -------- | -------------------------------- |
| `min`    | Fastest observed value           |
| `median` | Median observed value            |
| `max`    | Slowest observed value           |
| `p95`    | 95th percentile observed value   |

Each mode also includes `raw_trials` so results can be inspected or reprocessed.

## Abbreviated Output

```json
{
  "environment": {
    "ruby": "ruby 4.0.5 ...",
    "cpu_count": 4
  },
  "config": {
    "trials": 5,
    "min_duration_seconds": 0.1,
    "worker_counts": [1, 2, 4],
    "workload": "cpu"
  },
  "serial": {
    "events_per_second": {
      "min": 1810.12,
      "median": 1832.44,
      "max": 1855.91,
      "p95": 1855.91
    }
  },
  "worker_sweep": [
    {
      "workers": 4,
      "repeated_process": {
        "ratio_to_serial_median_events_per_second": 0.42,
        "interpretation": "serial faster"
      },
      "process_many": {
        "ratio_to_serial_median_events_per_second": 1.97,
        "interpretation": "parallel faster"
      }
    }
  ]
}
```

## Interpretation

`ratio_to_serial_median_events_per_second` compares a parallel mode's median
throughput against serial median throughput.

```text
ratio_to_serial_median_events_per_second > 1.0  => parallel mode faster
ratio_to_serial_median_events_per_second = 1.0  => equivalent
ratio_to_serial_median_events_per_second < 1.0  => serial faster
```

Tiny workloads primarily measure dispatch overhead, so serial execution may be
faster. CPU-bound and batched workloads are better indicators of useful
parallel throughput.

## Reproducibility

Benchmark results vary depending on:

- CPU model
- core count
- operating system
- Ruby version
- background system activity
- thermal and power-management state

Use multiple trials, a minimum measurement duration, and worker-count sweeps
when comparing results across machines or releases.
