# frozen_string_literal: true

require "bundler/setup"
require "json"
require "socket"
require "time"
require "etc"

require "cdc_core"
require "cdc_parallel"
require "ratomic"

module RatomicCDCParallelPOC
  module_function

  Config = Data.define(
    :events,
    :warmup,
    :workers,
    :trials,
    :tables
  )

  Trial = Data.define(
    :elapsed,
    :events_processed,
    :events_failed,
    :metrics
  )

  TABLES = %w[users orders invoices audit_logs].freeze
  OPERATIONS = %i[insert update delete].freeze

  def integer_env(name, default)
    Integer(ENV.fetch(name, default.to_s))
  rescue ArgumentError
    warn "#{name} must be an integer"
    exit 1
  end

  def positive_integer_env(name, default)
    value = integer_env(name, default)
    return value if value.positive?

    warn "#{name} must be greater than zero"
    exit 1
  end

  def config
    Config.new(
      events: positive_integer_env("RATOMIC_POC_EVENTS", 10_000),
      warmup: positive_integer_env("RATOMIC_POC_WARMUP", 1_000),
      workers: positive_integer_env("RATOMIC_POC_WORKERS", Etc.nprocessors),
      trials: positive_integer_env("RATOMIC_POC_TRIALS", 5),
      tables: TABLES
    )
  end

  def monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def change_event(index)
    table = TABLES[index % TABLES.length]
    operation = OPERATIONS[index % OPERATIONS.length]

    Ractor.make_shareable(
      CDC::Core::ChangeEvent.new(
        operation: operation,
        schema: "public",
        table: table,
        old_values: { "id" => index, "status" => "old" },
        new_values: { "id" => index, "status" => "new" },
        transaction_id: index
      )
    )
  end

  def events(count)
    Ractor.make_shareable(Array.new(count) { |index| change_event(index + 1) })
  end

  class MetricsProcessor < CDC::Core::Processor
    ractor_safe!

    def initialize(metrics:)
      @metrics = metrics
      super()
      Ractor.make_shareable(self)
    end

    def process(event)
      @metrics.increment("events.total")
      @metrics.increment("operations.#{event.operation}")
      @metrics.increment("tables.#{event.table}")
      @metrics.increment("routes.public.#{event.table}.#{event.operation}")

      CDC::Core::ProcessorResult.success(
        Ractor.make_shareable(
          {
            operation: event.operation,
            table: event.table,
            transaction_id: event.transaction_id
          }
        )
      )
    rescue StandardError => e
      @metrics.increment("events.failed")
      CDC::Core::ProcessorResult.failure(e)
    end
  end

  class RatomicMetrics
    def initialize(keys:)
      @keys = Ractor.make_shareable(keys.map(&:dup).freeze)
      @counters = Ratomic::Map.new
      @keys.each { |key| @counters.set(key, Ratomic::Counter.new) }

      freeze
      Ractor.make_shareable(self)
    end

    def increment(key)
      @counters.get(key).increment(1)
    end

    def snapshot
      result = {}

      @keys.each do |key|
        result[key] = @counters.get(key).read
      end

      Ractor.make_shareable(result)
    end
  end

  def metric_keys(settings)
    [
      "events.total",
      "events.failed",
      *OPERATIONS.map { |operation| "operations.#{operation}" },
      *settings.tables.map { |table| "tables.#{table}" },
      *settings.tables.flat_map do |table|
        OPERATIONS.map { |operation| "routes.public.#{table}.#{operation}" }
      end
    ].freeze
  end

  def run_trial(settings)
    keys = metric_keys(settings)
    warmup_events = events(settings.warmup)
    benchmark_events = events(settings.events)

    warmup_pool = CDC::Parallel::ProcessorPool.new(
      processor: MetricsProcessor.new(metrics: RatomicMetrics.new(keys: keys)),
      size: settings.workers
    )
    warmup_pool.process_many(warmup_events)
    warmup_pool.shutdown

    metrics = RatomicMetrics.new(keys: keys)
    pool = CDC::Parallel::ProcessorPool.new(
      processor: MetricsProcessor.new(metrics: metrics),
      size: settings.workers
    )

    started_at = monotonic
    results = pool.process_many(benchmark_events)
    elapsed = monotonic - started_at

    failed = results.count { |result| !result.success? }

    Trial.new(
      elapsed: elapsed,
      events_processed: results.length,
      events_failed: failed,
      metrics: metrics.snapshot
    )
  ensure
    warmup_pool&.shutdown
    pool&.shutdown
  end

  def run_trials(settings)
    Array.new(settings.trials) { run_trial(settings) }
  end

  def report(settings, trials)
    {
      benchmark: "ratomic_cdc_parallel_poc",
      gem: "cdc-parallel",
      purpose: "Ractor workers update shared Ratomic metrics while processing CDC events",
      timestamp: Time.now.utc.iso8601,
      environment: environment,
      config: {
        events: settings.events,
        warmup: settings.warmup,
        workers: settings.workers,
        trials: settings.trials,
        tables: settings.tables
      },
      summary: summarize_trials(trials),
      raw_trials: trials.map { |trial| trial_report(trial) }
    }
  end

  def environment
    {
      ruby: RUBY_DESCRIPTION,
      platform: RUBY_PLATFORM,
      hostname: Socket.gethostname,
      cpu_count: Etc.nprocessors
    }
  end

  def summarize_trials(trials)
    throughputs = trials.map { |trial| trial.events_processed / trial.elapsed }

    {
      elapsed_seconds: distribution(trials.map(&:elapsed)),
      events_per_second: distribution(throughputs),
      total_events_processed: trials.sum(&:events_processed),
      total_events_failed: trials.sum(&:events_failed)
    }
  end

  def trial_report(trial)
    {
      elapsed_seconds: trial.elapsed.round(6),
      events_processed: trial.events_processed,
      events_failed: trial.events_failed,
      events_per_second: (trial.events_processed / trial.elapsed).round(2),
      metrics: trial.metrics
    }
  end

  def distribution(values)
    sorted = values.sort

    {
      min: format_stat(sorted.first),
      median: format_stat(median(sorted)),
      max: format_stat(sorted.last),
      p95: format_stat(percentile(sorted, 95))
    }
  end

  def median(values)
    sorted = values.sort
    mid = sorted.length / 2

    return sorted[mid] if sorted.length.odd?

    (sorted[mid - 1] + sorted[mid]) / 2.0
  end

  def percentile(sorted_values, percentile)
    index = ((percentile / 100.0) * (sorted_values.length - 1)).ceil
    sorted_values[index]
  end

  def format_stat(value)
    value.is_a?(Integer) ? value : value.round(6)
  end

  def run
    settings = config
    trials = run_trials(settings)

    puts JSON.pretty_generate(report(settings, trials))
  end
end

RatomicCDCParallelPOC.run
