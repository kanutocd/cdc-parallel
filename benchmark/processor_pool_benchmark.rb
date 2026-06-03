# frozen_string_literal: true

require "bundler/setup"
require "digest"
require "json"
require "time"
require "cdc_parallel"
require "cdc_core"

# Reproducible processor-pool benchmark entrypoint.
module CDCParallelBenchmark # rubocop:disable Metrics/ModuleLength
  module_function

  Config = Data.define(:iterations, :warmup, :workers, :workload, :batch_size, :cpu_rounds)
  Timing = Data.define(:serial_elapsed, :parallel_elapsed)

  VALID_WORKLOADS = %w[tiny cpu batch].freeze

  def integer_env(name, default)
    value = ENV.fetch(name, default.to_s)
    Integer(value)
  rescue ArgumentError
    warn "#{name} must be an integer; got #{value.inspect}"
    exit 1
  end

  def workload_env
    workload = ENV.fetch("BENCHMARK_WORKLOAD", "tiny")

    return workload if VALID_WORKLOADS.include?(workload)

    warn "BENCHMARK_WORKLOAD must be one of #{VALID_WORKLOADS.join(", ")}; got #{workload.inspect}"
    exit 1
  end

  def monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def measure(iterations, &block)
    started_at = monotonic
    iterations.times(&block)
    monotonic - started_at
  end

  def config
    Config.new(
      iterations: integer_env("BENCHMARK_ITERATIONS", 1_000),
      warmup: integer_env("BENCHMARK_WARMUP", 100),
      workers: integer_env("BENCHMARK_WORKERS", Etc.nprocessors),
      workload: workload_env,
      batch_size: integer_env("BENCHMARK_BATCH_SIZE", 100),
      cpu_rounds: integer_env("BENCHMARK_CPU_ROUNDS", 250)
    )
  end

  def change_event(counter)
    CDC::Core::ChangeEvent.new(
      operation: :update,
      schema: "public",
      table: "benchmark_events",
      old_values: { "counter" => counter - 1 },
      new_values: { "counter" => counter },
      transaction_id: counter
    )
  end

  def event
    ::Ractor.make_shareable(change_event(42))
  end

  def batch_event(settings)
    events = Array.new(settings.batch_size) { |index| change_event(index + 1) }

    ::Ractor.make_shareable(events)
  end

  # Minimal Ractor-safe processor.
  #
  # This intentionally benchmarks the overhead of the processor pool itself.
  class TinyProcessor < CDC::Core::Processor
    ractor_safe!

    def process(event)
      payload = ::Ractor.make_shareable(
        {
          operation: event.operation,
          schema: event.schema,
          table: event.table,
          changed: event.new_values.keys
        }
      )

      CDC::Core::ProcessorResult.success(payload)
    end
  end

  # CPU-heavy processor.
  #
  # This is useful for finding the point where Ractor overhead is amortized.
  class CpuProcessor < CDC::Core::Processor
    ractor_safe!

    def initialize(rounds:)
      @rounds = rounds
      super()
      ::Ractor.make_shareable(self)
    end

    def process(event)
      input = "#{event.schema}.#{event.table}:#{event.transaction_id}:#{event.new_values.inspect}"

      digest = input
      @rounds.times do
        digest = Digest::SHA256.hexdigest(digest)
      end

      payload = ::Ractor.make_shareable(
        {
          operation: event.operation,
          table: event.table,
          digest: digest
        }
      )

      CDC::Core::ProcessorResult.success(payload)
    end
  end

  # Batch processor.
  #
  # This models CDC workloads where a runtime dispatch handles many events at once.
  class BatchProcessor < CDC::Core::Processor
    ractor_safe!

    def process(events)
      operations = Hash.new(0)
      tables = Hash.new(0)

      events.each do |event|
        operations[event.operation] += 1
        tables[event.table] += 1
      end

      payload = ::Ractor.make_shareable(
        {
          count: events.length,
          operations: operations,
          tables: tables
        }
      )

      CDC::Core::ProcessorResult.success(payload)
    end
  end

  def processor_for(settings)
    case settings.workload
    when "tiny"
      TinyProcessor.new
    when "cpu"
      CpuProcessor.new(rounds: settings.cpu_rounds)
    when "batch"
      BatchProcessor.new
    else
      raise "unsupported workload: #{settings.workload}"
    end
  end

  def sample_event_for(settings)
    case settings.workload
    when "batch"
      batch_event(settings)
    else
      event
    end
  end

  def serial_elapsed(processor, sample_event, settings)
    settings.warmup.times { processor.process(sample_event) }

    measure(settings.iterations) do
      result = processor.process(sample_event)
      raise "serial processor failed" unless result.success?
    end
  end

  def parallel_elapsed(pool, sample_event, settings)
    warmup_items = Array.new(settings.warmup) { sample_event }
    benchmark_items = Array.new(settings.iterations) { sample_event }

    pool.process_many(warmup_items)

    started_at = monotonic
    results = pool.process_many(benchmark_items)
    elapsed = monotonic - started_at

    raise "parallel processor failed" unless results.all?(&:success?)

    elapsed
  end

  def effective_events(settings)
    case settings.workload
    when "batch"
      settings.iterations * settings.batch_size
    else
      settings.iterations
    end
  end

  def report(settings, timing) # rubocop:disable Metrics/MethodLength
    {
      benchmark: "processor_pool",
      gem: "cdc-parallel",
      timestamp: Time.now.utc.iso8601,
      ruby: RUBY_DESCRIPTION,
      platform: RUBY_PLATFORM,
      workers: settings.workers,
      iterations: settings.iterations,
      warmup: settings.warmup,
      workload: settings.workload,
      workload_options: workload_options(settings),
      serial: measurement(effective_events(settings), timing.serial_elapsed),
      parallel: measurement(effective_events(settings), timing.parallel_elapsed),
      ratio: ratio(timing),
      interpretation: interpretation(timing),
      effective_events: effective_events(settings),
      parallel_model: "prewarmed_process_many"
    }
  end

  def workload_options(settings)
    case settings.workload
    when "tiny"
      {}
    when "cpu"
      { cpu_rounds: settings.cpu_rounds }
    when "batch"
      { batch_size: settings.batch_size }
    end
  end

  def measurement(iterations, elapsed)
    {
      elapsed_seconds: elapsed.round(6),
      events_per_second: (iterations / elapsed).round(2)
    }
  end

  def ratio(timing)
    { parallel_to_serial: (timing.serial_elapsed / timing.parallel_elapsed).round(4) }
  end

  def interpretation(timing)
    value = timing.serial_elapsed / timing.parallel_elapsed

    if value > 1
      "parallel faster"
    elsif value == 1
      "parallel equal to serial"
    else
      "serial faster"
    end
  end

  def run
    settings = config
    processor = processor_for(settings)
    sample_event = sample_event_for(settings)
    pool = CDC::Parallel::ProcessorPool.new(processor:, size: settings.workers)

    timing = Timing.new(
      serial_elapsed: serial_elapsed(processor, sample_event, settings),
      parallel_elapsed: parallel_elapsed(pool, sample_event, settings)
    )

    puts JSON.pretty_generate(report(settings, timing))
  ensure
    pool&.shutdown
  end
end

CDCParallelBenchmark.run
