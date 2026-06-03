# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/processors"
require_relative "../support/events"

class ProcessorPoolPerformanceTest < Minitest::Test
  include EventFixtures

  EVENT_COUNT = 1_000

  # rubocop:disable Metrics/AbcSize
  def test_process_many_is_faster_than_repeated_process_for_batch_dispatch
    skip "set CDC_PARALLEL_PERFORMANCE_TESTS=1" unless ENV["CDC_PARALLEL_PERFORMANCE_TESTS"] == "1"

    events = Array.new(EVENT_COUNT) { change_event }
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: Etc.nprocessors)

    repeated_elapsed = measure do
      events.each { |event| assert pool.process(event).success? }
    end

    batch_elapsed = measure do
      results = pool.process_many(events)
      assert_equal EVENT_COUNT, results.length
      assert results.all?(&:success?)
    end

    assert_operator batch_elapsed, :<, repeated_elapsed
  ensure
    pool&.shutdown
  end
  # rubocop:enable Metrics/AbcSize

  def test_process_many_completes_within_smoke_threshold
    skip "set CDC_PARALLEL_PERFORMANCE_TESTS=1" unless ENV["CDC_PARALLEL_PERFORMANCE_TESTS"] == "1"

    events = Array.new(EVENT_COUNT) { change_event }
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 2)

    elapsed = measure do
      results = pool.process_many(events)
      assert_equal EVENT_COUNT, results.length
      assert results.all?(&:success?)
    end

    assert_operator elapsed, :<, 10.0
  ensure
    pool&.shutdown
  end

  private

  def measure
    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
  end
end
