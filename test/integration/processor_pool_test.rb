# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/processors"
require_relative "../support/events"

class ProcessorPoolTest < Minitest::Test
  include EventFixtures

  def test_processes_event
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)

    result = pool.process(change_event)

    assert result.success?
    assert_equal :update, result.event[:operation]
  ensure
    pool&.shutdown
  end

  def test_processes_many_events_in_input_order
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 2)
    events = [change_event, change_event]

    results = pool.process_many(events)

    assert_equal 2, results.length
    assert results.all?(&:success?)
    assert_equal(%i[update update], results.map { |result| result.event[:operation] })
  ensure
    pool&.shutdown
  end

  def test_accepts_concurrent_threaded_callers
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 2)
    threads = 4.times.map do
      Thread.new { pool.process_many(Array.new(10) { change_event }) }
    end

    results = threads.flat_map(&:value)

    assert_equal 40, results.length
    assert results.all?(&:success?)
  ensure
    pool&.shutdown
    threads&.each(&:join)
  end

  def test_wraps_processor_error
    pool = CDC::Parallel::ProcessorPool.new(processor: FailingProcessor.new, size: 1)

    result = pool.process(change_event)

    assert result.failure?
    assert_instance_of CDC::Parallel::ProcessorExecutionError, result.error
    assert_equal "RuntimeError", result.error.original_class
    assert_equal "boom", result.error.original_message
  ensure
    pool&.shutdown
  end

  def test_worker_stays_alive_after_processor_error
    pool = CDC::Parallel::ProcessorPool.new(processor: ConditionalFailingProcessor.new, size: 1)

    failed = pool.process(change_event(table: "boom"))
    succeeded = pool.process(change_event(table: "users"))

    assert failed.failure?
    assert succeeded.success?
    assert_equal "users", succeeded.event[:table]
  ensure
    pool&.shutdown
  end

  def test_times_out_when_worker_does_not_reply_before_deadline
    pool = CDC::Parallel::ProcessorPool.new(processor: SlowProcessor.new, size: 1, timeout: 0.001)

    result = pool.process(change_event)

    assert result.failure?
    assert_instance_of CDC::Parallel::TimeoutError, result.error
    assert_match "processor pool timed out", result.error.message
  ensure
    pool&.shutdown
  end

  def test_worker_survives_late_reply_after_timeout
    pool = CDC::Parallel::ProcessorPool.new(processor: ConditionalSlowProcessor.new, size: 1, timeout: 0.001)

    timed_out = pool.process(change_event(table: "slow"))
    sleep 0.1

    succeeded = pool.process(change_event)

    assert timed_out.failure?
    assert succeeded.success?
  ensure
    pool&.shutdown
  end

  def test_shutdown_waits_for_pending_work_to_finish
    pool = CDC::Parallel::ProcessorPool.new(processor: SlowProcessor.new, size: 1)
    workers = pool.instance_variable_get(:@workers)
    result = process_during_shutdown(pool)

    assert result.success?
    assert(workers.all? { |worker| worker.inspect.include?("terminated") })
  ensure
    pool&.shutdown
  end

  def test_shutdown_with_timeout_does_not_wait_indefinitely_for_pending_work
    pool = CDC::Parallel::ProcessorPool.new(processor: SlowProcessor.new, size: 1, timeout: 0.001)

    pool.process(change_event)
    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    pool.shutdown

    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at, :<, 0.05
  ensure
    pool&.shutdown
  end

  private

  def process_during_shutdown(pool)
    thread = Thread.new { pool.process(change_event) }
    sleep 0.005
    pool.shutdown
    thread.value
  ensure
    thread&.join
  end
end
