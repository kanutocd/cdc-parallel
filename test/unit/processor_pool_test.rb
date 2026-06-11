# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/processors"
require_relative "../support/events"

class ProcessorPoolUnitTest < Minitest::Test
  include EventFixtures

  def test_rejects_unsafe_processor
    error = assert_raises(CDC::Parallel::UnsafeProcessorError) do
      CDC::Parallel::ProcessorPool.new(processor: UnsafeProcessor.new, size: 1)
    end

    assert_equal "UnsafeProcessor must declare ractor_safe!", error.message
  end

  def test_rejects_processing_after_shutdown
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)
    pool.shutdown

    assert_raises(CDC::Parallel::ShutdownError) { pool.process(change_event) }
  end

  def test_process_many_accepts_empty_batch
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)
    results = pool.process_many([])

    assert_empty results
    assert_predicate results, :frozen?
  ensure
    pool&.shutdown
  end

  def test_process_many_accepts_empty_batch_with_timeout
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1, timeout: 1)
    results = pool.process_many([])

    assert_empty results
    assert_predicate results, :frozen?
  ensure
    pool&.shutdown
  end

  def test_process_many_raises_when_item_cannot_be_shared
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)

    assert_raises(Ractor::IsolationError) { pool.process_many([proc {}]) }
  ensure
    pool&.shutdown
  end

  def test_timeout_result_when_deadline_has_already_expired
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1, timeout: 0.000000001)
    reply_port = ::Ractor::Port.new

    result = pool.send(:collect_results, reply_port, 1).fetch(0)

    assert result.failure?
    assert_instance_of CDC::Parallel::TimeoutError, result.error
  ensure
    reply_port&.close
    pool&.shutdown
  end

  def test_wait_for_workers_with_timeout_returns_when_timeout_is_nil
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)

    assert_nil pool.send(:wait_for_workers_with_timeout)
  ensure
    pool&.shutdown
  end

  def test_collect_results_with_timeout_returns_frozen_results_when_timeout_is_nil
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)
    reply_port = ::Ractor::Port.new
    results = []

    returned = pool.send(:collect_results_with_timeout, reply_port, results)

    assert_same results, returned
    assert_predicate returned, :frozen?
  ensure
    reply_port&.close
    pool&.shutdown
  end

  def test_prewarms_workers_during_initialization
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 2)
    workers = pool.instance_variable_get(:@workers)
    inboxes = pool.instance_variable_get(:@worker_inboxes)

    assert_equal 2, workers.length
    assert_equal 2, inboxes.length
    assert(workers.all? { |worker| worker.is_a?(::Ractor) })
    assert(inboxes.all? { |inbox| inbox.is_a?(::Ractor::Port) })
  ensure
    pool&.shutdown
  end

  def test_shutdown_is_idempotent
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)

    pool.shutdown
    pool.shutdown

    assert true
  end

  def test_shutdown_is_idempotent_with_timeout
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1, timeout: 1)

    pool.shutdown
    pool.shutdown

    assert true
  end

  def test_shutdown_confirms_workers_stopped_after_processed_work
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 2)
    workers = pool.instance_variable_get(:@workers)

    result = pool.process(change_event)
    pool.shutdown

    assert result.success?
    assert(workers.all? { |worker| terminated?(worker) })
  end

  private

  def terminated?(worker)
    worker.inspect.include?("terminated")
  end
end
