# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/processors"
require_relative "../support/events"

# Verifies that ProcessorPool, TransactionPool, and Runtime honour the
# cdc-core CDC::Core::Processor lifecycle contract: start / stop / flush /
# healthy?.
# rubocop:disable Metrics/ClassLength
class ProcessorLifecycleContractTest < Minitest::Test
  include EventFixtures

  def teardown
    LifecycleTrackingProcessor.reset_logs
  end

  # ---------------------------------------------------------------------------
  # ProcessorPool – standalone lifecycle management (manage_lifecycle: true)
  # ---------------------------------------------------------------------------

  def test_processor_pool_calls_start_before_workers_boot
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::ProcessorPool.new(processor:, size: 1)

    assert_equal 1, processor.start_count
  ensure
    pool&.shutdown
  end

  def test_processor_pool_calls_flush_and_stop_on_shutdown
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::ProcessorPool.new(processor:, size: 1)

    pool.shutdown

    assert_equal 1, processor.flush_count
    assert_equal 1, processor.stop_count
  end

  def test_processor_pool_calls_flush_before_stop
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::ProcessorPool.new(processor:, size: 1)

    pool.shutdown

    flush_pos = processor.calls.index(:flush)
    stop_pos  = processor.calls.index(:stop)
    assert flush_pos < stop_pos, "expected flush before stop, got: #{processor.calls.inspect}"
  end

  def test_processor_pool_shutdown_is_idempotent_for_lifecycle_hooks
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::ProcessorPool.new(processor:, size: 1)

    pool.shutdown
    pool.shutdown

    assert_equal 1, processor.flush_count
    assert_equal 1, processor.stop_count
  end

  def test_processor_pool_calls_start_exactly_once_regardless_of_pool_size
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::ProcessorPool.new(processor:, size: 4)

    assert_equal 1, processor.start_count
  ensure
    pool&.shutdown
  end

  def test_processor_pool_calls_stop_exactly_once_regardless_of_pool_size
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::ProcessorPool.new(processor:, size: 4)

    pool.shutdown

    assert_equal 1, processor.stop_count
  end

  # ---------------------------------------------------------------------------
  # ProcessorPool – manage_lifecycle: false (used when Runtime owns lifecycle)
  # ---------------------------------------------------------------------------

  def test_processor_pool_skips_start_when_manage_lifecycle_false
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::ProcessorPool.new(processor:, size: 1, manage_lifecycle: false)

    assert_equal 0, processor.start_count
  ensure
    pool&.shutdown
  end

  def test_processor_pool_skips_flush_and_stop_when_manage_lifecycle_false
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::ProcessorPool.new(processor:, size: 1, manage_lifecycle: false)

    pool.shutdown

    assert_equal 0, processor.flush_count
    assert_equal 0, processor.stop_count
  end

  # ---------------------------------------------------------------------------
  # TransactionPool – lifecycle delegation
  # ---------------------------------------------------------------------------

  def test_transaction_pool_calls_start_on_processor
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::TransactionPool.new(processor:, size: 1)

    assert_equal 1, processor.start_count
  ensure
    pool&.shutdown
  end

  def test_transaction_pool_calls_flush_and_stop_on_shutdown
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::TransactionPool.new(processor:, size: 1)

    pool.shutdown

    assert_equal 1, processor.flush_count
    assert_equal 1, processor.stop_count
  end

  def test_transaction_pool_skips_lifecycle_when_manage_lifecycle_false
    processor = LifecycleTrackingProcessor.new
    pool = CDC::Parallel::TransactionPool.new(processor:, size: 1, manage_lifecycle: false)

    pool.shutdown

    assert_equal 0, processor.start_count
    assert_equal 0, processor.flush_count
    assert_equal 0, processor.stop_count
  end

  # ---------------------------------------------------------------------------
  # Runtime – owns lifecycle; pools must not double-call hooks
  # ---------------------------------------------------------------------------

  def test_runtime_calls_start_exactly_once_despite_two_internal_pools
    processor = LifecycleTrackingProcessor.new
    runtime = CDC::Parallel::Runtime.new(processor:, size: 1)

    assert_equal 1, processor.start_count
  ensure
    runtime&.shutdown
  end

  def test_runtime_calls_flush_exactly_once_on_shutdown
    processor = LifecycleTrackingProcessor.new
    runtime = CDC::Parallel::Runtime.new(processor:, size: 1)

    runtime.shutdown

    assert_equal 1, processor.flush_count
  end

  def test_runtime_calls_stop_exactly_once_on_shutdown
    processor = LifecycleTrackingProcessor.new
    runtime = CDC::Parallel::Runtime.new(processor:, size: 1)

    runtime.shutdown

    assert_equal 1, processor.stop_count
  end

  def test_runtime_calls_flush_before_stop
    processor = LifecycleTrackingProcessor.new
    runtime = CDC::Parallel::Runtime.new(processor:, size: 1)

    runtime.shutdown

    flush_pos = processor.calls.index(:flush)
    stop_pos  = processor.calls.index(:stop)
    assert flush_pos < stop_pos, "expected flush before stop, got: #{processor.calls.inspect}"
  end

  def test_runtime_shutdown_is_idempotent_for_lifecycle_hooks
    processor = LifecycleTrackingProcessor.new
    runtime = CDC::Parallel::Runtime.new(processor:, size: 1)

    runtime.shutdown
    runtime.shutdown

    assert_equal 1, processor.flush_count
    assert_equal 1, processor.stop_count
  end

  def test_runtime_processes_work_after_start_is_called
    processor = LifecycleTrackingProcessor.new
    runtime = CDC::Parallel::Runtime.new(processor:, size: 1)

    result = runtime.process(change_event)

    assert result.success?
    assert_equal 1, processor.start_count
  ensure
    runtime&.shutdown
  end

  # ---------------------------------------------------------------------------
  # healthy? – contract recognition (not gated in dispatch today, but must be
  #            accessible so higher layers can consult it)
  # ---------------------------------------------------------------------------

  def test_unhealthy_processor_reports_not_healthy
    refute UnhealthyProcessor.new.healthy?
  end

  def test_safe_processor_reports_healthy_by_default
    assert SafeProcessor.new.healthy?
  end

  def test_lifecycle_tracking_processor_reports_healthy_by_default
    assert LifecycleTrackingProcessor.new.healthy?
  end
end
# rubocop:enable Metrics/ClassLength
