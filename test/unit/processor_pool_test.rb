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

  def test_prewarms_workers_during_initialization
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 2)
    workers = pool.instance_variable_get(:@workers)

    assert_equal 2, workers.length
    assert(workers.all? { |worker| worker.is_a?(::Ractor) })
  ensure
    pool&.shutdown
  end

  def test_shutdown_is_idempotent
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)

    pool.shutdown
    pool.shutdown

    assert true
  end
end
