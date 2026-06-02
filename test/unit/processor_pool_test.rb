# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/processors"
require_relative "../support/events"

class ProcessorPoolUnitTest < Minitest::Test
  include EventFixtures

  FakeWorkerWithoutValue = Struct.new(:result) do
    def take = result
  end

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

  def test_take_falls_back_to_take_when_worker_has_no_value_method
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)
    result = CDC::Core::ProcessorResult.success(:ok)
    worker = FakeWorkerWithoutValue.new(result)

    assert_same result, pool.send(:take, worker)
  ensure
    pool&.shutdown
  end
end
