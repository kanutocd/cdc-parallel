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
end
