# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/processors"
require_relative "../support/events"

class RuntimeTest < Minitest::Test
  include EventFixtures

  def test_processes_change_event
    runtime = CDC::Parallel::Runtime.new(processor: SafeProcessor.new, size: 2)

    result = runtime.process(change_event)

    assert result.success?
    assert_equal "users", result.event[:table]
  ensure
    runtime&.shutdown
  end

  def test_processes_transaction_envelope
    runtime = CDC::Parallel::Runtime.new(processor: SafeProcessor.new, size: 2)

    result = runtime.process_transaction(transaction)

    assert result.success?
    assert_equal 1, result.event.length
  ensure
    runtime&.shutdown
  end

  def test_process_routes_transaction_envelope
    runtime = CDC::Parallel::Runtime.new(processor: SafeProcessor.new, size: 2)

    result = runtime.process(transaction)

    assert result.success?
    assert_equal 1, result.event.length
  ensure
    runtime&.shutdown
  end

  def test_rejects_unsafe_processor
    assert_raises(CDC::Parallel::UnsafeProcessorError) do
      CDC::Parallel::Runtime.new(processor: UnsafeProcessor.new, size: 1)
    end
  end

  def test_rejects_processing_after_shutdown
    runtime = CDC::Parallel::Runtime.new(processor: SafeProcessor.new, size: 1)
    runtime.shutdown

    assert_raises(CDC::Parallel::ShutdownError) { runtime.process(change_event) }
  end

  def test_shutdown_is_idempotent
    runtime = CDC::Parallel::Runtime.new(processor: SafeProcessor.new, size: 1)

    runtime.shutdown
    runtime.shutdown

    assert_raises(CDC::Parallel::ShutdownError) { runtime.process(change_event) }
  end
end
