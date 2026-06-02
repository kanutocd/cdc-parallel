# frozen_string_literal: true

require_relative "../test_helper"

class ResultCollectorTest < Minitest::Test
  def test_normalizes_existing_processor_result
    original = CDC::Core::ProcessorResult.success(:ok)

    result = CDC::Parallel::ResultCollector.normalize(original)

    assert_same original, result
  end

  def test_wraps_plain_value_in_success_result
    result = CDC::Parallel::ResultCollector.normalize(:ok)

    assert result.success?
    assert_equal :ok, result.event
  end

  def test_worker_failure_payload_is_shareable
    error = RuntimeError.new("boom")
    error.set_backtrace(["worker.rb:1:in `process'"])

    payload = CDC::Parallel::ResultCollector.worker_failure(error)

    assert Ractor.shareable?(payload)
    assert_equal :__cdc_parallel_failure__, payload[:type]
    assert_equal "RuntimeError", payload[:class]
    assert_equal "boom", payload[:message]
    assert_equal ["worker.rb:1:in `process'"], payload[:backtrace]
  end

  def test_normalizes_worker_failure_payload
    payload = CDC::Parallel::ResultCollector.worker_failure(ArgumentError.new("bad input"))

    result = CDC::Parallel::ResultCollector.normalize(payload)

    assert result.failure?
    assert_instance_of CDC::Parallel::ProcessorExecutionError, result.error
    assert_equal "ArgumentError", result.error.original_class
    assert_equal "bad input", result.error.original_message
  end
end
