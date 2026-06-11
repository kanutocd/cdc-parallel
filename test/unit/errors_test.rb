# frozen_string_literal: true

require_relative "../test_helper"

class ErrorsTest < Minitest::Test
  def test_processor_execution_error_preserves_original_error_details
    error = CDC::Parallel::ProcessorExecutionError.new(
      original_class: "RuntimeError",
      original_message: "boom",
      original_backtrace: ["processor.rb:42:in `process'"]
    )

    assert_equal "RuntimeError", error.original_class
    assert_equal "boom", error.original_message
    assert_equal ["processor.rb:42:in `process'"], error.original_backtrace
    assert_equal "RuntimeError: boom", error.message
    assert_equal ["processor.rb:42:in `process'"], error.backtrace
  end

  def test_processor_execution_error_allows_empty_backtrace
    error = CDC::Parallel::ProcessorExecutionError.new(
      original_class: "RuntimeError",
      original_message: "boom"
    )

    assert_empty error.original_backtrace
    assert_nil error.backtrace
  end

  def test_processor_execution_error_freezes_backtrace
    backtrace = ["processor.rb:42:in `process'"]

    error = CDC::Parallel::ProcessorExecutionError.new(
      original_class: "RuntimeError",
      original_message: "boom",
      original_backtrace: backtrace
    )

    assert_predicate error.original_backtrace, :frozen?
  end
end
