# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/processors"
require_relative "../support/events"

class TransactionPoolTest < Minitest::Test
  include EventFixtures

  def test_processes_all_transaction_events_successfully
    pool = CDC::Parallel::TransactionPool.new(processor: SafeProcessor.new, size: 2)

    result = pool.process(transaction_with_tables("users", "orders"))

    assert result.success?
    assert_equal 2, result.event.length
    assert(result.event.all?(&:success?))
  ensure
    pool&.shutdown
  end

  def test_transaction_fails_when_any_event_fails
    pool = CDC::Parallel::TransactionPool.new(processor: FlakyProcessor.new, size: 2)

    result = pool.process(transaction_with_tables("users", "failures", "orders"))

    assert result.failure?
    assert_equal 3, result.event.length
    assert_equal [false, true, false], result.event.map(&:failure?)
    assert_instance_of CDC::Parallel::ProcessorExecutionError, result.error
  ensure
    pool&.shutdown
  end

  def test_transaction_failure_uses_diagnostic_error_when_event_failure_has_no_error
    pool = CDC::Parallel::TransactionPool.allocate
    event_results = [
      CDC::Core::ProcessorResult.new(:failure, error: nil),
      CDC::Core::ProcessorResult.success(change_event(table: "users"))
    ]
    processor_pool = Minitest::Mock.new
    processor_pool.expect(:process_many, event_results, [Array])
    pool.instance_variable_set(:@processor_pool, processor_pool)

    result = pool.process(transaction_with_tables("failures", "users"))

    assert result.failure?
    assert_instance_of CDC::Parallel::ProcessorExecutionError, result.error
    assert_equal "CDC::Core::ProcessorResult", result.error.original_class
    assert_equal event_results, result.event
    processor_pool.verify
  end

  private

  def transaction_with_tables(*tables)
    CDC::Core::TransactionEnvelope.new(
      transaction_id: 123,
      events: tables.map { |table| change_event(table:) },
      commit_lsn: "0/16B6C50",
      committed_at: Time.utc(2026, 5, 31)
    )
  end
end
