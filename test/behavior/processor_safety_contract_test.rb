# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/processors"

class ProcessorSafetyContractTest < Minitest::Test
  def test_safe_processor_declares_ractor_safety_on_its_class
    assert SafeProcessor.ractor_safe?
    assert SafeProcessor.new.ractor_safe?
  end

  def test_failing_processor_can_be_accepted_before_it_fails_at_execution_time
    assert FailingProcessor.ractor_safe?
    assert FailingProcessor.new.ractor_safe?
  end

  def test_unsafe_processor_does_not_implicitly_opt_in
    refute UnsafeProcessor.ractor_safe?
    refute UnsafeProcessor.new.ractor_safe?
  end

  def test_worker_survives_processor_failure # rubocop:disable Metrics/MethodLength
    failing_event = CDC::Core::ChangeEvent.new(
      operation: :update,
      schema: "public",
      table: "failures",
      old_values: {},
      new_values: {},
      transaction_id: 1
    )

    passing_event = CDC::Core::ChangeEvent.new(
      operation: :update,
      schema: "public",
      table: "events",
      old_values: {},
      new_values: {},
      transaction_id: 2
    )

    pool = CDC::Parallel::ProcessorPool.new(
      processor: FlakyProcessor.new,
      size: 1
    )

    failed = pool.process(failing_event)
    passed = pool.process(passing_event)

    refute failed.success?
    assert passed.success?
  ensure
    pool&.shutdown
  end
end
