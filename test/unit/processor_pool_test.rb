# frozen_string_literal: true

require "timeout"
require_relative "../test_helper"
require_relative "../support/processors"
require_relative "../support/events"

class ProcessorPoolUnitTest < Minitest::Test # rubocop:disable Metrics/ClassLength
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

  def test_collect_results_without_timeout_does_not_require_assignments
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)
    reply_port = receive_reply_port_for([0, :ok])

    results = pool.send(:collect_results_without_timeout, reply_port, [nil])

    assert_predicate results.fetch(0), :success?
    assert_equal :ok, results.fetch(0).event
    assert_predicate results, :frozen?
  ensure
    pool&.shutdown
  end

  def test_collect_results_with_timeout_does_not_require_assignments
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1, timeout: 1)
    reply_port = receive_reply_port_for([0, :ok])

    results = pool.send(:collect_results_with_timeout, reply_port, [nil])

    assert_predicate results.fetch(0), :success?
    assert_equal :ok, results.fetch(0).event
    assert_predicate results, :frozen?
  ensure
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
    assert(workers.all? { |worker| worker.join.equal?(worker) })
  end

  def test_supervisor_respawns_worker_after_fatal_death
    pool = CDC::Parallel::ProcessorPool.new(processor: ConditionalFatalProcessor.new, size: 1, timeout: 1)

    fatal_result = pool.process(event_for_table("boom"))
    eventually { pool.respawns >= 1 }
    recovered_result = pool.process(change_event)

    assert fatal_result.failure?
    assert_operator pool.respawns, :>=, 1
    assert recovered_result.success?
    assert_equal "users", recovered_result.event[:table]
  ensure
    pool&.shutdown
  end

  def test_supervisor_respawns_only_the_crashed_slot
    pool = CDC::Parallel::ProcessorPool.new(processor: ConditionalFatalProcessor.new, size: 2, timeout: 1)

    fatal_result, healthy_result = pool.process_many([event_for_table("boom"), change_event])
    eventually { pool.respawns >= 1 }

    assert fatal_result.failure?
    assert healthy_result.success?
    assert_equal "users", healthy_result.event[:table]
    assert_operator pool.respawns, :>=, 1
    assert_operator pool.respawns, :<, 2
  ensure
    pool&.shutdown
  end

  def test_supervisor_does_not_respawn_after_shutdown
    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 1)
    pool.shutdown

    assert_equal 0, pool.respawns
    refute pool.degraded?
  end

  def test_supervisor_marks_slot_degraded_during_crash_loop_cooldown
    pool = CDC::Parallel::ProcessorPool.new(
      processor: FatalProcessor.new,
      size: 1,
      timeout: 1,
      max_respawns: 0,
      respawn_window: 60,
      respawn_cooldown: 0.2
    )

    result = pool.process(change_event)
    eventually { pool.degraded? }

    assert result.failure?
    assert pool.degraded?
  ensure
    pool&.shutdown
  end

  def test_worker_slot_fails_inflight_without_respawn_when_supervision_disabled
    slot = worker_slot(processor: ConditionalFatalProcessor.new, supervision: false)
    reply_port = ::Ractor::Port.new

    slot.send_work(0, event_for_table("boom"), reply_port)
    index, response = receive_from_port(reply_port)

    assert_equal 0, index
    result = CDC::Parallel::ResultCollector.normalize(response)

    assert result.failure?
    assert_equal 0, slot.respawns
    refute slot.degraded?
  ensure
    reply_port&.close
    slot&.shutdown
    slot&.join
  end

  # rubocop:disable Metrics/MethodLength
  def test_worker_slot_rejects_work_while_degraded
    slot = worker_slot
    reply_port = ::Ractor::Port.new

    slot.instance_variable_set(
      :@degraded_until,
      Process.clock_gettime(Process::CLOCK_MONOTONIC) + 60
    )

    slot.send_work(0, change_event, reply_port)
    index, response = receive_from_port(reply_port)

    assert_equal 0, index
    result = CDC::Parallel::ResultCollector.normalize(response)

    assert result.failure?
    assert_match "cooling down", result.error.message
  ensure
    reply_port&.close
    slot&.shutdown
    slot&.join
  end
  # rubocop:enable Metrics/MethodLength

  def test_worker_slot_clears_expired_degraded_state
    slot = worker_slot
    slot.instance_variable_set(
      :@degraded_until,
      Process.clock_gettime(Process::CLOCK_MONOTONIC) - 1
    )

    refute slot.degraded?
    assert_nil slot.instance_variable_get(:@degraded_until)
  ensure
    slot&.shutdown
    slot&.join
  end

  def test_worker_slot_shutdown_tolerates_missing_inbox
    slot = worker_slot
    original_inbox = slot.inbox
    slot.instance_variable_set(:@inbox, nil)

    assert_nil slot.shutdown
  ensure
    # The test intentionally removes the slot inbox to cover the nil-safe
    # shutdown branch. Keep a direct reference to the original inbox so the
    # worker can still be stopped; otherwise the supervisor waits forever on
    # a worker blocked in inbox.receive.
    original_inbox&.send(nil)
    slot&.join
  end

  def test_start_worker_boots_worker_and_reports_inbox
    boot_port = ::Ractor::Port.new
    worker = CDC::Parallel::ProcessorPool.send(
      :start_worker,
      ::Ractor.make_shareable(SafeProcessor.new),
      boot_port
    )
    inbox = receive_from_port(boot_port)

    assert_instance_of ::Ractor::Port, inbox
  ensure
    inbox&.send(nil)
    worker&.join
    boot_port&.close
  end

  # rubocop:disable Metrics/MethodLength
  # rubocop:disable Metrics/AbcSize
  def test_worker_slot_reports_failure_when_inbox_is_closed_before_send
    slot = worker_slot
    original_inbox = slot.inbox
    reply_port = ::Ractor::Port.new
    closed_inbox = Object.new
    closed_inbox.define_singleton_method(:send) do |_message|
      raise Ractor::ClosedError, "worker inbox closed"
    end

    slot.instance_variable_set(:@inbox, closed_inbox)
    slot.send_work(0, change_event, reply_port)
    index, response = receive_from_port(reply_port)
    result = CDC::Parallel::ResultCollector.normalize(response)

    assert_equal 0, index
    assert result.failure?
    assert_instance_of CDC::Parallel::ProcessorExecutionError, result.error
    assert_equal "Ractor::ClosedError", result.error.original_class
    assert_match(/worker inbox closed/, result.error.message)
    assert_empty slot.instance_variable_get(:@inflight)
  ensure
    reply_port&.close
    # Mark the slot as shutting down before stopping the original worker.
    # Otherwise the supervisor treats the stopped worker as an unexpected
    # death and boots a replacement, which makes join wait forever.
    slot&.shutdown
    original_inbox&.send(nil)
    slot&.join
  end
  # rubocop:enable Metrics/MethodLength
  # rubocop:enable Metrics/AbcSize

  def test_worker_slot_send_failure_tolerates_closed_reply_port
    slot = worker_slot

    assert_nil slot.send(:send_failure, closed_reply_port, 0, RuntimeError.new("closed"))
  ensure
    slot&.shutdown
    slot&.join
  end

  def test_worker_loop_exits_on_stop_message
    inbox = inbox_with(nil)

    assert_nil CDC::Parallel::ProcessorPool.send(:run_worker_loop, SafeProcessor.new, inbox)
  end

  def test_worker_loop_processes_work_message
    reply_port = recording_reply_port
    inbox = inbox_with([0, change_event, reply_port], nil)

    CDC::Parallel::ProcessorPool.send(:run_worker_loop, SafeProcessor.new, inbox)
    index, response = reply_port.messages.fetch(0)
    result = CDC::Parallel::ResultCollector.normalize(response)

    assert_equal 0, index
    assert result.success?
    assert_equal "users", result.event[:table]
  end

  def test_worker_loop_serializes_processor_failure
    reply_port = recording_reply_port
    inbox = inbox_with([0, change_event, reply_port], nil)

    CDC::Parallel::ProcessorPool.send(:run_worker_loop, FailingProcessor.new, inbox)
    _index, response = reply_port.messages.fetch(0)
    result = CDC::Parallel::ResultCollector.normalize(response)

    assert result.failure?
    assert_instance_of CDC::Parallel::ProcessorExecutionError, result.error
  end

  def test_worker_loop_tolerates_closed_reply_port
    reply_port = closed_reply_port
    inbox = inbox_with([0, change_event, reply_port], nil)

    assert_nil CDC::Parallel::ProcessorPool.send(:run_worker_loop, SafeProcessor.new, inbox)
  end

  private

  def receive_from_port(port, timeout: 1)
    Timeout.timeout(timeout) { port.receive }
  rescue Timeout::Error
    raise Minitest::Assertion, "reply was not received before timeout"
  end

  def worker_slot(processor: SafeProcessor.new, supervision: true, max_respawns: 3, respawn_window: 60,
                  respawn_cooldown: 0.01)
    CDC::Parallel::ProcessorPool::WorkerSlot.new(
      index: 0,
      processor: ::Ractor.make_shareable(processor),
      supervision:,
      max_respawns:,
      respawn_window:,
      respawn_cooldown:
    )
  end

  def event_for_table(table)
    change_event(table: table)
  end

  def eventually(timeout: 1)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

    until yield
      if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        raise Minitest::Assertion,
              "condition was not met before timeout"
      end

      sleep 0.01
    end
  end

  def inbox_with(*messages)
    messages = messages.dup

    Object.new.tap do |inbox|
      inbox.define_singleton_method(:receive) { messages.shift }
    end
  end

  def receive_reply_port_for(*messages)
    messages = messages.dup

    Object.new.tap do |port|
      port.define_singleton_method(:receive) { messages.shift }
    end
  end

  def recording_reply_port
    messages = []

    Object.new.tap do |port|
      port.define_singleton_method(:messages) { messages }
      port.define_singleton_method(:<<) { |message| messages << message }
    end
  end

  def closed_reply_port
    Object.new.tap do |port|
      port.define_singleton_method(:<<) do |_message|
        raise Ractor::ClosedError, "reply port closed"
      end
    end
  end
end
