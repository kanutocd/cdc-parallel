# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/processors"
require_relative "../support/events"

class ProcessorPoolPerformanceTest < Minitest::Test
  include EventFixtures

  def test_processes_many_events_within_smoke_threshold
    skip "set PERFORMANCE=true to run performance smoke tests" unless ENV["PERFORMANCE"] == "true"

    pool = CDC::Parallel::ProcessorPool.new(processor: SafeProcessor.new, size: 2)
    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    100.times do
      assert pool.process(change_event).success?
    end

    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
    assert_operator elapsed, :<, 10.0
  ensure
    pool&.shutdown
  end
end
