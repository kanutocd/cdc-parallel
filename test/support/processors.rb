# frozen_string_literal: true

require "cdc_core"

class SafeProcessor < CDC::Core::Processor
  ractor_safe!

  def process(event)
    payload = ::Ractor.make_shareable({
                                        operation: event.operation,
                                        table: event.table
                                      })

    CDC::Core::ProcessorResult.success(payload)
  end
end

class UnsafeProcessor < CDC::Core::Processor
  def process(event)
    CDC::Core::ProcessorResult.success(event)
  end
end

class FailingProcessor < CDC::Core::Processor
  ractor_safe!

  def process(_event)
    raise "boom"
  end
end

class ConditionalFailingProcessor < CDC::Core::Processor
  ractor_safe!

  def process(event)
    raise "boom" if event.table == "boom"

    payload = ::Ractor.make_shareable({ operation: event.operation, table: event.table })

    CDC::Core::ProcessorResult.success(payload)
  end
end

class FlakyProcessor < CDC::Core::Processor
  ractor_safe!

  def process(event)
    raise "boom" if event.table == "failures"

    CDC::Core::ProcessorResult.success(
      ::Ractor.make_shareable(
        {
          operation: event.operation,
          table: event.table
        }
      )
    )
  end
end

class SlowProcessor < CDC::Core::Processor
  ractor_safe!

  def process(event)
    sleep 0.05

    CDC::Core::ProcessorResult.success(
      ::Ractor.make_shareable(
        {
          operation: event.operation,
          table: event.table
        }
      )
    )
  end
end

class ConditionalSlowProcessor < CDC::Core::Processor
  ractor_safe!

  def process(event)
    sleep 0.05 if event.table == "slow"

    CDC::Core::ProcessorResult.success(
      ::Ractor.make_shareable(
        {
          operation: event.operation,
          table: event.table
        }
      )
    )
  end
end

# Records calls to lifecycle hooks so tests can assert on them.
#
# Ractor.make_shareable deep-freezes the processor instance and all objects
# reachable from it, which means instance variables cannot be mutated after the
# processor is passed to a pool. To work around this, call records are kept in a
# class-level hash keyed by object_id. The hash is never passed across a Ractor
# boundary and is only accessed from the test thread.
# rubocop:disable Lint/HashCompareByIdentity
class LifecycleTrackingProcessor < CDC::Core::Processor
  ractor_safe!

  CALL_LOG = Hash.new { |h, k| h[k] = [] }

  def self.reset_logs
    CALL_LOG.clear
  end

  def start
    CALL_LOG[object_id] << :start
    self
  end

  def stop
    CALL_LOG[object_id] << :stop
    self
  end

  def flush
    CALL_LOG[object_id] << :flush
    self
  end

  def calls        = CALL_LOG[object_id]
  def start_count  = calls.count(:start)
  def stop_count   = calls.count(:stop)
  def flush_count  = calls.count(:flush)

  def process(event)
    CDC::Core::ProcessorResult.success(
      ::Ractor.make_shareable({ table: event.table })
    )
  end
end
# rubocop:enable Lint/HashCompareByIdentity

# Always reports healthy? => false.
class UnhealthyProcessor < CDC::Core::Processor
  ractor_safe!

  def healthy?
    false
  end

  def process(event)
    CDC::Core::ProcessorResult.success(
      ::Ractor.make_shareable({ table: event.table })
    )
  end
end

# Crashes the worker Ractor by raising outside the StandardError hierarchy.
class FatalProcessor < CDC::Core::Processor
  ractor_safe!

  def process(_event)
    raise Exception, "fatal worker death" # rubocop:disable Lint/RaiseException
  end
end

# Starts by mutating internal state so runtime lifecycle ordering is verified
# before the processor is made shareable by the underlying pools.
class MutableStartProcessor < CDC::Core::Processor
  ractor_safe!

  attr_reader :started_at

  def start
    @started_at = :started
    self
  end

  def process(event)
    CDC::Core::ProcessorResult.success(
      ::Ractor.make_shareable({ table: event.table, started_at: @started_at })
    )
  end
end

# Crashes only for events whose table is "boom".
class ConditionalFatalProcessor < CDC::Core::Processor
  ractor_safe!

  def process(event)
    raise Exception, "fatal worker death" if event.table == "boom" # rubocop:disable Lint/RaiseException

    CDC::Core::ProcessorResult.success(
      ::Ractor.make_shareable({ table: event.table })
    )
  end
end
