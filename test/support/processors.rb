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
