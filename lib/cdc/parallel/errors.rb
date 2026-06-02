# frozen_string_literal: true

module CDC
  module Parallel
    # Base cdc-parallel error.
    class Error < StandardError; end

    # Raised when a processor has not declared itself Ractor-safe.
    class UnsafeProcessorError < Error; end

    # Raised when work is submitted after shutdown.
    class ShutdownError < Error; end

    # Raised when the runtime receives an unsupported work item.
    class UnsupportedWorkItemError < Error; end

    # Raised when processor execution fails inside a worker Ractor.
    class ProcessorExecutionError < Error
      attr_reader :original_class, :original_message, :original_backtrace

      def initialize(original_class:, original_message:, original_backtrace: [])
        @original_class = original_class
        @original_message = original_message
        @original_backtrace = original_backtrace.freeze

        super("#{original_class}: #{original_message}")
        set_backtrace(@original_backtrace) unless @original_backtrace.empty?
      end
    end

    # Raised when a worker does not return a result before timeout.
    class TimeoutError < Error; end
  end
end
