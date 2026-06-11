# frozen_string_literal: true

module CDC
  module Parallel
    # Base error for all cdc-parallel-specific failures.
    #
    # Rescue this class when callers want to handle any failure raised directly
    # by the parallel runtime layer.
    #
    # @api public
    class Error < StandardError; end

    # Raised when a processor has not declared itself Ractor-safe.
    #
    # Processors must opt in with `ractor_safe!` before they can be used by
    # {ProcessorPool}, {TransactionPool}, or {Runtime}. This prevents accidental
    # movement of mutable or otherwise unsafe processor objects across Ractor
    # boundaries.
    #
    # @api public
    class UnsafeProcessorError < Error; end

    # Raised when work is submitted after a pool or runtime has been shut down.
    #
    # @api public
    class ShutdownError < Error; end

    # Raised when the runtime receives an unsupported work item shape.
    #
    # `cdc-parallel` accepts normalized `CDC::Core::ChangeEvent` and
    # `CDC::Core::TransactionEnvelope` objects. Source-specific payloads must be
    # normalized by a source adapter before they reach this runtime layer.
    #
    # @api public
    class UnsupportedWorkItemError < Error; end

    # Represents an exception raised inside a worker Ractor.
    #
    # Worker exceptions are serialized before they cross the Ractor boundary and
    # reconstructed as `ProcessorExecutionError` instances by
    # {ResultCollector.normalize}. The original exception class name, message,
    # and backtrace are exposed for diagnostics.
    #
    # @example Inspecting the original worker exception
    #   result = runtime.process(event)
    #   if result.failure?
    #     error = result.error
    #     error.original_class
    #     error.original_message
    #   end
    #
    # @attr_reader original_class [String] original exception class name.
    # @attr_reader original_message [String] original exception message.
    # @attr_reader original_backtrace [Array<String>] original exception backtrace.
    # @api public
    class ProcessorExecutionError < Error
      attr_reader :original_class, :original_message, :original_backtrace

      # Create a reconstructed worker exception.
      #
      # @param original_class [String]
      #   Class name of the exception raised inside the worker.
      # @param original_message [String]
      #   Message from the exception raised inside the worker.
      # @param original_backtrace [Array<String>]
      #   Backtrace captured inside the worker.
      # @return [void]
      def initialize(original_class:, original_message:, original_backtrace: [])
        @original_class = original_class
        @original_message = original_message
        @original_backtrace = original_backtrace.freeze

        super("#{original_class}: #{original_message}")
        set_backtrace(@original_backtrace) unless @original_backtrace.empty?
      end
    end

    # Raised when a pool does not receive worker results before the configured
    # timeout.
    #
    # Timeout failures are normally returned inside `CDC::Core::ProcessorResult`
    # failure objects rather than raised directly to the caller during result
    # collection.
    #
    # @api public
    class TimeoutError < Error; end
  end
end
