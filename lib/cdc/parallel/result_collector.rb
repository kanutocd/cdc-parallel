# frozen_string_literal: true

module CDC
  module Parallel
    # Converts raw worker responses into `CDC::Core::ProcessorResult` objects.
    #
    # Ractors cannot freely share arbitrary mutable Ruby objects. Worker
    # responses must therefore be normalized into shareable payloads before they
    # cross back to the caller. `ResultCollector` owns that small translation
    # boundary.
    #
    # The worker side uses {worker_success} and {worker_failure}. The caller side
    # uses {normalize} to convert those payloads into the public result contract.
    #
    # @example Normalizing a processor return value
    #   response = CDC::Parallel::ResultCollector.worker_success(value)
    #   result = CDC::Parallel::ResultCollector.normalize(response)
    #
    # @example Normalizing a worker exception
    #   response = CDC::Parallel::ResultCollector.worker_failure(error)
    #   result = CDC::Parallel::ResultCollector.normalize(response)
    #   result.failure? #=> true
    #
    # @api public
    class ResultCollector
      # Internal marker used to identify serialized worker failures.
      #
      # @return [Symbol]
      FAILURE_MARKER = :__cdc_parallel_failure__

      # Build a shareable success payload that can safely cross a Ractor boundary.
      #
      # If the processor already returned a `CDC::Core::ProcessorResult`, that
      # result is preserved. Any other shareable value will later be wrapped in a
      # success result by {normalize}.
      #
      # @param value [Object]
      #   Processor return value.
      # @raise [Ractor::Error]
      #   Raised by Ruby when the value cannot be made shareable.
      # @return [Object]
      #   Shareable success payload.
      def self.worker_success(value)
        ::Ractor.make_shareable(value)
      end

      # Build a shareable failure payload that can safely cross a Ractor boundary.
      #
      # Exceptions themselves are not used as the cross-Ractor payload. Instead,
      # the class name, message, and backtrace are serialized into a simple hash
      # that can be reconstructed as a {ProcessorExecutionError} by {normalize}.
      #
      # @param error [Exception]
      #   Exception raised inside a worker Ractor.
      # @return [Hash]
      #   Shareable serialized failure payload.
      def self.worker_failure(error)
        ::Ractor.make_shareable(
          {
            type: FAILURE_MARKER,
            class: error.class.name,
            message: error.message,
            backtrace: (error.backtrace || []).map { |line| String(line) }.freeze
          }
        )
      end

      # Normalize a worker return value into a `CDC::Core::ProcessorResult`.
      #
      # Failure payloads become failed processor results containing a
      # {ProcessorExecutionError}. Existing processor results are returned
      # unchanged. Other values are wrapped in a successful processor result.
      #
      # @param value [Object]
      #   Raw worker response.
      # @return [CDC::Core::ProcessorResult]
      def self.normalize(value)
        if worker_failure?(value)
          CDC::Core::ProcessorResult.failure(
            ProcessorExecutionError.new(
              original_class: value[:class],
              original_message: value[:message],
              original_backtrace: value[:backtrace]
            )
          )
        elsif value.is_a?(CDC::Core::ProcessorResult)
          value
        else
          CDC::Core::ProcessorResult.success(value)
        end
      end

      # Return whether a value is a serialized worker failure payload.
      #
      # @param value [Object]
      #   Raw worker response.
      # @return [Boolean]
      # @api private
      def self.worker_failure?(value)
        value.is_a?(Hash) && value[:type] == FAILURE_MARKER
      end
      private_class_method :worker_failure?
    end
  end
end
