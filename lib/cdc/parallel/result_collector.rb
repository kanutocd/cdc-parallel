# frozen_string_literal: true

module CDC
  module Parallel
    # Normalizes values returned by workers.
    class ResultCollector
      FAILURE_MARKER = :__cdc_parallel_failure__

      # Build a shareable success payload that can safely cross a Ractor boundary.
      #
      # @param value [Object]
      # @return [Object]
      def self.worker_success(value)
        ::Ractor.make_shareable(value)
      end

      # Build a shareable failure payload that can safely cross a Ractor boundary.
      #
      # @param error [Exception]
      # @return [Hash]
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

      # Normalize a worker return value into a ProcessorResult.
      #
      # @param value [Object]
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

      def self.worker_failure?(value)
        value.is_a?(Hash) && value[:type] == FAILURE_MARKER
      end
      private_class_method :worker_failure?
    end
  end
end
