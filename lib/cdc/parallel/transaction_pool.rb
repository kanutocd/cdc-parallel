# frozen_string_literal: true

module CDC
  module Parallel
    # Processes a TransactionEnvelope as a single ordering-preserving unit.
    class TransactionPool
      # @param processor [CDC::Core::Processor]
      # @param size [Integer]
      # @param timeout [Float, nil]
      def initialize(processor:, size: Etc.nprocessors, timeout: nil)
        @processor_pool = ProcessorPool.new(processor:, size:, timeout:)
      end

      # Process all events inside a transaction envelope.
      #
      # @param transaction [CDC::Core::TransactionEnvelope]
      # @return [CDC::Core::ProcessorResult]
      def process(transaction)
        results = @processor_pool.process_many(transaction.events).freeze
        failure = results.find(&:failure?)

        return CDC::Core::ProcessorResult.failure(failure.error, event: results) if failure

        CDC::Core::ProcessorResult.success(results)
      end

      # Shut down worker resources.
      #
      # @return [void]
      def shutdown
        @processor_pool.shutdown
      end
    end
  end
end
