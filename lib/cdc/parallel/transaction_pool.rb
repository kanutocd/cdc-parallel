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
        results = transaction.events.map { |event| @processor_pool.process(event) }.freeze
        ResultCollector.normalize(results)
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
