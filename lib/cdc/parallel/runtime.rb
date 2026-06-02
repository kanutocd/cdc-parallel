# frozen_string_literal: true

module CDC
  module Parallel
    # High-level Ractor runtime facade for cdc-core processors.
    class Runtime
      # @param processor [CDC::Core::Processor]
      # @param size [Integer]
      # @param timeout [Float, nil]
      # @return [void]
      def initialize(processor:, size: Etc.nprocessors, timeout: nil)
        @processor_pool = ProcessorPool.new(processor:, size:, timeout:)
        @transaction_pool = TransactionPool.new(processor:, size:, timeout:)
        @router = Router.new(processor_pool: @processor_pool, transaction_pool: @transaction_pool)
        @shutdown = false
      end

      # Process a ChangeEvent or TransactionEnvelope.
      #
      # @param item [CDC::Core::ChangeEvent, CDC::Core::TransactionEnvelope]
      # @return [CDC::Core::ProcessorResult]
      def process(item)
        raise ShutdownError, "runtime has been shut down" if @shutdown

        @router.process(item)
      end

      # Alias for transaction-oriented processing.
      #
      # @param transaction [CDC::Core::TransactionEnvelope]
      # @return [CDC::Core::ProcessorResult]
      def process_transaction(transaction)
        process(transaction)
      end

      # Shut down all runtime resources.
      #
      # @return [void]
      def shutdown
        return if @shutdown

        @shutdown = true
        @processor_pool.shutdown
        @transaction_pool.shutdown
      end
    end
  end
end
