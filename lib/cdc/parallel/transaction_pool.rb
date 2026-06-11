# frozen_string_literal: true

module CDC
  module Parallel
    # Processes a `CDC::Core::TransactionEnvelope` as one transaction-oriented
    # work unit.
    #
    # `TransactionPool` uses {ProcessorPool} to process the events inside an
    # envelope and then collapses the event-level results into one
    # `CDC::Core::ProcessorResult` for the whole transaction.
    #
    # This class preserves the transaction boundary at the API level: callers
    # submit a transaction envelope and receive a single success or failure
    # result. Event results inside the transaction are still produced by the
    # configured processor and are returned as the success value when every event
    # succeeds.
    #
    # @example Processing a transaction envelope
    #   pool = CDC::Parallel::TransactionPool.new(
    #     processor: AuditProcessor.new,
    #     size: 4
    #   )
    #
    #   result = pool.process(transaction)
    #   result.success? #=> true
    #
    # @note This class preserves the transaction as a result boundary. More
    #   advanced ordering, checkpointing, retry, and atomic sink semantics belong
    #   to higher-level runtime/sink contracts.
    # @see CDC::Parallel::ProcessorPool
    # @api public
    class TransactionPool
      # Create a transaction pool.
      #
      # @param processor [CDC::Core::Processor]
      #   Ractor-safe processor used for each event inside the transaction.
      # @param size [Integer]
      #   Number of worker Ractors in the underlying processor pool.
      # @param timeout [Numeric, nil]
      #   Optional timeout in seconds for result collection and shutdown waits.
      # @raise [UnsafeProcessorError]
      #   Raised when the processor class has not declared `ractor_safe!`.
      # @return [void]
      def initialize(processor:, size: Etc.nprocessors, timeout: nil)
        @processor_pool = ProcessorPool.new(processor:, size:, timeout:)
      end

      # Process all events inside a transaction envelope.
      #
      # The returned result is successful only when every event result succeeds.
      # If any event fails, the transaction result is a failure using the first
      # failure error and the complete event-result list as context.
      #
      # @param transaction [CDC::Core::TransactionEnvelope]
      #   Transaction envelope whose `events` will be processed.
      # @return [CDC::Core::ProcessorResult]
      #   Success containing the ordered event results, or failure containing the
      #   first event error.
      def process(transaction)
        results = @processor_pool.process_many(transaction.events).freeze
        failure = results.find(&:failure?)

        return CDC::Core::ProcessorResult.failure(failure.error, event: results) if failure

        CDC::Core::ProcessorResult.success(results)
      end

      # Shut down worker resources.
      #
      # Delegates to the underlying {ProcessorPool}. Shutdown is idempotent
      # because the underlying pool is idempotent.
      #
      # @return [void]
      def shutdown
        @processor_pool.shutdown
      end
    end
  end
end
