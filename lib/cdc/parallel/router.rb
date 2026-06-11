# frozen_string_literal: true

module CDC
  module Parallel
    # Routes normalized `cdc-core` work items to the matching parallel runtime
    # primitive.
    #
    # `Router` is deliberately small. It does not inspect source-specific
    # payloads, apply filters, decode database values, or decide scheduling
    # policy. Its responsibility is only to look at the already-normalized
    # `cdc-core` object shape and forward it to the pool that knows how to
    # process that shape.
    #
    # @example Routing a single event
    #   router.process(change_event)
    #
    # @example Routing a transaction envelope
    #   router.process(transaction_envelope)
    #
    # @see CDC::Parallel::ProcessorPool
    # @see CDC::Parallel::TransactionPool
    # @api public
    class Router
      # Create a router for event and transaction work items.
      #
      # @param processor_pool [ProcessorPool]
      #   Pool used for individual `CDC::Core::ChangeEvent` objects.
      # @param transaction_pool [TransactionPool]
      #   Pool used for `CDC::Core::TransactionEnvelope` objects.
      # @return [void]
      def initialize(processor_pool:, transaction_pool:)
        @processor_pool = processor_pool
        @transaction_pool = transaction_pool
      end

      # Process a supported CDC work item.
      #
      # @param item [CDC::Core::ChangeEvent, CDC::Core::TransactionEnvelope]
      #   Normalized CDC work item.
      # @raise [UnsupportedWorkItemError]
      #   Raised when the item is not a supported `cdc-core` work item shape.
      # @return [CDC::Core::ProcessorResult]
      def process(item)
        case item
        when CDC::Core::ChangeEvent
          @processor_pool.process(item)
        when CDC::Core::TransactionEnvelope
          @transaction_pool.process(item)
        else
          raise UnsupportedWorkItemError, "unsupported CDC work item: #{item.class}"
        end
      end
    end
  end
end
