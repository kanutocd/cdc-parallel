# frozen_string_literal: true

module CDC
  module Parallel
    # Routes supported CDC objects to the correct runtime pool.
    class Router
      # @param processor_pool [ProcessorPool]
      # @param transaction_pool [TransactionPool]
      def initialize(processor_pool:, transaction_pool:)
        @processor_pool = processor_pool
        @transaction_pool = transaction_pool
      end

      # Process a supported CDC work item.
      #
      # @param item [CDC::Core::ChangeEvent, CDC::Core::TransactionEnvelope]
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
