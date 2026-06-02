# frozen_string_literal: true

module CDC
  module Parallel
    # Executes one Ractor-safe processor in isolated Ractor workers.
    #
    # This v0.1 implementation intentionally uses one-shot worker Ractors for
    # deterministic synchronous semantics while preserving the public pool API.
    # The parallel-pool dependency is kept as the runtime foundation for later
    # async/throughput-focused versions.
    class ProcessorPool
      # @param processor [CDC::Core::Processor]
      # @param size [Integer]
      # @param timeout [Float, nil]
      # @return [void]
      def initialize(processor:, size: Etc.nprocessors, timeout: nil)
        validate_processor!(processor)

        @processor = ::Ractor.make_shareable(processor)
        @configuration = Configuration.new(size:, timeout:)
        @shutdown = false
      end

      # Process one ChangeEvent.
      #
      # @param event [CDC::Core::ChangeEvent]
      # @return [CDC::Core::ProcessorResult]
      def process(event)
        raise ShutdownError, "processor pool has been shut down" if @shutdown

        work = ::Ractor.make_shareable(event)
        worker = ::Ractor.new(@processor, work) do |processor, item|
          CDC::Parallel::ResultCollector.normalize(processor.process(item))
        rescue StandardError => e
          CDC::Parallel::ResultCollector.worker_failure(e)
        end

        ResultCollector.normalize(take(worker))
      end

      # Shut down the pool.
      #
      # @return [void]
      def shutdown
        @shutdown = true
      end

      private

      def validate_processor!(processor)
        return if processor.class.respond_to?(:ractor_safe?) && processor.class.ractor_safe?

        raise UnsafeProcessorError, "#{processor.class} must declare ractor_safe!"
      end

      def take(worker)
        if worker.respond_to?(:value)
          worker.value
        else
          worker.take
        end
      end
    end
  end
end
