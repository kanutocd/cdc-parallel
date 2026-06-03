# frozen_string_literal: true

module CDC
  module Parallel
    # Executes one Ractor-safe processor in pre-warmed persistent Ractor workers.
    #
    # Workers are created during initialization and reused for every dispatch.
    # This pays Ractor startup cost once, keeps workers alive after processor
    # failures, and provides both synchronous single-item processing and batched
    # dispatch for throughput-oriented benchmarks and runtimes.
    class ProcessorPool
      # @param processor [CDC::Core::Processor]
      # @param size [Integer]
      # @param timeout [Float, nil]
      # @return [void]
      def initialize(processor:, size: Etc.nprocessors, timeout: nil)
        validate_processor!(processor)

        @processor = ::Ractor.make_shareable(processor)
        @configuration = Configuration.new(size:, timeout:)
        @workers = Array.new(@configuration.size) do
          build_worker(@processor)
        end.freeze

        @next_worker = 0
        @shutdown = false
      end

      # Process one work item synchronously.
      #
      # @param item [Object]
      # @return [CDC::Core::ProcessorResult]
      def process(item)
        process_many([item]).fetch(0)
      end

      # Process many work items using the pre-warmed worker pool.
      #
      # Results are returned in the same order as the supplied work items.
      #
      # @param items [Array<Object>]
      # @return [Array<CDC::Core::ProcessorResult>]
      def process_many(items)
        raise ShutdownError, "processor pool has been shut down" if @shutdown

        work_items = items.map { |item| ::Ractor.make_shareable(item) }
        reply_port = ::Ractor::Port.new

        work_items.each_with_index do |item, index|
          next_worker.send([index, item, reply_port])
        end

        collect_results(reply_port, work_items.length)
      ensure
        reply_port&.close
      end

      # Shut down the pool.
      #
      # @return [void]
      def shutdown
        return if @shutdown

        @shutdown = true

        @workers.each do |worker|
          worker.send(nil)
        rescue Ractor::ClosedError
          # Already stopped.
        end
      end

      private

      def validate_processor!(processor)
        return if processor.class.respond_to?(:ractor_safe?) &&
                  processor.class.ractor_safe?

        raise UnsafeProcessorError,
              "#{processor.class} must declare ractor_safe!"
      end

      def build_worker(processor)
        ::Ractor.new(processor) do |safe_processor|
          loop do
            message = ::Ractor.receive
            break if message.nil?

            index, item, reply_port = message

            response = begin
              CDC::Parallel::ResultCollector.worker_success(
                safe_processor.process(item)
              )
            rescue StandardError => e
              CDC::Parallel::ResultCollector.worker_failure(e)
            end

            reply_port << [index, response]
          end
        end
      end

      def next_worker
        worker = @workers[@next_worker]

        @next_worker += 1
        @next_worker = 0 if @next_worker >= @workers.length

        worker
      end

      def collect_results(reply_port, count)
        results = Array.new(count)

        count.times do
          index, response = reply_port.receive
          results[index] = ResultCollector.normalize(response)
        end

        results.freeze
      end
    end
  end
end
