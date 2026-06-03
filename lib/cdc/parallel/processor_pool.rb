# frozen_string_literal: true

module CDC
  module Parallel
    # Executes one Ractor-safe processor in pre-warmed persistent Ractor workers.
    #
    # Workers are created during initialization and reused for every dispatch.
    # This pays Ractor startup cost once, keeps workers alive after processor
    # failures, and provides both synchronous single-item processing and batched
    # dispatch for throughput-oriented benchmarks and runtimes.
    class ProcessorPool # rubocop:disable Metrics/ClassLength
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

        signal_workers
        wait_for_workers
      end

      private

      def signal_workers
        @workers.each do |worker|
          worker.send(nil)
        rescue Ractor::ClosedError
          # Already stopped.
        end
      end

      def wait_for_workers
        if @configuration.timeout
          wait_for_workers_with_timeout
        else
          @workers.each(&:join)
        end
      end

      def wait_for_workers_with_timeout
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @configuration.timeout

        @workers.each do |worker|
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          break unless remaining.positive?

          ::Timeout.timeout(remaining, TimeoutError) { worker.join }
        rescue TimeoutError
          break
        end
      end

      def validate_processor!(processor)
        return if processor.class.respond_to?(:ractor_safe?) &&
                  processor.class.ractor_safe?

        raise UnsafeProcessorError,
              "#{processor.class} must declare ractor_safe!"
      end

      def build_worker(processor) # rubocop:disable Metrics/MethodLength
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

            begin
              reply_port << [index, response]
            rescue Ractor::ClosedError
              # The caller may have timed out and closed the reply port.
            end
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
        return results.freeze if count.zero?

        if @configuration.timeout
          collect_results_with_timeout(reply_port, results)
        else
          collect_results_without_timeout(reply_port, results)
        end
      end

      def collect_results_without_timeout(reply_port, results)
        results.length.times do
          index, response = reply_port.receive
          results[index] = ResultCollector.normalize(response)
        end

        results.freeze
      end

      def collect_results_with_timeout(reply_port, results)
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @configuration.timeout

        results.length.times do
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          return timeout_results(results) unless remaining.positive?

          index, response = ::Timeout.timeout(remaining, TimeoutError) { reply_port.receive }
          results[index] = ResultCollector.normalize(response)
        rescue TimeoutError
          return timeout_results(results)
        end

        results.freeze
      end

      def timeout_results(results)
        missing = results.count(&:nil?)
        timeout_error = TimeoutError.new(
          "processor pool timed out after #{@configuration.timeout} seconds waiting for #{missing} result(s)"
        )

        results.map do |result|
          result || CDC::Core::ProcessorResult.failure(timeout_error)
        end.freeze
      end
    end
  end
end
