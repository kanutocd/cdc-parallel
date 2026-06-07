# frozen_string_literal: true

module CDC
  module Parallel
    # Executes one Ractor-safe processor using a pool of pre-warmed,
    # persistent Ractor workers.
    #
    # Workers are created during initialization and reused for every dispatch.
    # This pays Ractor startup cost once, keeps workers alive after processor
    # failures, and provides both synchronous single-item processing and batched
    # dispatch for throughput-oriented workloads.
    #
    # Architecture
    #
    # The pool implements a classic fan-out / fan-in pattern:
    #
    #   work items
    #        |
    #        v
    #   ProcessorPool
    #        |
    #        +----> Worker Ractor 1
    #        |
    #        +----> Worker Ractor 2
    #        |
    #        +----> Worker Ractor N
    #                    |
    #                    v
    #              ProcessorResult
    #                    |
    #                    v
    #              reply_port
    #                    |
    #                    v
    #             ordered results
    #
    # Fan-out:
    #
    # * Work items are distributed across worker-owned inbox ports.
    # * Dispatch uses round-robin worker selection.
    # * Multiple items may execute concurrently in different Ractors.
    #
    # Fan-in:
    #
    # * Workers publish results to a shared reply port.
    # * Results may arrive out of execution order.
    # * ProcessorPool reorders responses using submission indexes so the
    #   returned array always matches the input order.
    #
    # Example:
    #
    #   Input:
    #     [A, B, C, D]
    #
    #   Completion:
    #     C, A, D, B
    #
    #   Returned:
    #     [result_A, result_B, result_C, result_D]
    #
    # Submission is safe from multiple Ruby threads. Work is delivered to
    # worker-owned Ractor::Port inboxes and executed inside isolated Ractors.
    # Advanced execution strategies
    #
    # ProcessorPool guarantees fan-out across pre-warmed Ractor workers.
    # It does not guarantee, manage, or validate any additional concurrency
    # created inside a worker Ractor.
    #
    # A processor may choose to create threads, fibers, Async tasks, or other
    # local concurrency inside its own Ractor execution context, but that is
    # the processor implementor's responsibility.
    #
    # Declaring ractor_safe! only means the processor can cross the Ractor
    # boundary and execute inside a worker Ractor. It does not automatically
    # make any internal threads, fibers, clients, connection pools, caches,
    # or mutable state safe.
    #
    # In short: cdc-parallel provides the outer parallel execution boundary.
    # Inner concurrency is possible, but "thread" lightly.
    #
    # @see CDC::Parallel::ResultCollector
    class ProcessorPool # rubocop:disable Metrics/ClassLength
      # @param processor [CDC::Core::Processor]
      # @param size [Integer]
      # @param timeout [Float, nil]
      # @return [void]
      def initialize(processor:, size: Etc.nprocessors, timeout: nil)
        validate_processor!(processor)

        @processor = ::Ractor.make_shareable(processor)
        @configuration = Configuration.new(size:, timeout:)
        booted_workers = Array.new(@configuration.size) do
          build_worker(@processor)
        end

        @workers = booted_workers.map(&:first).freeze
        @worker_inboxes = booted_workers.map(&:last).freeze

        @next_worker = 0
        @dispatch_mutex = Mutex.new
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
        work_items = items.map { |item| ::Ractor.make_shareable(item) }
        reply_port = ::Ractor::Port.new

        dispatch(work_items, reply_port)

        collect_results(reply_port, work_items.length)
      ensure
        reply_port&.close
      end

      # Shut down the pool.
      #
      # @return [void]
      def shutdown
        should_wait = @dispatch_mutex.synchronize do
          return if @shutdown

          @shutdown = true
          signal_workers
          true
        end

        wait_for_workers if should_wait
      end

      private

      def dispatch(work_items, reply_port)
        @dispatch_mutex.synchronize do
          raise ShutdownError, "processor pool has been shut down" if @shutdown

          work_items.each_with_index do |item, index|
            next_worker_inbox.send([index, item, reply_port])
          end
        end
      end

      def signal_workers
        @worker_inboxes.each do |inbox|
          inbox.send(nil)
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

      def build_worker(processor)
        boot_port = ::Ractor::Port.new
        worker = start_worker(processor, boot_port)

        [worker, boot_port.receive]
      ensure
        boot_port&.close
      end

      def start_worker(processor, boot_port) # rubocop:disable Metrics/MethodLength
        ::Ractor.new(processor, boot_port) do |safe_processor, ready_port|
          inbox = ::Ractor::Port.new
          ready_port << inbox

          loop do
            message = inbox.receive
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

      def next_worker_inbox
        inbox = @worker_inboxes[@next_worker]

        @next_worker += 1
        @next_worker = 0 if @next_worker >= @worker_inboxes.length

        inbox
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
