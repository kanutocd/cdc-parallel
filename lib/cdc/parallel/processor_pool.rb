# frozen_string_literal: true

module CDC
  module Parallel
    # Executes one Ractor-safe `cdc-core` processor across a fixed set of
    # pre-warmed Ractor workers.
    #
    # `ProcessorPool` is the low-level execution primitive used by
    # {CDC::Parallel::Runtime}. It accepts normalized `cdc-core` work items,
    # sends them across Ractor boundaries, invokes the configured processor, and
    # returns `CDC::Core::ProcessorResult` objects in input order.
    #
    # This class is intentionally focused on **CPU-bound parallel execution**.
    # Use it when the processor spends most of its time doing Ruby work such as
    # transformation, enrichment, serialization, compression, scoring, or other
    # in-memory computation. For I/O-heavy work, the CDC Ecosystem boundary is a
    # future fiber-friendly runtime such as `cdc-concurrent`.
    #
    # ## Processor safety contract
    #
    # The supplied processor must declare `ractor_safe!` on its class. That
    # declaration is treated as the processor author's explicit promise that the
    # processor object and its dependencies can safely cross a Ractor boundary.
    #
    # `ProcessorPool` validates this declaration before booting workers:
    #
    # @example Declaring a processor as Ractor-safe
    #   class AnalyticsProcessor < CDC::Core::Processor
    #     ractor_safe!
    #
    #     def process(event)
    #       CDC::Core::ProcessorResult.success(event)
    #     end
    #   end
    #
    #   pool = CDC::Parallel::ProcessorPool.new(
    #     processor: AnalyticsProcessor.new,
    #     size: 4
    #   )
    #
    # Declaring `ractor_safe!` does not make unsafe code safe. It only allows the
    # processor to be passed into worker Ractors. Mutable global state, database
    # connections, sockets, caches, file handles, and non-shareable objects still
    # need to be designed carefully by the processor implementor.
    #
    # ## Execution model
    #
    # Workers are created during initialization and reused for all dispatches.
    # This pays Ractor startup cost once and keeps the pool stable even when
    # individual processor calls fail.
    #
    # The pool uses a fan-out / fan-in pattern:
    #
    # ```text
    # work items
    #      |
    #      v
    # ProcessorPool
    #      |
    #      +----> Worker Ractor 1
    #      +----> Worker Ractor 2
    #      +----> Worker Ractor N
    #                    |
    #                    v
    #             ProcessorResult
    #                    |
    #                    v
    #              ordered results
    # ```
    #
    # Fan-out uses round-robin worker selection. Fan-in collects responses from a
    # reply port and reorders them by submission index, so `process_many` always
    # returns results in the same order as the input array even when work
    # completes out of order.
    #
    # @example Processing one item
    #   result = pool.process(event)
    #   result.success? #=> true
    #
    # @example Processing a batch while preserving result order
    #   results = pool.process_many([event_a, event_b, event_c])
    #   results.map(&:success?)
    #
    # @example Shutting down explicitly
    #   pool.shutdown
    #
    # @note `ProcessorPool` preserves the order of returned results, not the
    #   order in which independent items execute. If a sink needs strict ordering
    #   by transaction, relation, or primary key, use the ecosystem ordering
    #   contract and an ordered dispatcher/runtime above this primitive.
    #
    # @see CDC::Parallel::Runtime High-level facade for processing supported CDC work items
    # @see CDC::Parallel::TransactionPool Transaction-envelope processing wrapper
    # @see CDC::Parallel::ResultCollector Worker response normalization
    # @api public
    class ProcessorPool # rubocop:disable Metrics/ClassLength
      # Create a new pool and boot its worker Ractors.
      #
      # @param processor [CDC::Core::Processor]
      #   Processor instance used by every worker. Its class must respond to
      #   `ractor_safe?` and return `true`.
      # @param size [Integer]
      #   Number of worker Ractors to boot. Defaults to `Etc.nprocessors`.
      # @param timeout [Numeric, nil]
      #   Optional timeout, in seconds, used when waiting for worker results and
      #   during shutdown. `nil` means wait indefinitely.
      # @raise [UnsafeProcessorError]
      #   Raised when the processor class has not declared `ractor_safe!`.
      # @raise [ArgumentError]
      #   Raised by {Configuration} when `size` or `timeout` is invalid.
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
      # This is a convenience wrapper around {#process_many}. The work still
      # executes inside a worker Ractor; the call blocks until the corresponding
      # `CDC::Core::ProcessorResult` is available or until the optional timeout
      # is reached.
      #
      # @param item [Object]
      #   Shareable work item, usually a `CDC::Core::ChangeEvent`.
      # @raise [ShutdownError]
      #   Raised when work is submitted after {#shutdown} has started.
      # @return [CDC::Core::ProcessorResult]
      #   Normalized processor result. Processor exceptions are captured as
      #   failure results rather than escaping directly from the worker Ractor.
      def process(item)
        process_many([item]).fetch(0)
      end

      # Process many work items using the pre-warmed worker pool.
      #
      # Each item is made shareable before dispatch. Items are assigned to worker
      # inboxes using round-robin selection. Responses are collected through a
      # per-call reply port and returned in the same order as the input array.
      #
      # @param items [Array<Object>]
      #   Work items to process. Empty arrays are valid and return an empty
      #   frozen array.
      # @raise [ShutdownError]
      #   Raised when work is submitted after {#shutdown} has started.
      # @return [Array<CDC::Core::ProcessorResult>]
      #   Frozen array of normalized results, ordered to match `items`.
      def process_many(items)
        work_items = items.map { |item| ::Ractor.make_shareable(item) }
        reply_port = ::Ractor::Port.new

        dispatch(work_items, reply_port)

        collect_results(reply_port, work_items.length)
      ensure
        reply_port&.close
      end

      # Shut down the pool and wait for worker Ractors to exit.
      #
      # Shutdown is idempotent. The first caller signals all worker inboxes with
      # a stop message and waits for workers to join. Later calls return without
      # doing anything.
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
        timeout = @configuration.timeout
        return unless timeout

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

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
        timeout = @configuration.timeout
        return results.freeze unless timeout

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

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
