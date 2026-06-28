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
      # @param supervision [Boolean]
      #   Whether worker Ractors should be respawned after unexpected death.
      # @param max_respawns [Integer]
      #   Maximum crash count inside `respawn_window` before a worker slot enters
      #   cooldown.
      # @param respawn_window [Numeric]
      #   Time window, in seconds, used by the crash-loop circuit breaker.
      # @param respawn_cooldown [Numeric]
      #   Cooldown, in seconds, before a crash-looping slot is revived again.
      # @param manage_lifecycle [Boolean]
      #   When `true` (default), the pool calls `processor.start` during
      #   initialization and `processor.flush` + `processor.stop` during shutdown.
      #   Set to `false` when a higher-level runtime (e.g. {Runtime}) owns the
      #   processor lifecycle so that `start`/`stop`/`flush` are not called
      #   multiple times when the same processor is shared across pools.
      # rubocop:disable Metrics/MethodLength
      def initialize(
        processor:,
        size: Etc.nprocessors,
        timeout: nil,
        supervision: true,
        max_respawns: 3,
        respawn_window: 60,
        respawn_cooldown: 5,
        manage_lifecycle: true
      )
        validate_processor!(processor)

        processor.start if manage_lifecycle
        @processor = ::Ractor.make_shareable(processor)
        @manage_lifecycle = manage_lifecycle
        @configuration = Configuration.new(size:, timeout:)
        @slots = Array.new(@configuration.size) do |index|
          WorkerSlot.new(
            index:,
            processor: @processor,
            supervision:,
            max_respawns:,
            respawn_window:,
            respawn_cooldown:
          )
        end.freeze
        @workers = @slots.map(&:worker).freeze
        @worker_inboxes = @slots.map(&:inbox).freeze

        @next_worker = 0
        @dispatch_mutex = Mutex.new
        @shutdown = false
      end
      # rubocop:enable Metrics/MethodLength

      # Return total worker-slot respawns since this pool booted.
      #
      # @return [Integer]
      def respawns
        @slots.sum(&:respawns)
      end

      # Return whether any worker slot is currently in crash-loop cooldown.
      #
      # @return [Boolean]
      def degraded?
        @slots.any?(&:degraded?)
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

        assignments = dispatch(work_items, reply_port)

        collect_results(reply_port, work_items.length, assignments)
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
        @dispatch_mutex.synchronize do
          return if @shutdown

          @shutdown = true
          signal_workers
        end

        wait_for_workers
        return unless @manage_lifecycle

        @processor.flush
        @processor.stop
      end

      private

      def dispatch(work_items, reply_port)
        @dispatch_mutex.synchronize do
          raise ShutdownError, "processor pool has been shut down" if @shutdown

          assignments = Array.new(work_items.length)
          work_items.each_with_index do |item, index|
            slot = next_worker_slot
            assignments[index] = slot
            slot.send_work(index, item, reply_port)
          end
          assignments
        end
      end

      def signal_workers
        @slots.each(&:shutdown)
      end

      def wait_for_workers
        if @configuration.timeout
          wait_for_workers_with_timeout
        else
          @slots.each(&:join)
        end
      end

      def wait_for_workers_with_timeout
        timeout = @configuration.timeout
        return unless timeout

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

        @slots.each do |slot|
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          break unless remaining.positive?

          ::Timeout.timeout(remaining, TimeoutError) { slot.join }
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

      def self.start_worker(processor, boot_port)
        ::Ractor.new(processor, boot_port) do |safe_processor, ready_port|
          inbox = ::Ractor::Port.new
          ready_port << inbox

          CDC::Parallel::ProcessorPool.send(:run_worker_loop, safe_processor, inbox)
        end
      end
      private_class_method :start_worker

      def self.run_worker_loop(safe_processor, inbox)
        loop do
          message = inbox.receive
          break if message.nil?

          index, item, reply_port = message
          response = worker_response(safe_processor, item)

          begin
            reply_port << [index, response]
          rescue Ractor::ClosedError
            # The caller may have timed out and closed the reply port.
          end
        end
      end
      private_class_method :run_worker_loop

      def self.worker_response(safe_processor, item)
        CDC::Parallel::ResultCollector.worker_success(
          safe_processor.process(item)
        )
      rescue StandardError => e
        CDC::Parallel::ResultCollector.worker_failure(e)
      end
      private_class_method :worker_response

      def next_worker_slot
        slot = @slots[@next_worker]

        @next_worker += 1
        @next_worker = 0 if @next_worker >= @slots.length

        slot
      end

      def collect_results(reply_port, count, assignments = [])
        results = Array.new(count)
        return results.freeze if count.zero?

        if @configuration.timeout
          collect_results_with_timeout(reply_port, results, assignments)
        else
          collect_results_without_timeout(reply_port, results, assignments)
        end
      end

      def collect_results_without_timeout(reply_port, results, assignments = [])
        results.length.times do
          index, response = reply_port.receive
          results[index] = ResultCollector.normalize(response)
          assignments[index]&.complete(index)
        end

        results.freeze
      end

      def collect_results_with_timeout(reply_port, results, assignments = [])
        timeout = @configuration.timeout
        return results.freeze unless timeout

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

        results.length.times do
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          return timeout_results(results) unless remaining.positive?

          index, response = ::Timeout.timeout(remaining, TimeoutError) { reply_port.receive }
          results[index] = ResultCollector.normalize(response)
          assignments[index]&.complete(index)
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

      # One supervised worker position in the pool.
      #
      # A slot keeps its identity while its underlying Ractor can be replaced
      # after unexpected death. The slot also owns in-flight reply ports so a
      # fatal worker exit can be reported back to callers instead of leaving
      # them blocked forever.
      #
      # @api private
      # rubocop:disable Metrics/ClassLength
      class WorkerSlot
        # Number of times this slot has booted a replacement worker.
        #
        # @return [Integer]
        # @api private
        attr_reader :respawns

        # Create a supervised worker slot.
        #
        # @param index [Integer]
        #   Stable slot index inside the owning pool.
        # @param processor [CDC::Core::Processor]
        #   Shareable processor instance used by the worker.
        # @param supervision [Boolean]
        #   Whether unexpected worker death should trigger respawn.
        # @param max_respawns [Integer]
        #   Maximum crash count inside the respawn window before cooldown.
        # @param respawn_window [Numeric]
        #   Sliding crash-loop window in seconds.
        # @param respawn_cooldown [Numeric]
        #   Cooldown duration in seconds after repeated crashes.
        # @return [void]
        # @api private
        def initialize(index:, processor:, supervision:, max_respawns:, respawn_window:, respawn_cooldown:)
          @index = index
          @processor = processor
          @supervision = supervision
          @max_respawns = Integer(max_respawns)
          @respawn_window = Float(respawn_window)
          @respawn_cooldown = Float(respawn_cooldown)
          @lock = Mutex.new
          @inflight = {}
          @crashes = []
          @respawns = 0
          @shutdown = false
          @degraded_until = nil
          boot!
          @supervisor_thread = Thread.new { supervise }
        end

        # Return the current worker Ractor.
        #
        # @return [Ractor]
        # @api private
        def worker
          @lock.synchronize { @worker }
        end

        # Return the current worker inbox port.
        #
        # @return [Ractor::Port]
        # @api private
        def inbox
          @lock.synchronize { @inbox }
        end

        # Send one indexed work item to the current worker.
        #
        # If the slot is cooling down or the worker inbox is already closed, a
        # serialized failure response is sent to the caller reply port.
        #
        # @param index [Integer]
        # @param item [Object]
        # @param reply_port [Ractor::Port]
        # @return [void]
        # @api private
        def send_work(index, item, reply_port)
          target = nil
          immediate_failure = nil

          @lock.synchronize do
            if degraded_locked?
              immediate_failure = RuntimeError.new("worker slot #{@index} is cooling down after repeated crashes")
            else
              @inflight[index] = reply_port
              target = @inbox
            end
          end

          return send_failure(reply_port, index, immediate_failure) if immediate_failure

          target.send([index, item, reply_port])
        rescue Ractor::ClosedError => e
          complete(index)
          send_failure(reply_port, index, e)
        end

        # Mark an in-flight work item as completed.
        #
        # @param index [Integer]
        # @return [void]
        # @api private
        def complete(index)
          @lock.synchronize { @inflight.delete(index) }
        end

        # Return whether this slot is currently in crash-loop cooldown.
        #
        # @return [Boolean]
        # @api private
        def degraded?
          @lock.synchronize { degraded_locked? }
        end

        # Return whether this slot is degraded while the caller holds the lock.
        #
        # @return [Boolean]
        # @api private
        def degraded_locked?
          degraded_until = @degraded_until
          return false unless degraded_until

          if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= degraded_until
            @degraded_until = nil
            false
          else
            true
          end
        end

        # Request worker shutdown and prevent future respawns.
        #
        # @return [void]
        # @api private
        def shutdown
          target = nil
          @lock.synchronize do
            @shutdown = true
            target = @inbox
          end

          target&.send(nil)
        rescue Ractor::ClosedError
          nil
        end

        # Wait for the supervisor thread to finish.
        #
        # @return [Thread]
        # @api private
        def join
          @supervisor_thread.join
        end

        private

        def boot!
          boot_port = ::Ractor::Port.new
          worker = CDC::Parallel::ProcessorPool.send(:start_worker, @processor, boot_port)
          inbox = boot_port.receive

          @lock.synchronize do
            @worker = worker
            @inbox = inbox
          end
        ensure
          boot_port&.close
        end

        def supervise
          loop do
            current_worker = worker
            cause = wait_for_worker_death(current_worker)
            break if shutting_down?

            fail_inflight(cause)
            break unless @supervision

            cool_down_if_needed
            break if shutting_down?

            boot!
            @lock.synchronize { @respawns += 1 }
          end
        end

        def wait_for_worker_death(current_worker)
          current_worker.value
          RuntimeError.new("worker slot #{@index} exited unexpectedly")
        rescue Ractor::Error => e
          (e.respond_to?(:cause) && e.cause) || e
        end

        def fail_inflight(cause)
          pending = nil
          @lock.synchronize do
            pending = @inflight.dup
            @inflight.clear
          end

          pending.each { |index, reply_port| send_failure(reply_port, index, cause) }
        end

        def send_failure(reply_port, index, error)
          reply_port << [index, ResultCollector.worker_failure(error)]
        rescue Ractor::ClosedError
          nil
        end

        def cool_down_if_needed
          return unless crash_loop?

          deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @respawn_cooldown
          @lock.synchronize { @degraded_until = deadline }
          sleep @respawn_cooldown
          @lock.synchronize { @degraded_until = nil }
        end

        def crash_loop?
          now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          @lock.synchronize do
            @crashes = @crashes.select { |timestamp| now - timestamp <= @respawn_window }
            @crashes << now
            @crashes.length > @max_respawns
          end
        end

        def shutting_down?
          @lock.synchronize { @shutdown }
        end
      end
      # rubocop:enable Metrics/ClassLength
    end
  end
end
